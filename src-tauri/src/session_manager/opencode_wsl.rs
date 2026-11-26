//! OpenCode SQLite access for databases that live inside WSL.
//!
//! Windows reaches a WSL distro through the `\\wsl$` / `\\wsl.localhost` 9P
//! redirector, which does not provide the byte-range locks SQLite needs. A
//! direct `rusqlite` open there can fail with "database is locked"; even when
//! it succeeds, a read-only connection may not see committed rows that are
//! still in the `-wal` file.
//!
//! This module follows the Hermes WSL reader: fixed read operations run under
//! `wsl.exe` with the distro's own Python/SQLite, and the results come back as
//! framed JSON. Writes are limited to the explicit session-delete operation.
//!
//! The module is compiled on Windows only; non-Windows builds keep using the
//! native path.

use std::io::{self, BufRead, BufReader, Read};
use std::os::windows::process::CommandExt;
use std::path::Path;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

const READER: &str = include_str!("opencode_wsl_reader.py");
/// A single framed JSON line. The Python side emits bounded string chunks;
/// this cap is intentionally larger than Hermes's because OpenCode stores tool
/// output and message content in the same `data` column.
const MAX_FRAME: u64 = 4 * 1024 * 1024;

/// Decode bounded JSON-string frames as a byte stream. Deserializing directly
/// avoids retaining another complete encoded copy of a large result.
struct FramedReader<R> {
    source: BufReader<R>,
    frame: io::Cursor<Vec<u8>>,
}

impl<R: Read> FramedReader<R> {
    fn new(source: R) -> Self {
        Self {
            source: BufReader::new(source),
            frame: io::Cursor::new(Vec::new()),
        }
    }
}

impl<R: Read> Read for FramedReader<R> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        loop {
            let count = self.frame.read(output)?;
            if count != 0 {
                return Ok(count);
            }
            let mut line = Vec::new();
            let count = self
                .source
                .by_ref()
                .take(MAX_FRAME + 1)
                .read_until(b'\n', &mut line)?;
            if count == 0 {
                return Ok(0);
            }
            if count as u64 > MAX_FRAME || line.last() != Some(&b'\n') {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "OpenCode WSL frame exceeds its limit or is incomplete",
                ));
            }
            let decoded: String = serde_json::from_slice(&line)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            self.frame = io::Cursor::new(decoded.into_bytes());
        }
    }
}

/// Split a WSL UNC path into `(distro, linux_path)`.
///
/// Accepts `\\wsl$\<distro>\...`, `\\wsl.localhost\<distro>\...`, and
/// `\\?\UNC\...`. Empty and parent segments are rejected so the Python
/// process can only see the database path that was validated by the caller.
fn target(path: &Path) -> Option<(String, String)> {
    let text = path.to_str()?;
    let rest = text
        .strip_prefix(r"\\?\UNC\")
        .or_else(|| text.strip_prefix(r"\\"))?;
    let mut parts = rest.split('\\');
    let host = parts.next()?;
    if !host.eq_ignore_ascii_case("wsl.localhost") && !host.eq_ignore_ascii_case("wsl$") {
        return None;
    }
    let distro = parts.next()?;
    if distro.is_empty() || distro.starts_with('-') {
        return None;
    }
    let rest: Vec<_> = parts.collect();
    if rest.is_empty() || rest.iter().any(|p| p.is_empty() || *p == "." || *p == "..") {
        return None;
    }
    Some((distro.to_string(), format!("/{}", rest.join("/"))))
}

/// Whether this database path is reached through a WSL UNC share.
pub(crate) fn is_wsl(path: &Path) -> bool {
    target(path).is_some()
}

/// The WSL distribution name for a database path, if it is a WSL path.
pub(crate) fn distro(path: &Path) -> Option<String> {
    target(path).map(|(distro, _)| distro)
}

/// Run one fixed read-only operation inside the distro.
pub(crate) fn read(db_path: &Path, request: Value) -> Result<Value, String> {
    let (distro, native) = target(db_path).ok_or("Invalid OpenCode WSL database path")?;
    let mut command = Command::new("wsl.exe");
    command.args([
        "--distribution",
        &distro,
        "--exec",
        "timeout",
        "8s",
        "python3",
        "-I",
        "-c",
        READER,
        &native,
        &request.to_string(),
    ]);
    // Do not show a console window on each session refresh.
    command.creation_flags(0x08000000);
    let mut child = command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("Failed to start OpenCode WSL reader: {e}"))?;
    let stdout = child.stdout.take().unwrap();
    let stderr = child.stderr.take().unwrap();
    let out = thread::spawn(move || {
        serde_json::from_reader::<_, Value>(BufReader::new(FramedReader::new(stdout)))
    });
    let err = thread::spawn(move || {
        let mut bytes = Vec::new();
        stderr.take(4096).read_to_end(&mut bytes).map(|_| bytes)
    });
    let deadline = Instant::now() + Duration::from_secs(10);
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break Ok(status),
            Ok(None) if Instant::now() < deadline => thread::sleep(Duration::from_millis(20)),
            result => {
                let _ = child.kill();
                let _ = child.wait();
                break Err(match result {
                    Err(e) => format!("Failed to wait for OpenCode WSL reader: {e}"),
                    _ => "OpenCode WSL reader timed out".to_string(),
                });
            }
        }
    };
    let stdout = out
        .join()
        .map_err(|_| "OpenCode WSL reader thread failed")?
        .map_err(|e| format!("Invalid OpenCode WSL response: {e}"));
    let stderr = err
        .join()
        .map_err(|_| "OpenCode WSL reader thread failed")?
        .map_err(|e| e.to_string())?;
    if !status?.success() {
        return Err(format!(
            "OpenCode WSL read failed (python3 is required): {}",
            String::from_utf8_lossy(&stderr).trim()
        ));
    }
    stdout
}

/// Read one whitelisted SQLite cell (tool output, arguments, thinking text).
pub(crate) fn read_cell(
    db_path: &Path,
    session_id: &str,
    table: &str,
    id: &str,
    column: &str,
    max_bytes: u64,
) -> Result<String, String> {
    let value = read(
        db_path,
        json!({
            "operation": "cell",
            "session_id": session_id,
            "table": table,
            "id": id,
            "column": column,
            "max_bytes": max_bytes,
        }),
    )?;
    value
        .as_str()
        .map(str::to_string)
        .ok_or_else(|| "OpenCode WSL cell is not text".to_string())
}

/// Delete a session inside the distro. The Python side performs the same
/// fixed V1/V2 cleanup as the native path.
pub(crate) fn delete_session(db_path: &Path, session_id: &str) -> Result<bool, String> {
    let value = read(
        db_path,
        json!({ "operation": "delete_session", "session_id": session_id }),
    )?;
    value
        .get("deleted")
        .and_then(Value::as_bool)
        .ok_or_else(|| "Invalid OpenCode WSL delete response".to_string())
}

/// Check whether a distro is already running.
///
/// `wsl.exe --list --running` only asks the WSL service; it does not start the
/// distribution. The background usage sync uses this to avoid waking a stopped
/// distro every ten minutes.
pub(crate) fn distro_running(distro: &str) -> bool {
    let output = Command::new("wsl.exe")
        .args(["--list", "--running", "--quiet"])
        .creation_flags(0x08000000)
        .output();
    match output {
        Ok(output) if output.status.success() => parse_running_distros(&output.stdout)
            .iter()
            .any(|name| name.eq_ignore_ascii_case(distro)),
        _ => false,
    }
}

fn parse_running_distros(bytes: &[u8]) -> Vec<String> {
    decode_wsl_list(bytes)
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect()
}

/// `wsl.exe --list` often emits UTF-16LE without a BOM; `-q` on newer builds
/// may emit UTF-8. Accept both so the running check is not locale-dependent.
fn decode_wsl_list(bytes: &[u8]) -> String {
    if bytes.starts_with(&[0xFF, 0xFE]) {
        return decode_utf16le(&bytes[2..]);
    }
    if bytes.len() >= 2 && bytes.iter().skip(1).step_by(2).any(|byte| *byte == 0) {
        return decode_utf16le(bytes);
    }
    String::from_utf8_lossy(bytes).into_owned()
}

fn decode_utf16le(bytes: &[u8]) -> String {
    let units: Vec<u16> = bytes
        .chunks_exact(2)
        .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]]))
        .collect();
    String::from_utf16_lossy(&units)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    use std::path::PathBuf;

    #[test]
    fn routes_only_wsl_hosts_and_preserves_linux_path() {
        for path in [
            r"\\wsl.localhost\Ubuntu-22.04\home\user\.local\share\opencode\opencode.db",
            r"\\wsl$\Ubuntu-22.04\home\user\.local\share\opencode\opencode.db",
            r"\\?\UNC\wsl.localhost\Ubuntu-22.04\home\user\.local\share\opencode\opencode.db",
        ] {
            assert_eq!(
                target(Path::new(path)),
                Some((
                    "Ubuntu-22.04".into(),
                    "/home/user/.local/share/opencode/opencode.db".into()
                ))
            );
        }
        for path in [
            r"C:\Users\user\.local\share\opencode\opencode.db",
            r"\\server\share\opencode.db",
            r"\\wsl$\-option\opencode.db",
            r"\\wsl$\Ubuntu\..\opencode.db",
        ] {
            assert!(target(Path::new(path)).is_none(), "{path}");
        }
    }

    #[test]
    fn decodes_utf8_and_utf16_running_distros() {
        let utf8 = b"Ubuntu-24.04\r\nDebian\r\n";
        assert_eq!(
            parse_running_distros(utf8),
            vec!["Ubuntu-24.04".to_string(), "Debian".to_string()]
        );

        let text: Vec<u16> = "Ubuntu-24.04\r\nDebian\r\n".encode_utf16().collect();
        let bytes: Vec<u8> = text.iter().flat_map(|unit| unit.to_le_bytes()).collect();
        assert_eq!(
            parse_running_distros(&bytes),
            vec!["Ubuntu-24.04".to_string(), "Debian".to_string()]
        );
    }

    #[test]
    fn framed_reader_rejects_oversized_or_incomplete_frames() {
        let oversized = format!("\"{}\"\n", "x".repeat(MAX_FRAME as usize));
        let mut reader = FramedReader::new(oversized.as_bytes());
        let mut sink = Vec::new();
        assert!(reader.read_to_end(&mut sink).is_err());

        let mut reader = FramedReader::new(b"\"unfinished\"".as_ref());
        let mut sink = Vec::new();
        assert!(reader.read_to_end(&mut sink).is_err());
    }

    #[test]
    fn target_accepts_pathbuf_roundtrip() {
        let path = PathBuf::from(r"\\wsl$\Ubuntu\home\me\.local\share\opencode\opencode.db");
        assert_eq!(distro(&path).as_deref(), Some("Ubuntu"));
        assert!(is_wsl(&path));
    }
}
