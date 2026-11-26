"""Fixed OpenCode SQLite operations; invoked through WSL, never a shell.

Read operations use a read-only URI connection. `delete_session` is the only
write operation and is issued by an explicit user action. Table and column
names are allowlisted here as well as in Rust.
"""
import json
import pathlib
import sqlite3
import sys

sys.stdout.reconfigure(encoding="utf-8")

FRAME_CHARS = 128 * 1024
DB_PATH = pathlib.Path(sys.argv[1])
request = json.loads(sys.argv[2])


def jsonable(value):
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def rows(connection, sql, parameters=()):
    result = []
    for row in connection.execute(sql, parameters):
        result.append({key: jsonable(row[key]) for key in row.keys()})
    return result


def table_exists(connection, table):
    return bool(
        connection.execute(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name=?)",
            (table,),
        ).fetchone()[0]
    )


def table_columns(connection, table):
    if not table_exists(connection, table):
        return set()
    return {row["name"] for row in connection.execute("PRAGMA table_info(" + table + ")")}


def session_schema(connection):
    v2 = table_exists(connection, "session_v2") and table_exists(connection, "session_message")
    return "v2" if v2 else "v1"


def usage_schema(connection):
    v2 = table_exists(connection, "session_v2") and table_exists(connection, "session_message")
    v1 = table_exists(connection, "session") and table_exists(connection, "message")
    if v2 and v1:
        return "mixed"
    if v2:
        return "v2"
    return "v1"


def migration_cutoff(connection):
    if not table_exists(connection, "kv"):
        return 0
    row = connection.execute(
        "SELECT value, max(coalesce(time_created, 0), coalesce(time_updated, 0)) "
        "FROM kv WHERE key = 'migration.v1-v2'"
    ).fetchone()
    if row is None:
        return 0
    try:
        value = json.loads(row["value"])
    except (TypeError, ValueError):
        return 0
    if value.get("phase") == "completed":
        return int(row[1] or 0)
    return 0


def session_rows(connection):
    return rows(
        connection,
        "SELECT id, COALESCE(title, '') AS title, directory, time_created, time_updated "
        "FROM session ORDER BY time_updated DESC",
    )


def v2_session_rows(connection):
    return rows(
        connection,
        "SELECT s.id, COALESCE(s.title, '') AS title, s.directory, s.time_created, "
        "s.time_updated, "
        "MAX(s.time_updated, COALESCE(MAX(m.time_updated), s.time_updated)) AS last_active_at "
        "FROM session_v2 s "
        "LEFT JOIN session_message m ON m.session_id = s.id "
        "GROUP BY s.id "
        "ORDER BY last_active_at DESC",
    )


def scan_sessions(connection):
    schema = session_schema(connection)
    if schema == "v1":
        return {"schema": schema, "v1": session_rows(connection), "v2": []}
    v2 = v2_session_rows(connection)
    v1 = []
    cutoff = 0
    if table_exists(connection, "session"):
        cutoff = migration_cutoff(connection)
        v1 = rows(
            connection,
            "SELECT id, COALESCE(title, '') AS title, directory, time_created, time_updated "
            "FROM session WHERE time_updated > ? ORDER BY time_updated DESC",
            (cutoff,),
        )
    return {"schema": schema, "v1": v1, "v2": v2}


def message_rows_v1(connection, session_id):
    messages = rows(
        connection,
        "SELECT id, time_created, data FROM message "
        "WHERE session_id = ? ORDER BY time_created ASC, id ASC",
        (session_id,),
    )
    parts = []
    if table_exists(connection, "part"):
        parts = rows(
            connection,
            "SELECT id, message_id, data FROM part "
            "WHERE session_id = ? ORDER BY time_created ASC, id ASC",
            (session_id,),
        )
    return {"layout": "v1", "messages": messages, "parts": parts}


def message_rows_v2(connection, session_id):
    order = "seq ASC, rowid ASC" if "seq" in table_columns(connection, "session_message") else "rowid ASC"
    return {
        "layout": "v2",
        "messages": rows(
            connection,
            "SELECT id, type, time_created, data FROM session_message "
            "WHERE session_id = ? ORDER BY " + order,
            (session_id,),
        ),
        "parts": [],
    }


def load_messages(connection):
    session_id = request["session_id"]
    schema = session_schema(connection)
    if schema == "v1":
        return message_rows_v1(connection, session_id)

    in_session_v2 = bool(
        connection.execute(
            "SELECT EXISTS(SELECT 1 FROM session_v2 WHERE id = ?)", (session_id,)
        ).fetchone()[0]
    )
    if in_session_v2:
        prefer_v1 = False
        if table_exists(connection, "message"):
            if table_exists(connection, "part"):
                v1_ts = connection.execute(
                    "SELECT MAX(ts) FROM ("
                    " SELECT MAX(time_created) AS ts FROM message WHERE session_id = ?"
                    " UNION ALL"
                    " SELECT MAX(time_created) AS ts FROM part WHERE session_id = ?"
                    ")",
                    (session_id, session_id),
                ).fetchone()[0]
            else:
                v1_ts = connection.execute(
                    "SELECT MAX(time_created) FROM message WHERE session_id = ?",
                    (session_id,),
                ).fetchone()[0]
            v2_ts = connection.execute(
                "SELECT MAX(ts) FROM ("
                " SELECT MAX(time_updated) AS ts FROM session_message WHERE session_id = ?"
                " UNION ALL"
                " SELECT MAX(time_created) AS ts FROM session_message WHERE session_id = ?"
                ")",
                (session_id, session_id),
            ).fetchone()[0]
            if v1_ts is not None and (v2_ts is None or v1_ts > v2_ts):
                prefer_v1 = True
        if prefer_v1:
            return message_rows_v1(connection, session_id)
        return message_rows_v2(connection, session_id)

    if table_exists(connection, "message"):
        return message_rows_v1(connection, session_id)
    return message_rows_v2(connection, session_id)


def read_cell(connection):
    table = request["table"]
    column = request["column"]
    if table not in ("part", "message", "session_message"):
        raise ValueError("Unsupported OpenCode table")
    if column not in ("data", "content"):
        raise ValueError("Unsupported OpenCode column")
    if not table_exists(connection, table):
        raise ValueError("OpenCode table is missing")
    if column not in table_columns(connection, table):
        raise ValueError("OpenCode column is missing")

    session_id = request["session_id"]
    row_id = request["id"]
    size, value = connection.execute(
        "SELECT length(CAST(" + column + " AS BLOB)), " + column + " FROM " + table +
        " WHERE id = ? AND session_id = ?",
        (row_id, session_id),
    ).fetchone()
    if size is None:
        raise ValueError("OpenCode content is no longer available")
    if int(size) > int(request["max_bytes"]):
        raise ValueError("OpenCode content is too large")
    if value is None:
        return None
    return jsonable(value)


def usage_sessions(connection):
    schema = usage_schema(connection)
    v1_message_ts = "time_updated" if "time_updated" in table_columns(connection, "message") else "time_created"
    if schema == "v1":
        query = (
            "SELECT s.id, MAX(s.time_updated, "
            "COALESCE(MAX(m." + v1_message_ts + "), s.time_updated)) AS sync_watermark "
            "FROM session s LEFT JOIN message m ON m.session_id = s.id "
            "GROUP BY s.id"
        )
        data = rows(connection, query)
    elif schema == "v2":
        query = (
            "SELECT s.id, MAX(s.time_updated, "
            "COALESCE(MAX(m.time_updated), s.time_updated)) AS sync_watermark "
            "FROM session_v2 s LEFT JOIN session_message m ON m.session_id = s.id "
            "GROUP BY s.id"
        )
        data = rows(connection, query)
    else:
        query = (
            "SELECT id, MAX(sync_watermark) AS sync_watermark FROM ("
            " SELECT s.id, MAX(s.time_updated, "
            "COALESCE(MAX(m.time_updated), s.time_updated)) AS sync_watermark "
            " FROM session_v2 s LEFT JOIN session_message m ON m.session_id = s.id "
            " GROUP BY s.id"
            " UNION ALL"
            " SELECT s.id, MAX(s.time_updated, "
            "COALESCE(MAX(m." + v1_message_ts + "), s.time_updated)) AS sync_watermark "
            " FROM session s LEFT JOIN message m ON m.session_id = s.id "
            " GROUP BY s.id"
            ") GROUP BY id"
        )
        data = rows(connection, query)
    return {"schema": schema, "sessions": data}


def usage_messages(connection):
    schema = request["schema"]
    session_id = request["session_id"]
    cutoff = int(request.get("cutoff_ms", 0))

    if schema == "v1":
        data = rows(
            connection,
            "SELECT id, data, NULL AS type FROM message "
            "WHERE session_id = ? ORDER BY time_created",
            (session_id,),
        )
    elif schema == "v2":
        data = rows(
            connection,
            "SELECT id, data, type FROM session_message "
            "WHERE session_id = ? AND type IN ('assistant', 'compaction') "
            "ORDER BY time_created",
            (session_id,),
        )
    else:
        v1_time = "time_updated" if "time_updated" in table_columns(connection, "message") else "time_created"
        data = rows(
            connection,
            "SELECT id, data, type FROM ("
            " SELECT id, data, type, time_updated, 0 AS layout_priority"
            " FROM session_message"
            " WHERE session_id = ? AND type IN ('assistant', 'compaction')"
            "   AND time_updated > ?"
            " UNION ALL"
            " SELECT id, data, NULL AS type, " + v1_time + " AS time_updated, 1 AS layout_priority"
            " FROM message WHERE session_id = ? AND " + v1_time + " > ?"
            ") ORDER BY time_updated DESC, layout_priority, id",
            (session_id, cutoff, session_id, cutoff),
        )
    return {"messages": data}


def delete_session(connection):
    session_id = request["session_id"]
    schema = session_schema(connection)
    deleted = 0
    connection.execute("BEGIN IMMEDIATE")
    try:
        if schema == "v2":
            deleted += connection.execute(
                "DELETE FROM session_message WHERE session_id = ?", (session_id,)
            ).rowcount
            for child in (
                "session_pending",
                "session_inbox",
                "instruction_entry",
                "instruction_state",
            ):
                if table_exists(connection, child):
                    connection.execute(
                        "DELETE FROM " + child + " WHERE session_id = ?", (session_id,)
                    )
            if table_exists(connection, "event_sequence"):
                connection.execute(
                    "DELETE FROM event_sequence WHERE aggregate_id = ?", (session_id,)
                )
            deleted += connection.execute(
                "DELETE FROM session_v2 WHERE id = ?", (session_id,)
            ).rowcount
            if table_exists(connection, "session"):
                if table_exists(connection, "part"):
                    connection.execute(
                        "DELETE FROM part WHERE session_id = ?", (session_id,)
                    )
                if table_exists(connection, "message"):
                    connection.execute(
                        "DELETE FROM message WHERE session_id = ?", (session_id,)
                    )
                deleted += connection.execute(
                    "DELETE FROM session WHERE id = ?", (session_id,)
                ).rowcount
        else:
            if table_exists(connection, "part"):
                connection.execute("DELETE FROM part WHERE session_id = ?", (session_id,))
            if table_exists(connection, "message"):
                connection.execute("DELETE FROM message WHERE session_id = ?", (session_id,))
            deleted += connection.execute(
                "DELETE FROM session WHERE id = ?", (session_id,)
            ).rowcount
        connection.execute("COMMIT")
    except Exception:
        connection.execute("ROLLBACK")
        raise
    return {"deleted": deleted > 0}


operation = request["operation"]
if operation in ("schema", "scan_sessions", "load_messages", "cell", "usage_sessions", "usage_messages"):
    connection = sqlite3.connect(
        DB_PATH.as_uri() + "?mode=ro",
        uri=True,
        timeout=2.0,
        isolation_level=None,
    )
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    connection.execute("BEGIN")
    try:
        if operation == "schema":
            scope = request.get("scope", "session")
            response = session_schema(connection) if scope == "session" else usage_schema(connection)
        elif operation == "scan_sessions":
            response = scan_sessions(connection)
        elif operation == "load_messages":
            response = load_messages(connection)
        elif operation == "cell":
            response = read_cell(connection)
        elif operation == "usage_sessions":
            response = usage_sessions(connection)
        else:
            response = usage_messages(connection)
        connection.rollback()
    finally:
        connection.close()
elif operation == "delete_session":
    connection = sqlite3.connect(DB_PATH, timeout=2.0, isolation_level=None)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys=ON")
    try:
        response = delete_session(connection)
    finally:
        connection.close()
else:
    raise ValueError("Unsupported OpenCode operation")

# Frame the JSON stream so Rust can apply display projection before a whole
# large result has to be materialized. Each frame stays bounded.
for part in json.JSONEncoder(ensure_ascii=False).iterencode(response):
    for start in range(0, len(part), FRAME_CHARS):
        print(json.dumps(part[start:start + FRAME_CHARS], ensure_ascii=False))
