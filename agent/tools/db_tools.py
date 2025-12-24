from pathlib import Path
import sqlite3
import re
import time

from data.main import get_db_connection, get_database_schema


def connect_to_db(db_file: str):
    """
    Connect to a database and return the connection and schema.
    """
    conn = get_db_connection(db_file)
    schema = get_database_schema(conn)
    return conn, schema


def _strip_comments_and_literals(statement: str) -> str:
    statement = re.sub(r"/\*.*?\*/", " ", statement, flags=re.S)
    statement = re.sub(r"--.*?(|$)", " ", statement)
    statement = re.sub(r"'([^']|'')*'", "''", statement)
    return statement

def _is_read_only(statement: str) -> bool:
    cleaned = _strip_comments_and_literals(statement).strip().lower().strip(';')
    if not re.match(r"^\s*(?:explain\s+(?:query\s+plan\s+)?)?(select|with)\b", cleaned):
        return False
    prohibited = (
        r"\b(insert|update|delete|replace|create|alter|drop|truncate|attach|detach|vacuum|"
        r"reindex|analyze|begin|commit|rollback|savepoint|release|pragma)\b"
    )
    return re.search(prohibited, cleaned) is None


def _progress_handler_generator(deadline):
    def _progress_handler():
        if deadline is not None and time.monotonic() >= deadline:
            return 1
        return 0
    return _progress_handler


def safe_query(conn: sqlite3.Connection, sql: str, msx_ms: int):
    start = time.monotonic()

    if not _is_read_only(sql):
        elapsed = (time.monotonic() - start) * 1000.0
        return {
            "success": False,
            "status": "NoSafe",
            "elapsed_ms": elapsed,
            "results": [],
            "error": "Query rejected by read-only policy",
        }

    deadline = start + (msx_ms / 1000.0) if msx_ms and msx_ms > 0 else None
    _progress_handler = _progress_handler_generator(deadline)

    conn.set_progress_handler(_progress_handler, 1000)
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        elapsed = (time.monotonic() - start) * 1000.0
        return {
            "success": True,
            "status": "success",
            "elapsed_ms": elapsed,
            "results": rows,
            "error": None,
        }
    except sqlite3.Error as exc:
        status = "TimeOut" if "interrupted" in str(exc).lower() else "ExecFailed"
        elapsed = (time.monotonic() - start) * 1000.0
        return {
            "success": False,
            "status": status,
            "elapsed_ms": elapsed,
            "results": [],
            "error": str(exc),
        }
    finally:
        conn.set_progress_handler(None, 0)
