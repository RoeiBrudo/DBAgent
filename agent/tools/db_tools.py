from pathlib import Path
import sqlite3
import re
import time

from data.main import get_db_connection, get_database_schema


def connect_to_db(db_file: str, read_only: bool = False):
    """
    Connect to a database and return the connection and schema.
    
    Args:
        db_file: Path to the database file
        read_only: If True, open in read-only mode (recommended for query execution)
    """
    if read_only:
        # Use URI mode for read-only connection - SQLite enforces this at DB level
        db_path = Path(db_file).resolve()
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    else:
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


def get_schema_enrichment(conn: sqlite3.Connection, schema: dict, sample_rows: int = 3) -> dict:
    """
    Enrich schema with data samples and column value enumeration.
    
    Returns dict with:
    - samples: {table: [sample rows]}
    - enums: {table: {column: [distinct values]}} for categorical columns
    """
    enrichment = {"samples": {}, "enums": {}}
    
    for table, columns in schema.items():
        # Sample rows
        try:
            cursor = conn.cursor()
            cursor.execute(f"SELECT * FROM {table} LIMIT {sample_rows}")
            rows = cursor.fetchall()
            enrichment["samples"][table] = {
                "columns": columns,
                "rows": [list(row) for row in rows]
            }
        except sqlite3.Error:
            continue
        
        # Enumerate categorical columns (text columns with <= 10 distinct values)
        enrichment["enums"][table] = {}
        for col in columns:
            try:
                cursor.execute(f"SELECT DISTINCT {col} FROM {table} LIMIT 15")
                distinct_values = [row[0] for row in cursor.fetchall() if row[0] is not None]
                
                # Only include if it looks categorical (few distinct values, text type)
                if len(distinct_values) <= 10 and distinct_values:
                    # Check if values are strings (categorical)
                    if all(isinstance(v, str) for v in distinct_values):
                        enrichment["enums"][table][col] = distinct_values
            except sqlite3.Error:
                continue
    
    return enrichment


def format_enrichment_for_prompt(enrichment: dict, max_tables: int = 5) -> str:
    """Format enrichment data for inclusion in LLM prompts."""
    lines = []
    
    tables = list(enrichment.get("samples", {}).keys())[:max_tables]
    
    for table in tables:
        sample_data = enrichment["samples"].get(table, {})
        columns = sample_data.get("columns", [])
        rows = sample_data.get("rows", [])
        
        if rows:
            lines.append(f"\n{table} (sample data):")
            lines.append(f"  Columns: {columns}")
            for i, row in enumerate(rows[:2]):  # Show max 2 rows
                lines.append(f"  Row {i+1}: {row}")
        
        # Show enumerated values
        enums = enrichment.get("enums", {}).get(table, {})
        if enums:
            for col, values in enums.items():
                lines.append(f"  {col} values: {values}")
    
    return "\n".join(lines) if lines else ""
