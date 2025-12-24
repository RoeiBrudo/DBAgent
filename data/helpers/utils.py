import json
import sqlite3
from pathlib import Path
from typing import Optional, Dict, Any, List

# Anchor all paths to the repository root (independent of current working directory)
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_DATA_DIR = _PROJECT_ROOT / "data"

CACHE_DIR = (_DATA_DIR / "_hf_cache")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

SPIDER_LOCAL_DIR = (_DATA_DIR / "external/prem-research_spider").resolve()
SPIDER_DB_ROOT = (SPIDER_LOCAL_DIR / "database").resolve()
BIRD_DB_ROOT = (_DATA_DIR / "external/bird_dataset/dev_databases").resolve()
COSQL_LOCAL_DIR = (_DATA_DIR / "external/cosql_dataset").resolve()
NORMALIZED_DIR = (_DATA_DIR / "normalized").resolve()

SPIDER_HF = "xlangai/spider"
SPIDER_REPO = "prem-research/spider"
BIRD_HF = "birdsql/bird_mini_dev"
SPARC_HF = "aherntech/sparc"


def verify_database_connection(db_path: Path) -> bool:
    """Verify if a database can be opened and queried."""
    p = Path(db_path)
    if not p.is_absolute():
        p = _PROJECT_ROOT / p
    con = sqlite3.connect(str(p))
    try:
        cur = con.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
        tables = [r[0] for r in cur.fetchall()]
        return len(tables) > 0
    finally:
        con.close()


def get_database_path(dataset: str, db_id: str) -> Optional[str]:
    """Get the file path for a database given dataset and db_id."""
    if dataset in ["spider", "cosql", "sparc"]:
        db_path = SPIDER_DB_ROOT / db_id / f"{db_id}.sqlite"
        return str(db_path) if db_path.exists() else None

    elif dataset == "bird":
        db_path = BIRD_DB_ROOT / db_id / f"{db_id}.sqlite"
        return str(db_path) if db_path.exists() else None
    return None


def make_turn_uid(dataset: str, split: str, conversation_id: str, turn_index: int) -> str:
    """Generate unique turn identifier."""
    return f"{dataset}:{split}:{conversation_id}:{turn_index}"


def add_database_paths(turns: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Add db_file path to each turn for database access."""
    for turn in turns:
        dataset = turn.get("dataset")
        db_id = turn.get("db_id")
        if dataset and db_id:
            abs_path = get_database_path(dataset, db_id)
            if abs_path:
                rel_path = str(Path(abs_path).resolve().relative_to(_PROJECT_ROOT))
                turn["db_file"] = rel_path
            else:
                turn["db_file"] = None
    return turns


def save_to_sqlite(turns: List[Dict[str, Any]], output_path: Path):
    """Save normalized turns to SQLite database."""
    print(f"\nSaving to SQLite: {output_path}")

    # Create output directory
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Remove existing database
    if output_path.exists():
        output_path.unlink()

    # Connect and create table
    conn = sqlite3.connect(str(output_path))
    cursor = conn.cursor()

    # Create turns table
    cursor.execute("""
        CREATE TABLE turns (
            turn_uid TEXT PRIMARY KEY,
            dataset TEXT NOT NULL,
            split TEXT NOT NULL,
            conversation_id TEXT NOT NULL,
            turn_index INTEGER NOT NULL,
            db_id TEXT,
            db_file TEXT,
            dialect TEXT,
            text TEXT,
            context TEXT,
            context_gold_sql TEXT,
            gold_sql TEXT,
            difficulty TEXT
        )
    """)

    # Insert data
    for turn in turns:
        cursor.execute("""
            INSERT INTO turns VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            turn.get("turn_uid"),
            turn.get("dataset"),
            turn.get("split"),
            turn.get("conversation_id"),
            turn.get("turn_index"),
            turn.get("db_id"),
            turn.get("db_file"),
            turn.get("dialect"),
            turn.get("text"),
            json.dumps(turn.get("context", [])),
            json.dumps(turn.get("context_gold_sql", [])),
            turn.get("gold_sql"),
            turn.get("difficulty")
        ))

    # Create indexes for common queries
    cursor.execute("CREATE INDEX idx_dataset ON turns(dataset)")
    cursor.execute("CREATE INDEX idx_db_id ON turns(db_id)")
    cursor.execute("CREATE INDEX idx_conversation ON turns(conversation_id)")

    conn.commit()
    conn.close()

    print(f"  ✓ Saved {len(turns)} turns to {output_path}")
