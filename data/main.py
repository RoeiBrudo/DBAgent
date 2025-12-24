"""
Main data access module for Text2SQL datasets.

Provides functions to:
1. Load data from the unified dataset (optionally filtered by source)
2. Get database connections for specific turns
"""

import json
import sqlite3
from pathlib import Path
from typing import List, Dict, Any, Optional


# =============================================================================
# Configuration
# =============================================================================

BASE_DIR = Path(__file__).resolve().parent
NORMALIZED_DIR = (BASE_DIR / "normalized").resolve()
NORMALIZED_DB = NORMALIZED_DIR / "turns.db"

SPIDER_DB_ROOT = (BASE_DIR / "external" / "prem-research_spider" / "database").resolve()
BIRD_DB_ROOT = (BASE_DIR / "external" / "bird_mini_dev" / "MINIDEV" / "dev_databases").resolve()


# =============================================================================
# Data Loading Functions
# =============================================================================

def load_data(
    source: Optional[str] = None,
    split: Optional[str] = None,
    limit: Optional[int] = None,
    min_turn_index: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    Load data from the unified dataset.
    
    Args:
        source: Filter by dataset source ('spider', 'bird', 'cosql', 'sparc').
                If None, returns all datasets.
        split: Filter by split ('train' or 'validation').
               If None, returns all splits.
        limit: Maximum number of turns to return. If None, returns all.
    
    Returns:
        List of turn dictionaries with all fields from the database.
    
    Example:
        # Load all Spider training data
        spider_train = load_data(source='spider', split='train')
        
        # Load first 100 turns from all datasets
        sample = load_data(limit=100)
        
        # Load all CoSQL data
        cosql = load_data(source='cosql')
    """
    if not NORMALIZED_DB.exists():
        raise FileNotFoundError(
            f"Unified database not found at {NORMALIZED_DB}. "
            "Please run helpers/prepare.py first."
        )
    
    conn = sqlite3.connect(str(NORMALIZED_DB))
    cursor = conn.cursor()
    
    # Build query
    query = "SELECT * FROM turns"
    conditions = []
    params = []
    
    if source:
        conditions.append("dataset = ?")
        params.append(source)
    
    if split:
        conditions.append("split = ?")
        params.append(split)
    
    if min_turn_index is not None:
        conditions.append("turn_index >= ?")
        params.append(int(min_turn_index))

    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    
    query += " ORDER BY turn_uid"
    
    if limit:
        query += f" LIMIT {limit}"
    
    cursor.execute(query, params)
    rows = cursor.fetchall()
    
    # Get column names
    columns = [desc[0] for desc in cursor.description]
    conn.close()
    
    # Convert to list of dicts and parse JSON fields
    turns = []
    for row in rows:
        turn = dict(zip(columns, row))
        # Parse JSON fields
        turn["context"] = json.loads(turn.get("context", "[]"))
        turn["context_gold_sql"] = json.loads(turn.get("context_gold_sql", "[]"))
        turns.append(turn)
    
    return turns


# =============================================================================
# Database Connection Functions
# =============================================================================

def get_db_connection(db_file) -> sqlite3.Connection:
    db_path = Path(db_file)
    if not db_path.is_absolute():
        db_path = BASE_DIR.parent / db_path
    if not db_path.exists():
        raise FileNotFoundError(f"Database file not found: {db_file}\n")
    
    return sqlite3.connect(str(db_path))


def get_database_schema(db_connection) -> Dict[str, List[str]]:
    cursor = db_connection.cursor()
    
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = [row[0] for row in cursor.fetchall() if not str(row[0]).startswith('sqlite_')]
    
    schema = {}
    for table in tables:
        tname = str(table)
        quoted = '"' + tname.replace('"', '""') + '"'
        cursor.execute(f"PRAGMA table_info({quoted})")
        columns = [row[1] for row in cursor.fetchall()]
        schema[table] = columns
    
    return schema


# =============================================================================
# Utility Functions
# =============================================================================

def get_turn(turn_uid: str) -> Optional[Dict[str, Any]]:
    """
    Get a single turn by its unique identifier.
    
    Args:
        turn_uid: The unique turn identifier.
    
    Returns:
        Turn dictionary if found, None otherwise.
    
    Example:
        # Get a specific turn
        turn = get_turn('spider:train:spider:train:0:0')
        if turn:
            print(f"Question: {turn['text']}")
            print(f"SQL: {turn['gold_sql']}")
    """
    if not NORMALIZED_DB.exists():
        raise FileNotFoundError(f"Unified database not found at {NORMALIZED_DB}")
    
    conn = sqlite3.connect(str(NORMALIZED_DB))
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT * FROM turns
        WHERE turn_uid = ?
    """, (turn_uid,))
    
    row = cursor.fetchone()
    columns = [desc[0] for desc in cursor.description]
    conn.close()
    
    if not row:
        return None
    
    # Convert to dict and parse JSON fields
    turn = dict(zip(columns, row))
    turn["context"] = json.loads(turn.get("context", "[]"))
    turn["context_gold_sql"] = json.loads(turn.get("context_gold_sql", "[]"))
    
    return turn


def get_conversation(conversation_id: str) -> List[Dict[str, Any]]:
    """
    Get all turns in a conversation, ordered by turn index.

    Args:
        conversation_id: The conversation identifier.

    Returns:
        List of turns in the conversation, ordered by turn_index.

    Example:
        # Get a conversation from CoSQL
        cosql_turns = load_data(source='cosql', limit=10)
        conv_id = cosql_turns[0]['conversation_id']
        conversation = get_conversation(conv_id)
        for turn in conversation:
            print(f"Turn {turn['turn_index']}: {turn['text']}")
    """
    if not NORMALIZED_DB.exists():
        raise FileNotFoundError(f"Unified database not found at {NORMALIZED_DB}")

    conn = sqlite3.connect(str(NORMALIZED_DB))
    cursor = conn.cursor()

    cursor.execute("""
        SELECT * FROM turns
        WHERE conversation_id = ?
        ORDER BY turn_index
    """, (conversation_id,))

    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    conn.close()

    # Convert to list of dicts
    turns = []
    for row in rows:
        turn = dict(zip(columns, row))
        turn["context"] = json.loads(turn.get("context", "[]"))
        turn["context_gold_sql"] = json.loads(turn.get("context_gold_sql", "[]"))
        turns.append(turn)

    return turns
