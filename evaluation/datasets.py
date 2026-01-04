"""
Dataset loaders for BIRD, Spider, CoSQL, and SParC benchmarks.

Uses the unified turns.db created by data/create.py.
All datasets are loaded from a single source - no duplicate implementations.
"""

import sqlite3
from pathlib import Path
from dataclasses import dataclass
from typing import List, Optional


TURNS_DB = Path(__file__).parent.parent / "data" / "normalized" / "turns.db"
SUPPORTED_DATASETS = ["bird", "spider", "cosql", "sparc"]


@dataclass
class TestCase:
    """Unified test case format for evaluation."""
    id: str
    db_id: str
    question: str
    gold_sql: str
    db_file: str
    difficulty: Optional[str] = None
    conversation_history: Optional[List[dict]] = None


@dataclass
class ConversationTurn:
    """A single turn in a conversation."""
    turn_index: int
    question: str
    gold_sql: str


@dataclass
class Conversation:
    """A full conversation with multiple turns."""
    id: str
    db_id: str
    db_file: str
    query_goal: str
    turns: List[ConversationTurn]


def get_db_path(test_case_or_conv) -> str:
    """Get the absolute path to a database file from a TestCase or Conversation."""
    base = Path(__file__).parent.parent
    return str(base / test_case_or_conv.db_file)


def load_dataset(
    dataset: str,
    sample_size: Optional[int] = None,
    split: str = "validation",
) -> List[TestCase]:
    """
    Load test cases from the unified turns.db.
    
    Args:
        dataset: One of "bird", "spider", "cosql", "sparc"
        sample_size: Number of samples to load (None = all)
        split: "train" or "dev" (default: "dev")
    
    Returns:
        List of TestCase objects
    """
    if dataset not in SUPPORTED_DATASETS:
        raise ValueError(f"Unknown dataset: {dataset}. Supported: {SUPPORTED_DATASETS}")
    
    conn = sqlite3.connect(TURNS_DB)
    cursor = conn.cursor()
    
    query = """
        SELECT turn_uid, db_id, text, gold_sql, db_file, difficulty, 
               conversation_id, turn_index, context, context_gold_sql
        FROM turns
        WHERE dataset = ? AND split = ?
        ORDER BY conversation_id, turn_index
    """
    
    if sample_size:
        query += f" LIMIT {sample_size}"
    
    cursor.execute(query, (dataset, split))
    rows = cursor.fetchall()
    conn.close()
    
    test_cases = []
    for row in rows:
        turn_uid, db_id, text, gold_sql, db_file, difficulty, conv_id, turn_idx, context, context_gold_sql = row
        
        # Parse context for conversation history
        conv_history = None
        if context and context != "[]":
            try:
                import json
                ctx_list = json.loads(context)
                ctx_sql_list = json.loads(context_gold_sql) if context_gold_sql else []
                conv_history = []
                for i, ctx_text in enumerate(ctx_list):
                    conv_history.append({
                        "role": "user" if i % 2 == 0 else "assistant",
                        "content": ctx_text,
                        "sql": ctx_sql_list[i] if i < len(ctx_sql_list) else None,
                    })
            except:
                pass
        
        test_cases.append(TestCase(
            id=turn_uid,
            db_id=db_id,
            question=text,
            gold_sql=gold_sql,
            db_file=db_file,
            difficulty=difficulty,
            conversation_history=conv_history,
        ))
    
    return test_cases


def load_conversations(
    dataset: str,
    sample_size: Optional[int] = None,
    split: str = "validation",
) -> List[Conversation]:
    """
    Load full conversations (for multi-turn datasets like CoSQL, SParC).
    
    Args:
        dataset: One of "cosql", "sparc" (or "bird", "spider" for single-turn)
        sample_size: Number of conversations to load (None = all)
        split: "train" or "dev" (default: "dev")
    
    Returns:
        List of Conversation objects
    """
    if dataset not in SUPPORTED_DATASETS:
        raise ValueError(f"Unknown dataset: {dataset}. Supported: {SUPPORTED_DATASETS}")
    
    conn = sqlite3.connect(TURNS_DB)
    cursor = conn.cursor()
    
    # Get all turns grouped by conversation
    query = """
        SELECT conversation_id, db_id, db_file, turn_index, text, gold_sql
        FROM turns
        WHERE dataset = ? AND split = ?
        ORDER BY conversation_id, turn_index
    """
    
    cursor.execute(query, (dataset, split))
    rows = cursor.fetchall()
    conn.close()
    
    # Group by conversation
    conversations_dict = {}
    for row in rows:
        conv_id, db_id, db_file, turn_idx, text, gold_sql = row
        
        if conv_id not in conversations_dict:
            conversations_dict[conv_id] = {
                "db_id": db_id,
                "db_file": db_file,
                "turns": [],
            }
        
        conversations_dict[conv_id]["turns"].append(ConversationTurn(
            turn_index=turn_idx,
            question=text,
            gold_sql=gold_sql,
        ))
    
    # Convert to Conversation objects
    conversations = []
    for conv_id, data in conversations_dict.items():
        if sample_size and len(conversations) >= sample_size:
            break
        
        turns = data["turns"]
        query_goal = turns[-1].question if turns else ""
        
        conversations.append(Conversation(
            id=conv_id,
            db_id=data["db_id"],
            db_file=data["db_file"],
            query_goal=query_goal,
            turns=turns,
        ))
    
    return conversations


def get_dataset_stats() -> dict:
    """Get counts for each dataset."""
    conn = sqlite3.connect(TURNS_DB)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT dataset, split, COUNT(*) 
        FROM turns 
        GROUP BY dataset, split
    """)
    
    stats = {}
    for row in cursor.fetchall():
        dataset, split, count = row
        if dataset not in stats:
            stats[dataset] = {}
        stats[dataset][split] = count
    
    conn.close()
    return stats
