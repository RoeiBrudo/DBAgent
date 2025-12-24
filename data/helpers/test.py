import sqlite3
from pathlib import Path
from typing import List, Dict, Any

from data.helpers.utils import get_database_path, SPIDER_DB_ROOT, BIRD_DB_ROOT


def verify_database_connection(db_path: Path) -> bool:
    con = sqlite3.connect(str(db_path))
    try:
        cur = con.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
        tables = [r[0] for r in cur.fetchall()]
        return len(tables) > 0
    finally:
        con.close()


# =============================================================================
# Individual Dataset Tests
# =============================================================================

def spider_dataset_test(spider):
    """Test Spider dataset for gold SQL and database availability."""
    print(f"  Splits: {list(spider.keys())}")
    
    total_rows = 0
    has_gold_sql = 0
    
    for split in spider.keys():
        split_size = len(spider[split])
        total_rows += split_size
        print(f"    {split}: {split_size} rows")
        
        # Check gold SQL
        for i in range(split_size):
            row = spider[split][i]
            sql = row.get("query") or row.get("sql") or row.get("SQL") or row.get("gold_sql")
            if sql and sql.strip():
                has_gold_sql += 1
    
    print(f"  Gold SQL: {has_gold_sql}/{total_rows} rows ({100*has_gold_sql/total_rows:.1f}%)")
    
    # Check database availability
    db_ids = []
    for split in spider.keys():
        db_ids.extend([spider[split][i]["db_id"] for i in range(len(spider[split]))])
    
    unique_db_ids = list(dict.fromkeys(db_ids))
    available_dbs = 0
    
    for db_id in unique_db_ids:
        spider_db_path = SPIDER_DB_ROOT / db_id / f"{db_id}.sqlite"
        if spider_db_path.exists() and verify_database_connection(spider_db_path):
            available_dbs += 1
    
    print(f"  Databases: {available_dbs}/{len(unique_db_ids)} available ({100*available_dbs/len(unique_db_ids):.1f}%)")


def bird_dataset_test(bird):
    """Test BIRD dataset for gold SQL and database availability."""
    split = "mini_dev_sqlite"
    total_rows = len(bird[split])
    print(f"  Split: {split}")
    print(f"    {total_rows} rows")
    
    # Check gold SQL
    has_gold_sql = 0
    for i in range(total_rows):
        row = bird[split][i]
        sql = row.get("query") or row.get("sql") or row.get("SQL") or row.get("gold_sql")
        if sql and sql.strip():
            has_gold_sql += 1
    
    print(f"  Gold SQL: {has_gold_sql}/{total_rows} rows ({100*has_gold_sql/total_rows:.1f}%)")
    
    # Check database availability
    db_ids = [bird[split][i]["db_id"] for i in range(total_rows)]
    unique_db_ids = list(dict.fromkeys(db_ids))
    available_dbs = 0

    for db_id in unique_db_ids:
        bird_db_path = BIRD_DB_ROOT / db_id / f"{db_id}.sqlite"
        if bird_db_path.exists() and verify_database_connection(bird_db_path):
            available_dbs += 1

    print(f"  Databases: {available_dbs}/{len(unique_db_ids)} available ({100*available_dbs/len(unique_db_ids):.1f}%)")


def cosql_dataset_test(cosql):
    """Test CoSQL dataset for gold SQL and database availability."""
    print(f"  Splits: {list(cosql.keys())}")
    
    total_turns = 0
    has_gold_sql = 0
    
    for split in cosql.keys():
        conversations = cosql[split]
        turns_in_split = 0
        
        for conv in conversations:
            interaction = conv.get('interaction', [])
            turns_in_split += len(interaction)
            
            # Check gold SQL for each turn
            for turn in interaction:
                sql = turn.get("query")
                if sql and sql.strip():
                    has_gold_sql += 1
        
        total_turns += turns_in_split
        print(f"    {split}: {len(conversations)} conversations, {turns_in_split} turns")
    
    print(f"  Gold SQL: {has_gold_sql}/{total_turns} turns ({100*has_gold_sql/total_turns:.1f}%)")
    
    # Check database availability
    db_ids = []
    for split in cosql.keys():
        for conv in cosql[split]:
            db_id = conv.get("database_id") or conv.get("db_id")
            if db_id:
                db_ids.append(db_id)
    
    unique_db_ids = list(dict.fromkeys(db_ids))
    available_dbs = 0
    
    for db_id in unique_db_ids:
        cosql_db_path = SPIDER_DB_ROOT / db_id / f"{db_id}.sqlite"
        if cosql_db_path.exists() and verify_database_connection(cosql_db_path):
            available_dbs += 1
    
    print(f"  Databases: {available_dbs}/{len(unique_db_ids)} available ({100*available_dbs/len(unique_db_ids):.1f}%)")


def sparc_dataset_test(sparc):
    """Test SParC dataset for gold SQL and database availability."""
    print(f"  Splits: {list(sparc.keys())}")
    
    total_turns = 0
    has_gold_sql = 0
    
    for split in sparc.keys():
        split_size = len(sparc[split])
        print(f"    {split}: {split_size} conversations")
        
        # Count turns and check gold SQL
        for i in range(split_size):
            row = sparc[split][i]
            queries = row.get("interaction_query") or row.get("queries") or row.get("query")
            
            if isinstance(queries, list):
                total_turns += len(queries)
                for sql in queries:
                    if sql and sql.strip():
                        has_gold_sql += 1
            else:
                # Single turn fallback
                total_turns += 1
                sql = row.get("query") or row.get("sql") or row.get("SQL")
                if sql and sql.strip():
                    has_gold_sql += 1
    
    print(f"  Total turns: {total_turns}")
    print(f"  Gold SQL: {has_gold_sql}/{total_turns} turns ({100*has_gold_sql/total_turns:.1f}%)")
    
    # Check database availability
    db_ids = []
    for split in sparc.keys():
        db_ids.extend([sparc[split][i]["db_id"] for i in range(len(sparc[split]))])
    
    unique_db_ids = list(dict.fromkeys(db_ids))
    available_dbs = 0
    
    for db_id in unique_db_ids:
        sparc_db_path = SPIDER_DB_ROOT / db_id / f"{db_id}.sqlite"
        if sparc_db_path.exists() and verify_database_connection(sparc_db_path):
            available_dbs += 1
    
    print(f"  Databases: {available_dbs}/{len(unique_db_ids)} available ({100*available_dbs/len(unique_db_ids):.1f}%)")


# =============================================================================
# Unified Dataset Test
# =============================================================================

def test_unified_dataset(turns: List[Dict[str, Any]]):
    """Test unified dataset for gold SQL, database availability, and context conversion."""
    print("\n" + "=" * 80)
    print("Testing Unified Dataset")
    print("=" * 80)
    
    total_turns = len(turns)
    print(f"\nTotal turns: {total_turns}")
    
    # Test 1: Gold SQL availability
    has_gold_sql = sum(1 for t in turns if t.get("gold_sql") and t.get("gold_sql").strip())
    print(f"1. Gold SQL: {has_gold_sql}/{total_turns} turns ({100*has_gold_sql/total_turns:.1f}%)")

    db_paths = list(dict.fromkeys([
        (t.get("db_file") or get_database_path(t.get("dataset"), t.get("db_id")))
        for t in turns
        if (t.get("db_file") or (t.get("dataset") and t.get("db_id")))
    ]))
    available_dbs = 0
    for p in db_paths:
        if not p:
            continue
        db_path = Path(p)
        if db_path.exists() and verify_database_connection(db_path):
            available_dbs += 1
    db_pct = (100*available_dbs/len(db_paths)) if db_paths else 0.0
    print(f"2. Databases: {available_dbs}/{len(db_paths)} available ({db_pct:.1f}%)")

    # Test 3: Context conversion
    print(f"3. Context Analysis:")
    
    # Single-turn (no context)
    single_turn = sum(1 for t in turns if t.get("turn_index") == 0 and len(t.get("context", [])) == 0)
    
    # Multi-turn with context
    multi_turn_with_context = sum(1 for t in turns if t.get("turn_index", 0) > 0 and len(t.get("context", [])) > 0)
    
    # Multi-turn without context (should be 0 ideally)
    multi_turn_no_context = sum(1 for t in turns if t.get("turn_index", 0) > 0 and len(t.get("context", [])) == 0)
    
    print(f"   - Single-turn (no context): {single_turn} turns ({100*single_turn/total_turns:.1f}%)")
    print(f"   - Multi-turn with context: {multi_turn_with_context} turns ({100*multi_turn_with_context/total_turns:.1f}%)")
    print(f"   - Multi-turn without context: {multi_turn_no_context} turns ({100*multi_turn_no_context/total_turns:.1f}%)")
    
    # Context depth analysis
    if multi_turn_with_context > 0:
        context_lengths = [len(t.get("context", [])) for t in turns if t.get("turn_index", 0) > 0]
        avg_context = sum(context_lengths) / len(context_lengths) if context_lengths else 0
        max_context = max(context_lengths) if context_lengths else 0
        print(f"   - Average context depth: {avg_context:.1f} utterances")
        print(f"   - Max context depth: {max_context} utterances")
    
    # Dataset breakdown
    print(f"\n4. Dataset Breakdown:")
    datasets = {}
    for t in turns:
        ds = t.get("dataset", "unknown")
        if ds not in datasets:
            datasets[ds] = 0
        datasets[ds] += 1
    
    for ds, count in sorted(datasets.items()):
        print(f"   - {ds}: {count} turns ({100*count/total_turns:.1f}%)")
    
    print("\n" + "=" * 80)

