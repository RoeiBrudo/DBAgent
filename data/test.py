from typing import List, Dict, Any

from data.main import get_db_connection, get_database_schema, load_data


def test_unified_dataset(turns: List[Dict[str, Any]]):
    print("\n" + "=" * 80)
    print("Testing Unified Dataset")
    print("=" * 80)

    total_turns = len(turns)
    print(f"\nTotal turns: {total_turns}")

    present = sum(1 for t in turns if t.get("gold_sql") and str(t.get("gold_sql")).strip())
    present_pct = (100 * present / total_turns) if total_turns else 0.0
    print(f"1. Gold SQL present: {present}/{total_turns} ({present_pct:.1f}%)")

    unique_dbs: List[str] = []
    seen = set()
    for t in turns:
        dbf = t.get("db_file")
        if dbf and dbf not in seen:
            seen.add(dbf)
            unique_dbs.append(dbf)

    db_ok = 0
    for dbf in unique_dbs:
        conn = get_db_connection(dbf)
        try:
            schema = get_database_schema(conn)
            if isinstance(schema, dict):
                db_ok += 1
        finally:
            conn.close()
    total_dbs = len(unique_dbs)
    db_ok_pct = (100 * db_ok / total_dbs) if total_dbs else 0.0
    print(f"2. Databases OK (connect + schema): {db_ok}/{total_dbs} ({db_ok_pct:.1f}%)")

    print("\n" + "=" * 80)


if __name__ == "__main__":
    test_unified_dataset(load_data())
