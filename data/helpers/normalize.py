from typing import List, Dict, Any, Iterable

from data.helpers.utils import make_turn_uid


def normalize_single_turn_rows(
        rows: Iterable[Dict[str, Any]],
        *,
        dataset: str,
        split: str,
        start_index: int = 0,
) -> List[Dict[str, Any]]:
    """
    Normalize single-turn dataset rows (e.g., Spider, BIRD).

    Each row becomes one turn with turn_index=0 and empty context.
    """
    out: List[Dict[str, Any]] = []

    for i, r in enumerate(rows, start=start_index):
        db_id = r.get("db_id") or r.get("database_id")
        text = r.get("question") or r.get("utterance") or r.get("nl_question")
        gold_sql = r.get("query") or r.get("sql") or r.get("SQL") or r.get("gold_sql")

        conversation_id = f"{dataset}:{split}:{i}"
        turn_index = 0

        out.append({
            "turn_uid": make_turn_uid(dataset, split, conversation_id, turn_index),
            "dataset": dataset,
            "split": split,
            "conversation_id": conversation_id,
            "turn_index": turn_index,
            "db_id": db_id,
            "dialect": "sqlite",
            "text": text,
            "context": [],
            "context_gold_sql": [],
            "gold_sql": gold_sql,
            "difficulty": r.get("difficulty") or r.get("hardness") or r.get("level"),
        })
    return out


def normalize_conversation_rows(
        conversations: Iterable[Dict[str, Any]],
        *,
        dataset: str,
        split: str,
) -> List[Dict[str, Any]]:
    """
    Normalize conversation dataset (CoSQL format with 'interaction' field).

    Each conversation has multiple turns with context.
    """
    out: List[Dict[str, Any]] = []

    for conv_idx, conv in enumerate(conversations):
        db_id = conv.get("database_id") or conv.get("db_id")
        interaction_id = conv.get("interaction_id") or f"{dataset}:{split}:{conv_idx}"
        conversation_id = str(interaction_id)

        interaction = conv.get("interaction", [])
        prev_utterances: List[str] = []
        prev_gold_sqls: List[str] = []
        prev_events: List[Dict[str, str]] = []

        for turn_idx, turn in enumerate(interaction):
            text = turn.get("utterance", "")
            gold_sql = turn.get("query")

            out.append({
                "turn_uid": make_turn_uid(dataset, split, conversation_id, turn_idx),
                "dataset": dataset,
                "split": split,
                "conversation_id": conversation_id,
                "turn_index": turn_idx,
                "db_id": db_id,
                "dialect": "sqlite",
                "text": text,
                "context": prev_events.copy(),
                "context_gold_sql": prev_gold_sqls.copy(),
                "gold_sql": gold_sql,
                "difficulty": None,
            })
            prev_utterances.append(text)
            prev_events.append({"type": "text", "value": text})
            if gold_sql:
                prev_gold_sqls.append(gold_sql)
                prev_events.append({"type": "sql", "value": gold_sql})

    return out


def spider_dataset_normalize(spider) -> List[Dict[str, Any]]:
    """Normalize Spider dataset (single-turn)."""
    print("  Normalizing Spider...")
    turn_rows = []
    for split in spider.keys():
        rows = (spider[split][i] for i in range(len(spider[split])))
        turn_rows.extend(normalize_single_turn_rows(rows, dataset="spider", split=split))
    print(f"    {len(turn_rows)} turns")
    return turn_rows


def bird_dataset_normalize(bird) -> List[Dict[str, Any]]:
    """Normalize BIRD dataset (single-turn with evidence)."""
    print("  Normalizing BIRD...")
    turn_rows = []
    BIRD_SPLIT = "mini_dev_sqlite"  # Original HF split name
    rows = (bird[BIRD_SPLIT][i] for i in range(len(bird[BIRD_SPLIT])))
    turn_rows.extend(
        normalize_single_turn_rows(
            rows, dataset="bird", split="validation"  # Map to validation
        )
    )
    print(f"    {len(turn_rows)} turns")
    return turn_rows


def cosql_dataset_normalize(cosql) -> List[Dict[str, Any]]:
    """Normalize CoSQL dataset (multi-turn conversations)."""
    print("  Normalizing CoSQL...")
    turn_rows = []
    for original_split in cosql.keys():
        # Map 'dev' to 'validation' for consistency
        split = "validation" if original_split == "dev" else original_split
        turn_rows.extend(
            normalize_conversation_rows(cosql[original_split], dataset="cosql", split=split)
        )
    print(f"    {len(turn_rows)} turns")
    return turn_rows


def sparc_dataset_normalize(sparc) -> List[Dict[str, Any]]:
    """Normalize SParC dataset (multi-turn conversations)."""
    print("  Normalizing SParC...")
    turn_rows = []
    for split in sparc.keys():
        for i in range(len(sparc[split])):
            r = sparc[split][i]
            # Check if it's conversational format
            is_conv = any(
                isinstance(r.get(k), list)
                for k in
                ["interaction", "turns", "dialogue", "questions", "queries", "utterances", "interaction_utterance"]
            )
            if is_conv:
                # SParC uses parallel lists format
                db_id = r.get("db_id") or r.get("database_id")
                questions = r.get("interaction_utterance") or r.get("questions") or r.get("utterances")
                queries = r.get("interaction_query") or r.get("queries") or r.get("query")

                if isinstance(questions, list):
                    conversation_id = f"sparc:{split}:{i}"
                    prev_utterances: List[str] = []
                    prev_gold_sqls: List[str] = []
                    prev_events: List[Dict[str, str]] = []

                    for t, q in enumerate(questions):
                        current_gold_sql = queries[t] if isinstance(queries, list) and t < len(queries) else None
                        turn_rows.append({
                            "turn_uid": make_turn_uid("sparc", split, conversation_id, t),
                            "dataset": "sparc",
                            "split": split,
                            "conversation_id": conversation_id,
                            "turn_index": t,
                            "db_id": db_id,
                            "dialect": "sqlite",
                            "text": q,
                            "context": prev_events.copy(),
                            "context_gold_sql": prev_gold_sqls.copy(),
                            "gold_sql": current_gold_sql,
                            "difficulty": None,
                        })
                        prev_utterances.append(q)
                        prev_events.append({"type": "text", "value": q})
                        if current_gold_sql:
                            prev_gold_sqls.append(current_gold_sql)
                            prev_events.append({"type": "sql", "value": current_gold_sql})
            else:
                # Single turn fallback
                turn_rows.extend(normalize_single_turn_rows([r], dataset="sparc", split=split, start_index=i))

    print(f"    {len(turn_rows)} turns")
    return turn_rows


