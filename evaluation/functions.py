
import json
import shutil
import time
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

import yaml

from agents.agent_old import BasicAgent
from agents.schemes import AgentResult
from agents.tools.db_tools import safe_query
from data.main import load_data, get_db_connection


def load_yaml_config(config_path: str) -> Dict[str, Any]:
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    return yaml.safe_load(path.read_text(encoding="utf-8"))


def summarize_turns(turns: List[Dict[str, Any]]) -> Dict[str, Any]:
    datasets = [t.get("dataset") for t in turns if t.get("dataset") is not None]
    splits = [t.get("split") for t in turns if t.get("split") is not None]
    db_files = [t.get("db_file") for t in turns if t.get("db_file")]

    return {
        "num_turns": len(turns),
        "datasets": dict(Counter(datasets)),
        "splits": dict(Counter(splits)),
        "unique_dbs": len(set(db_files)),
    }


def load_turns_from_config(cfg: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    data_cfg = cfg.get("data", {}) or {}

    turns = load_data(
        source=data_cfg.get("source"),
        split=data_cfg.get("split"),
        limit=data_cfg.get("limit"),
        min_turn_index=data_cfg.get("min_turn_index"),
    )
    return turns, summarize_turns(turns)


def _canonicalize_rows(rows: List[Any]) -> List[str]:
    canon = []
    for r in rows:
        if isinstance(r, (list, tuple)):
            canon.append(repr(tuple(r)))
        else:
            canon.append(repr((r,)))
    return sorted(canon)


def compare_results(pred_rows: List[Any], gold_rows: List[Any], order_insensitive: bool = True) -> bool:
    if order_insensitive:
        return _canonicalize_rows(pred_rows) == _canonicalize_rows(gold_rows)
    return pred_rows == gold_rows


def run_experiment(config_path: str) -> Path:
    cfg = load_yaml_config(config_path)
    experiment_name = str(cfg.get("experiment_name", "experiment")).strip() or "experiment"
    output_root = Path(str(cfg.get("output_dir", "results")))

    agent_cfg = cfg.get("agent", {}) or {}
    model = agent_cfg.get("model", "gpt-4o-mini")
    msx_ms = int(agent_cfg.get("msx_ms", 30000))
    max_steps = int(agent_cfg.get("max_steps", 4))

    eval_cfg = cfg.get("eval", {}) or {}
    order_insensitive = bool(eval_cfg.get("compare_order_insensitive", True))

    turns, data_summary = load_turns_from_config(cfg)
    agent = BasicAgent(model=model)

    started_at = datetime.utcnow().isoformat() + "Z"
    items = []

    match_count = 0
    comparable_count = 0
    pred_query_time_sum_ms = 0.0
    gold_query_time_sum_ms = 0.0
    pred_query_time_count = 0
    gold_query_time_count = 0

    for idx, t in enumerate(turns):
        question = t.get("text") or t.get("question") or ""
        db_file = t.get("db_file")
        turn_uid = t.get("turn_uid") or t.get("id") or idx
        gold_sql = t.get("gold_sql")

        agent_started = time.monotonic()
        agent_result: AgentResult = agent.run(
            question=question,
            db_file=db_file,
            msx_ms=msx_ms,
            max_steps=max_steps,
        )
        agent_wall_ms = (time.monotonic() - agent_started) * 1000.0

        pred_sql = agent_result.steps[-1].sql if agent_result.steps else ""
        pred_exec = agent_result.steps[-1].execution.to_dict() if agent_result.steps else None
        pred_rows = (
            agent_result.steps[-1].execution.results if agent_result.steps else []
        )
        pred_query_time_ms = None
        if isinstance(pred_exec, dict) and pred_exec.get("elapsed_ms") is not None:
            pred_query_time_ms = float(pred_exec.get("elapsed_ms"))
            pred_query_time_sum_ms += pred_query_time_ms
            pred_query_time_count += 1

        gold_exec = None
        gold_rows: List[Any] = []
        results_match = None
        gold_query_time_ms = None

        if db_file and gold_sql and str(gold_sql).strip():
            comparable_count += 1
            conn = get_db_connection(db_file)
            try:
                gold_exec = safe_query(conn, str(gold_sql), msx_ms)
            finally:
                conn.close()

            gold_rows = gold_exec.get("results", []) if isinstance(gold_exec, dict) else []

            if isinstance(gold_exec, dict) and gold_exec.get("elapsed_ms") is not None:
                gold_query_time_ms = float(gold_exec.get("elapsed_ms"))
                gold_query_time_sum_ms += gold_query_time_ms
                gold_query_time_count += 1

            if pred_exec is not None and isinstance(gold_exec, dict):
                if bool(pred_exec.get("success")) and bool(gold_exec.get("success")):
                    results_match = compare_results(pred_rows, gold_rows, order_insensitive)
                    if results_match:
                        match_count += 1
                else:
                    results_match = False

        items.append(
            {
                "turn_uid": turn_uid,
                "db_file": db_file,
                "question": question,
                "gold_sql": gold_sql,
                "agent_wall_ms": agent_wall_ms,
                "agent_result": agent_result.to_dict(),
                "pred_sql": pred_sql,
                "pred_execution": pred_exec,
                "pred_query_time_ms": pred_query_time_ms,
                "gold_execution": gold_exec,
                "gold_query_time_ms": gold_query_time_ms,
                "query_time_delta_ms": (pred_query_time_ms - gold_query_time_ms)
                if (pred_query_time_ms is not None and gold_query_time_ms is not None)
                else None,
                "results_match": results_match,
            }
        )

    finished_at = datetime.utcnow().isoformat() + "Z"
    accuracy = (match_count / comparable_count) if comparable_count else None

    results_payload = {
        "experiment_name": experiment_name,
        "started_at": started_at,
        "finished_at": finished_at,
        "config": cfg,
        "data_summary": data_summary,
        "metrics": {
            "comparable": comparable_count,
            "matches": match_count,
            "accuracy": accuracy,
            "pred_query_time_avg_ms": (pred_query_time_sum_ms / pred_query_time_count) if pred_query_time_count else None,
            "gold_query_time_avg_ms": (gold_query_time_sum_ms / gold_query_time_count) if gold_query_time_count else None,
        },
        "items": items,
    }

    exp_dir = output_root / experiment_name
    exp_dir.mkdir(parents=True, exist_ok=True)

    shutil.copyfile(str(config_path), str(exp_dir / "config.yaml"))
    (exp_dir / "results.json").write_text(
        json.dumps(results_payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    return exp_dir

