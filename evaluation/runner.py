"""
Evaluation runner CLI.

Usage:
    ./venv/bin/python -m evaluation.runner --name my_eval --datasets bird --samples 10
    ./venv/bin/python -m evaluation.runner --name multi_eval --datasets bird cosql --samples 5
    ./venv/bin/python -m evaluation.runner --name cosql_conv --datasets cosql --samples 5 --full-conversation
"""

import argparse
import json
import sys
import time
from datetime import datetime
from pathlib import Path

from langchain_core.messages import HumanMessage


class OutOfCreditsError(Exception):
    """Raised when API credits are exhausted."""
    pass


class LowCreditsWarning(Exception):
    """Raised when API credits are running low."""
    pass


def check_api_error(error: Exception) -> None:
    """
    Check if an error is related to API credits/quota.
    Raises OutOfCreditsError if credits are exhausted.
    Prints warning if credits are low.
    """
    error_str = str(error).lower()
    
    # Out of credits - stop execution
    if any(phrase in error_str for phrase in [
        "insufficient_quota",
        "you exceeded your current quota",
        "billing hard limit",
        "rate_limit_exceeded" and "quota" in error_str,
    ]):
        print("\n" + "="*60)
        print("⚠️  OUT OF CREDITS - Stopping evaluation")
        print("="*60)
        raise OutOfCreditsError(str(error))
    
    # Low credits warning (rate limit may indicate approaching limit)
    if "rate_limit" in error_str:
        print("\n⚠️  WARNING: Rate limit hit - credits may be running low")

from agent.state import AgentState
from agent.tools.db_tools import connect_to_db, get_schema_enrichment
from agent.nodes.gatekeeper import gatekeeper_node
from agent.nodes.organizer import organizer_node
from agent.nodes.planner import planner_node
from agent.nodes.clarifier import clarifier_node
from agent.nodes.writer import writer_node
from agent.nodes.execute import execute_node
from agent.nodes.analysis import analysis_node
from agent.nodes.validator import validator_node

from evaluation.datasets import load_dataset, load_conversations, get_db_path, TestCase, Conversation, SUPPORTED_DATASETS
from evaluation.metrics import execution_accuracy, compute_summary

import config


RESULTS_DIR = Path(__file__).parent.parent / "results"


def get_config_dict() -> dict:
    """Get current config as dict for inclusion in results."""
    return {
        "llm_model": config.LLM_MODEL,
        "llm_temperature": config.LLM_TEMPERATURE,
        "sql_timeout_ms": config.SQL_TIMEOUT_MS,
    }


def run_pipeline(test_case: TestCase, dataset: str) -> dict:
    """
    Run the agent pipeline on a single test case.
    
    Returns dict with all mid-step outputs and timing.
    """
    result = {
        "id": test_case.id,
        "db_id": test_case.db_id,
        "question": test_case.question,
        "gold_sql": test_case.gold_sql,
        "difficulty": test_case.difficulty,
        "steps": {},
        "generated_sql": None,
        "passed": False,
        "error": None,
        "total_latency_ms": 0,
    }
    
    total_start = time.time()
    
    # Connect to database
    try:
        db_path = get_db_path(test_case)
        conn, schema = connect_to_db(db_path, read_only=True)
        enrichment = get_schema_enrichment(conn, schema)
    except Exception as e:
        result["error"] = f"DB connection failed: {str(e)}"
        return result
    
    # Initialize state
    state = AgentState(
        messages=[HumanMessage(content=test_case.question)],
        conn=conn,
        schema=schema,
        schema_enrichment=enrichment,
    )
    
    # Add conversation history if available (multi-turn)
    if test_case.conversation_history:
        for msg in test_case.conversation_history:
            if msg["role"] == "user":
                state["messages"].append(HumanMessage(content=msg["content"]))
    
    try:
        # Step 1: Gatekeeper
        step_start = time.time()
        gatekeeper_result = gatekeeper_node(state)
        state.update(gatekeeper_result)
        step_latency = (time.time() - step_start) * 1000
        result["steps"]["gatekeeper"] = {
            "is_legal": gatekeeper_result.get("is_legal"),
            "latency_ms": round(step_latency, 2),
        }
        
        if not gatekeeper_result.get("is_legal", True):
            result["error"] = "Request blocked by gatekeeper"
            result["total_latency_ms"] = round((time.time() - total_start) * 1000, 2)
            conn.close()
            return result
        
        # Step 2: Organizer
        step_start = time.time()
        organizer_result = organizer_node(state)
        state.update(organizer_result)
        step_latency = (time.time() - step_start) * 1000
        data_sources = organizer_result.get("data_sources", {})
        result["steps"]["organizer"] = {
            "tables": data_sources.get("tables", []),
            "fields": data_sources.get("fields", []),
            "joins_count": len(data_sources.get("joins", [])),
            "latency_ms": round(step_latency, 2),
        }
        
        # Step 3: Planner
        step_start = time.time()
        planner_result = planner_node(state)
        state.update(planner_result)
        step_latency = (time.time() - step_start) * 1000
        result["steps"]["planner"] = {
            "plan": planner_result.get("logic_plan", ""),
            "latency_ms": round(step_latency, 2),
        }
        
        # Step 4: Clarifier (analyze output expectations)
        step_start = time.time()
        clarifier_result = clarifier_node(state)
        state.update(clarifier_result)
        step_latency = (time.time() - step_start) * 1000
        result["steps"]["clarifier"] = {
            "clarification": clarifier_result.get("clarification", {}),
            "latency_ms": round(step_latency, 2),
        }
        
        # Step 5: Writer
        step_start = time.time()
        writer_result = writer_node(state)
        state.update(writer_result)
        step_latency = (time.time() - step_start) * 1000
        result["steps"]["writer"] = {
            "sql": writer_result.get("sql_query", ""),
            "latency_ms": round(step_latency, 2),
        }
        result["generated_sql"] = writer_result.get("sql_query", "")
        
        # Check for writer validation error
        if writer_result.get("error"):
            result["error"] = writer_result.get("error")
            result["total_latency_ms"] = round((time.time() - total_start) * 1000, 2)
            conn.close()
            return result
        
        # Step 5: Execute (for timing, but we use metrics for accuracy)
        step_start = time.time()
        execute_result = execute_node(state)
        state.update(execute_result)
        step_latency = (time.time() - step_start) * 1000
        execution = execute_result.get("execution")
        result["steps"]["execute"] = {
            "success": execution.success if execution else False,
            "status": execution.status if execution else "Unknown",
            "latency_ms": round(step_latency, 2),
        }
        
        # Step 6: Validator (check for common issues, retry if needed)
        max_retries = 2
        retry_count = 0
        while retry_count < max_retries:
            step_start = time.time()
            validator_result = validator_node(state)
            state.update(validator_result)
            step_latency = (time.time() - step_start) * 1000
            
            if validator_result.get("validation_passed", True):
                result["steps"]["validator"] = {
                    "passed": True,
                    "retries": retry_count,
                    "latency_ms": round(step_latency, 2),
                }
                break
            
            # Validation failed - retry with fix suggestion
            retry_count += 1
            if retry_count < max_retries:
                # Re-run writer with error context
                state["error"] = validator_result.get("error", "Validation failed")
                state["iteration"] = retry_count
                
                writer_result = writer_node(state)
                state.update(writer_result)
                result["generated_sql"] = writer_result.get("sql_query", "")
                
                # Re-execute
                execute_result = execute_node(state)
                state.update(execute_result)
            else:
                result["steps"]["validator"] = {
                    "passed": False,
                    "error": validator_result.get("error", ""),
                    "retries": retry_count,
                    "latency_ms": round(step_latency, 2),
                }
        
        # Step 7: Analysis
        step_start = time.time()
        analysis_result = analysis_node(state)
        step_latency = (time.time() - step_start) * 1000
        result["steps"]["analysis"] = {
            "answer": analysis_result.get("final_answer", ""),
            "latency_ms": round(step_latency, 2),
        }
        
        # Compare with gold SQL using execution accuracy
        accuracy_result = execution_accuracy(conn, result["generated_sql"], test_case.gold_sql)
        result["passed"] = accuracy_result["passed"]
        result["gold_results"] = accuracy_result["gold_results"][:5] if accuracy_result["gold_results"] else []
        result["generated_results"] = accuracy_result["generated_results"][:5] if accuracy_result["generated_results"] else []
        result["generated_sql_time_ms"] = accuracy_result.get("generated_sql_time_ms", 0)
        result["gold_sql_time_ms"] = accuracy_result.get("gold_sql_time_ms", 0)
        if not accuracy_result["passed"]:
            result["error"] = accuracy_result.get("error")
        
        # Compute timing summary
        result["timing"] = {
            "agents": {
                "gatekeeper_ms": result["steps"].get("gatekeeper", {}).get("latency_ms", 0),
                "organizer_ms": result["steps"].get("organizer", {}).get("latency_ms", 0),
                "planner_ms": result["steps"].get("planner", {}).get("latency_ms", 0),
                "clarifier_ms": result["steps"].get("clarifier", {}).get("latency_ms", 0),
                "writer_ms": result["steps"].get("writer", {}).get("latency_ms", 0),
                "execute_ms": result["steps"].get("execute", {}).get("latency_ms", 0),
                "validator_ms": result["steps"].get("validator", {}).get("latency_ms", 0),
                "analysis_ms": result["steps"].get("analysis", {}).get("latency_ms", 0),
            },
            "generated_sql_ms": result["generated_sql_time_ms"],
            "gold_sql_ms": result["gold_sql_time_ms"],
            "total_workflow_ms": round((time.time() - total_start) * 1000, 2),
        }
        
    except Exception as e:
        check_api_error(e)  # Raises OutOfCreditsError if credits exhausted
        result["error"] = f"Pipeline error: {str(e)}"
    
    result["total_latency_ms"] = round((time.time() - total_start) * 1000, 2)
    conn.close()
    
    return result


def run_conversation(conversation: Conversation, dataset: str) -> dict:
    """
    Run the agent pipeline on a full conversation (all turns sequentially).
    
    Returns dict with conversation-level results and per-turn details.
    """
    result = {
        "id": conversation.id,
        "db_id": conversation.db_id,
        "query_goal": conversation.query_goal,
        "turns_count": len(conversation.turns),
        "turns": [],
        "passed_count": 0,
        "total_latency_ms": 0,
    }
    
    total_start = time.time()
    
    # Connect to database
    try:
        db_path = get_db_path(conversation)
        conn, schema = connect_to_db(db_path, read_only=True)
        enrichment = get_schema_enrichment(conn, schema)
    except Exception as e:
        result["error"] = f"DB connection failed: {str(e)}"
        return result
    
    # Initialize state with empty messages (will accumulate)
    state = AgentState(
        messages=[],
        conn=conn,
        schema=schema,
        schema_enrichment=enrichment,
    )
    
    # Run each turn in sequence, accumulating conversation history
    for turn in conversation.turns:
        turn_start = time.time()
        
        # Add user message to state
        state["messages"].append(HumanMessage(content=turn.question))
        
        turn_result = {
            "turn_index": turn.turn_index,
            "question": turn.question,
            "gold_sql": turn.gold_sql,
            "steps": {},
            "generated_sql": None,
            "passed": False,
            "error": None,
        }
        
        try:
            # Run pipeline steps
            # Step 1: Gatekeeper
            step_start = time.time()
            gatekeeper_result = gatekeeper_node(state)
            state.update(gatekeeper_result)
            turn_result["steps"]["gatekeeper"] = {
                "is_legal": gatekeeper_result.get("is_legal"),
                "latency_ms": round((time.time() - step_start) * 1000, 2),
            }
            
            if not gatekeeper_result.get("is_legal", True):
                turn_result["error"] = "Request blocked by gatekeeper"
                result["turns"].append(turn_result)
                continue
            
            # Step 2: Organizer
            step_start = time.time()
            organizer_result = organizer_node(state)
            state.update(organizer_result)
            data_sources = organizer_result.get("data_sources", {})
            turn_result["steps"]["organizer"] = {
                "tables": data_sources.get("tables", []),
                "joins_count": len(data_sources.get("joins", [])),
                "latency_ms": round((time.time() - step_start) * 1000, 2),
            }
            
            # Step 3: Planner
            step_start = time.time()
            planner_result = planner_node(state)
            state.update(planner_result)
            turn_result["steps"]["planner"] = {
                "plan": planner_result.get("logic_plan", "")[:200] + "...",
                "latency_ms": round((time.time() - step_start) * 1000, 2),
            }
            
            # Step 4: Clarifier
            step_start = time.time()
            clarifier_result = clarifier_node(state)
            state.update(clarifier_result)
            turn_result["steps"]["clarifier"] = {
                "clarification": clarifier_result.get("clarification", {}),
                "latency_ms": round((time.time() - step_start) * 1000, 2),
            }
            
            # Step 5: Writer
            step_start = time.time()
            writer_result = writer_node(state)
            state.update(writer_result)
            turn_result["steps"]["writer"] = {
                "sql": writer_result.get("sql_query", ""),
                "latency_ms": round((time.time() - step_start) * 1000, 2),
            }
            turn_result["generated_sql"] = writer_result.get("sql_query", "")
            
            if writer_result.get("error"):
                turn_result["error"] = writer_result.get("error")
                result["turns"].append(turn_result)
                continue
            
            # Step 5: Execute
            step_start = time.time()
            execute_result = execute_node(state)
            state.update(execute_result)
            execution = execute_result.get("execution")
            turn_result["steps"]["execute"] = {
                "success": execution.success if execution else False,
                "latency_ms": round((time.time() - step_start) * 1000, 2),
            }
            
            # Step 6: Analysis
            step_start = time.time()
            analysis_result = analysis_node(state)
            turn_result["steps"]["analysis"] = {
                "answer": analysis_result.get("final_answer", "")[:200] + "...",
                "latency_ms": round((time.time() - step_start) * 1000, 2),
            }
            
            # Check accuracy
            accuracy_result = execution_accuracy(conn, turn_result["generated_sql"], turn.gold_sql)
            turn_result["passed"] = accuracy_result["passed"]
            if accuracy_result["passed"]:
                result["passed_count"] += 1
            else:
                turn_result["error"] = accuracy_result.get("error")
            
        except Exception as e:
            check_api_error(e)  # Raises OutOfCreditsError if credits exhausted
            turn_result["error"] = f"Pipeline error: {str(e)}"
        
        turn_result["latency_ms"] = round((time.time() - turn_start) * 1000, 2)
        result["turns"].append(turn_result)
    
    result["total_latency_ms"] = round((time.time() - total_start) * 1000, 2)
    result["accuracy"] = result["passed_count"] / len(conversation.turns) if conversation.turns else 0
    conn.close()
    
    return result


def run_evaluation(
    name: str,
    dataset: str,
    samples: int,
    multi_turn: bool = False,
    full_conversation: bool = False,
) -> dict:
    """
    Run full evaluation and save results.
    
    Args:
        name: Name for this evaluation run (used as filename)
        dataset: "bird" or "cosql"
        samples: Number of samples to evaluate
        multi_turn: For CoSQL, include conversation history
        full_conversation: For CoSQL, run entire conversations sequentially
    
    Returns:
        Full results dict
    """
    # Full conversation mode (for multi-turn datasets: cosql, sparc)
    if full_conversation and dataset in ["cosql", "sparc"]:
        print(f"Loading {dataset} conversations ({samples} conversations)...")
        conversations = load_conversations(dataset, sample_size=samples)
        print(f"Loaded {len(conversations)} conversations")
        
        results = {
            "name": name,
            "timestamp": datetime.now().isoformat(),
            "config": get_config_dict(),
            "dataset": dataset,
            "mode": "full_conversation",
            "conversations_count": len(conversations),
            "summary": {},
            "conversations": [],
        }
        
        total_turns = 0
        total_passed = 0
        total_latency = 0
        
        try:
            for i, conv in enumerate(conversations):
                print(f"[{i+1}/{len(conversations)}] {conv.query_goal[:50]}... ({len(conv.turns)} turns)")
                conv_result = run_conversation(conv, dataset)
                results["conversations"].append(conv_result)
                
                total_turns += conv_result["turns_count"]
                total_passed += conv_result["passed_count"]
                total_latency += conv_result["total_latency_ms"]
                
                # Sum timings from turns
                turns = conv_result.get("turns", [])
                total_agents_ms = sum(sum(t.get("steps", {}).get(s, {}).get("latency_ms", 0) for s in ["gatekeeper", "organizer", "planner", "clarifier", "writer", "execute", "validator", "analysis"]) for t in turns)
                gen_sql_ms = sum(t.get("generated_sql_time_ms", 0) for t in turns)
                gold_sql_ms = sum(t.get("gold_sql_time_ms", 0) for t in turns)
                print(f"  {conv_result['passed_count']}/{conv_result['turns_count']} turns | Agents: {total_agents_ms:.0f}ms | Gen SQL: {gen_sql_ms:.0f}ms | Gold SQL: {gold_sql_ms:.0f}ms")
        except OutOfCreditsError:
            print(f"\n🛑 Evaluation stopped early due to credit limit ({len(results['conversations'])}/{len(conversations)} completed)")
            results["stopped_early"] = True
            results["stop_reason"] = "out_of_credits"
        
        results["summary"] = {
            "total_conversations": len(conversations),
            "total_turns": total_turns,
            "passed_turns": total_passed,
            "accuracy": round(total_passed / total_turns, 4) if total_turns > 0 else 0,
            "avg_latency_ms": round(total_latency / len(conversations), 2) if conversations else 0,
        }
        
        # Save results
        RESULTS_DIR.mkdir(exist_ok=True)
        output_path = RESULTS_DIR / f"{name}.json"
        with open(output_path, "w") as f:
            json.dump(results, f, indent=2, default=str)
        
        print(f"\nResults saved to: {output_path}")
        print(f"Summary: {total_passed}/{total_turns} turns passed ({results['summary']['accuracy']*100:.1f}%)")
        
        return results
    
    # Standard single-question mode
    print(f"Loading {dataset} dataset ({samples} samples)...")
    test_cases = load_dataset(dataset, sample_size=samples)
    print(f"Loaded {len(test_cases)} test cases")
    
    results = {
        "name": name,
        "timestamp": datetime.now().isoformat(),
        "config": get_config_dict(),
        "dataset": dataset,
        "mode": "single_question",
        "samples": len(test_cases),
        "multi_turn": multi_turn,
        "summary": {},
        "questions": [],
    }
    
    try:
        for i, test_case in enumerate(test_cases):
            print(f"[{i+1}/{len(test_cases)}] {test_case.question[:50]}...")
            question_result = run_pipeline(test_case, dataset)
            results["questions"].append(question_result)
            
            status = "✓" if question_result["passed"] else "✗"
            timing = question_result.get("timing", {})
            gen_sql_ms = timing.get("generated_sql_ms", 0)
            gold_sql_ms = timing.get("gold_sql_ms", 0)
            total_agents_ms = sum(timing.get("agents", {}).values())
            print(f"  {status} | Agents: {total_agents_ms:.0f}ms | Gen SQL: {gen_sql_ms:.0f}ms | Gold SQL: {gold_sql_ms:.0f}ms")
    except OutOfCreditsError:
        print(f"\n🛑 Evaluation stopped early due to credit limit ({len(results['questions'])}/{len(test_cases)} completed)")
        results["stopped_early"] = True
        results["stop_reason"] = "out_of_credits"
    
    # Compute summary
    results["summary"] = compute_summary(results["questions"])
    
    # Save to results folder
    RESULTS_DIR.mkdir(exist_ok=True)
    output_path = RESULTS_DIR / f"{name}.json"
    with open(output_path, "w") as f:
        json.dump(results, f, indent=2, default=str)
    
    print(f"\nResults saved to: {output_path}")
    print(f"Summary: {results['summary']['passed']}/{results['summary']['total']} passed ({results['summary']['accuracy']*100:.1f}%)")
    
    return results


def run_multi_dataset_evaluation(
    name: str,
    datasets: list,
    samples: int,
    full_conversation: bool = False,
) -> dict:
    """
    Run evaluation across multiple datasets and combine results.
    
    For CoSQL, always uses full conversation mode.
    """
    all_results = {
        "name": name,
        "timestamp": datetime.now().isoformat(),
        "config": get_config_dict(),
        "datasets": datasets,
        "samples_per_dataset": samples,
        "full_conversation_for_cosql": True,
        "per_dataset_results": {},
        "combined_summary": {},
    }
    
    total_questions = 0
    total_passed = 0
    
    stopped_early = False
    try:
        for dataset in datasets:
            print(f"\n{'='*50}")
            print(f"Running {dataset.upper()} evaluation...")
            print(f"{'='*50}")
            
            # For CoSQL/SParC, use full conversation mode
            use_full_conv = (dataset in ["cosql", "sparc"])
            
            result = run_evaluation(
                name=f"{name}_{dataset}",
                dataset=dataset,
                samples=samples,
                multi_turn=False,
                full_conversation=use_full_conv,
            )
            
            all_results["per_dataset_results"][dataset] = {
                "summary": result["summary"],
                "details": result.get("questions") or result.get("conversations", []),
            }
            
            # Aggregate stats
            if use_full_conv:
                total_questions += result["summary"].get("total_turns", 0)
                total_passed += result["summary"].get("passed_turns", 0)
            else:
                total_questions += result["summary"].get("total", 0)
                total_passed += result["summary"].get("passed", 0)
            
            # Delete intermediate file
            intermediate_path = RESULTS_DIR / f"{name}_{dataset}.json"
            if intermediate_path.exists():
                intermediate_path.unlink()
            
            # Check if this result was stopped early
            if result.get("stopped_early"):
                stopped_early = True
                break
    except OutOfCreditsError:
        stopped_early = True
        print("\n🛑 Multi-dataset evaluation stopped due to credit limit")
    
    if stopped_early:
        all_results["stopped_early"] = True
        all_results["stop_reason"] = "out_of_credits"
    
    all_results["combined_summary"] = {
        "total_questions": total_questions,
        "total_passed": total_passed,
        "overall_accuracy": round(total_passed / total_questions, 4) if total_questions > 0 else 0,
    }
    
    # Save final results as {name}.json
    RESULTS_DIR.mkdir(exist_ok=True)
    output_path = RESULTS_DIR / f"{name}.json"
    with open(output_path, "w") as f:
        json.dump(all_results, f, indent=2, default=str)
    
    print(f"\n{'='*50}")
    print(f"COMBINED RESULTS")
    print(f"{'='*50}")
    print(f"Overall: {total_passed}/{total_questions} ({all_results['combined_summary']['overall_accuracy']*100:.1f}%)")
    print(f"Saved to: {output_path}")
    
    return all_results


def run_from_config(config_path: str):
    """
    Run multiple evaluations from a YAML config file.
    
    Config format:
        runs:
          - name: eval_gpt4o
            model: gpt-4o
            datasets: [bird, spider]
            samples: 10
    """
    import yaml
    import config as app_config
    
    with open(config_path) as f:
        cfg = yaml.safe_load(f)
    
    runs = cfg.get("runs", [])
    print(f"Loaded {len(runs)} evaluation runs from config")
    
    for i, run in enumerate(runs):
        name = run.get("name", f"eval_{i}")
        model = run.get("model", "gpt-4o")
        datasets = run.get("datasets", ["spider"])
        samples = run.get("samples", 10)
        full_conversation = run.get("full_conversation", False)
        
        print(f"\n{'='*60}")
        print(f"Run {i+1}/{len(runs)}: {name} (model: {model})")
        print(f"{'='*60}")
        
        # Dynamically set the model for this run
        app_config.LLM_MODEL = model
        
        try:
            if len(datasets) == 1:
                dataset = datasets[0]
                use_full_conv = full_conversation or (dataset in ["cosql", "sparc"])
                run_evaluation(
                    name=name,
                    dataset=dataset,
                    samples=samples,
                    multi_turn=False,
                    full_conversation=use_full_conv,
                )
            else:
                run_multi_dataset_evaluation(
                    name=name,
                    datasets=datasets,
                    samples=samples,
                    full_conversation=full_conversation,
                )
        except OutOfCreditsError:
            print(f"\n🛑 Stopping all runs due to credit limit")
            break
    
    print(f"\n{'='*60}")
    print(f"All evaluation runs complete!")
    print(f"{'='*60}")


def main():
    parser = argparse.ArgumentParser(description="Run Text2SQL evaluation")
    parser.add_argument("--config", "-c", help="Path to YAML config file for batch runs")
    parser.add_argument("--name", help="Name for this evaluation run")
    parser.add_argument("--model", "-m", help="LLM model to use (overrides config.py)")
    parser.add_argument("--datasets", nargs="+", choices=SUPPORTED_DATASETS, help="Datasets to use")
    parser.add_argument("--samples", type=int, default=10, help="Number of samples per dataset")
    parser.add_argument("--full-conversation", action="store_true", help="Run full conversations (for cosql/sparc)")
    
    args = parser.parse_args()
    
    # Config file mode
    if args.config:
        run_from_config(args.config)
        return
    
    # CLI mode - require name and datasets
    if not args.name or not args.datasets:
        parser.error("--name and --datasets are required unless using --config")
    
    # Override model if specified
    if args.model:
        import config as app_config
        app_config.LLM_MODEL = args.model
    
    if len(args.datasets) == 1:
        # Single dataset mode
        dataset = args.datasets[0]
        use_full_conv = args.full_conversation or (dataset in ["cosql", "sparc"])
        run_evaluation(
            name=args.name,
            dataset=dataset,
            samples=args.samples,
            multi_turn=False,
            full_conversation=use_full_conv,
        )
    else:
        # Multi-dataset mode
        run_multi_dataset_evaluation(
            name=args.name,
            datasets=args.datasets,
            samples=args.samples,
            full_conversation=args.full_conversation,
        )


if __name__ == "__main__":
    main()
