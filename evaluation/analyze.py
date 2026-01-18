"""
Analysis script to compare model evaluation results.

Usage:
    ./venv/bin/python -m evaluation.analyze results/eval_gpt4o.json results/eval_claude.json ...
    ./venv/bin/python -m evaluation.analyze --all  # Analyze all result files
"""

import argparse
import json
from pathlib import Path
from typing import List, Dict

import matplotlib.pyplot as plt
import numpy as np


RESULTS_DIR = Path(__file__).parent.parent / "results"


def load_results(file_path: Path) -> Dict:
    """Load evaluation results from JSON file."""
    with open(file_path) as f:
        return json.load(f)


def extract_metrics(results: Dict) -> Dict:
    """Extract key metrics from evaluation results."""
    model = results.get("config", {}).get("llm_model", "unknown")
    
    # Collect timing and success data
    agent_times = []
    gen_sql_times = []
    gold_sql_times = []
    successes = []
    
    # Handle multi-dataset results
    if "per_dataset_results" in results:
        for dataset, data in results["per_dataset_results"].items():
            details = data.get("details", [])
            for item in details:
                # Single question or conversation turn
                if "timing" in item:
                    timing = item["timing"]
                    agent_times.append(sum(timing.get("agents", {}).values()))
                    gen_sql_times.append(timing.get("generated_sql_ms", 0))
                    gold_sql_times.append(timing.get("gold_sql_ms", 0))
                    successes.append(1 if item.get("passed") else 0)
                elif "turns" in item:
                    # Conversation with multiple turns
                    for turn in item.get("turns", []):
                        if "steps" in turn:
                            agent_time = sum(
                                turn["steps"].get(s, {}).get("latency_ms", 0)
                                for s in ["gatekeeper", "organizer", "planner", "clarifier", "writer", "execute", "validator", "analysis"]
                            )
                            agent_times.append(agent_time)
                            gen_sql_times.append(turn.get("generated_sql_time_ms", 0))
                            gold_sql_times.append(turn.get("gold_sql_time_ms", 0))
                            successes.append(1 if turn.get("passed") else 0)
    else:
        # Single dataset results
        questions = results.get("questions", [])
        for q in questions:
            if "timing" in q:
                timing = q["timing"]
                agent_times.append(sum(timing.get("agents", {}).values()))
                gen_sql_times.append(timing.get("generated_sql_ms", 0))
                gold_sql_times.append(timing.get("gold_sql_ms", 0))
                successes.append(1 if q.get("passed") else 0)
    
    return {
        "model": model,
        "agent_time_avg": np.mean(agent_times) if agent_times else 0,
        "agent_time_std": np.std(agent_times) if agent_times else 0,
        "gen_sql_time_avg": np.mean(gen_sql_times) if gen_sql_times else 0,
        "gen_sql_time_std": np.std(gen_sql_times) if gen_sql_times else 0,
        "gold_sql_time_avg": np.mean(gold_sql_times) if gold_sql_times else 0,
        "success_rate": np.mean(successes) if successes else 0,
        "total_queries": len(successes),
        "passed": sum(successes),
    }


def plot_comparison(metrics_list: List[Dict], output_path: Path = None):
    """Create comparison charts for multiple models."""
    models = [m["model"] for m in metrics_list]
    
    # Shorten model names for display
    display_names = []
    for m in models:
        if "gpt-4o-mini" in m:
            display_names.append("GPT-4o-mini")
        elif "gpt-4o" in m:
            display_names.append("GPT-4o")
        elif "claude-sonnet-4" in m:
            display_names.append("Claude Sonnet 4")
        elif "claude-3-5-haiku" in m:
            display_names.append("Claude Haiku")
        elif "claude-3-5-sonnet" in m:
            display_names.append("Claude Sonnet 3.5")
        elif "o3-mini" in m:
            display_names.append("O3-mini")
        else:
            display_names.append(m[:15])
    
    fig, axes = plt.subplots(1, 3, figsize=(15, 5))
    fig.suptitle("Model Comparison: DBAgent Evaluation", fontsize=14, fontweight="bold")
    
    colors = plt.cm.Set2(np.linspace(0, 1, len(models)))
    
    # 1. Agent Timing (LLM inference time)
    ax1 = axes[0]
    agent_times = [m["agent_time_avg"] for m in metrics_list]
    agent_stds = [m["agent_time_std"] for m in metrics_list]
    bars1 = ax1.bar(display_names, agent_times, yerr=agent_stds, color=colors, capsize=5)
    ax1.set_ylabel("Time (ms)")
    ax1.set_title("Agent Timing\n(LLM inference)")
    ax1.tick_params(axis='x', rotation=45)
    for bar, val in zip(bars1, agent_times):
        ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 100, 
                f'{val:.0f}', ha='center', va='bottom', fontsize=9)
    
    # 2. SQL Query Timing (execution time)
    ax2 = axes[1]
    gen_sql_times = [m["gen_sql_time_avg"] for m in metrics_list]
    gen_sql_stds = [m["gen_sql_time_std"] for m in metrics_list]
    bars2 = ax2.bar(display_names, gen_sql_times, yerr=gen_sql_stds, color=colors, capsize=5)
    ax2.set_ylabel("Time (ms)")
    ax2.set_title("Generated SQL Execution\n(query runtime)")
    ax2.tick_params(axis='x', rotation=45)
    for bar, val in zip(bars2, gen_sql_times):
        ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1, 
                f'{val:.1f}', ha='center', va='bottom', fontsize=9)
    
    # 3. Success Rate
    ax3 = axes[2]
    success_rates = [m["success_rate"] * 100 for m in metrics_list]
    bars3 = ax3.bar(display_names, success_rates, color=colors)
    ax3.set_ylabel("Success Rate (%)")
    ax3.set_title("Accuracy\n(execution match)")
    ax3.set_ylim(0, 100)
    ax3.tick_params(axis='x', rotation=45)
    for bar, m in zip(bars3, metrics_list):
        ax3.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 2, 
                f'{m["passed"]}/{m["total_queries"]}', ha='center', va='bottom', fontsize=9)
    
    plt.tight_layout()
    
    if output_path:
        plt.savefig(output_path, dpi=150, bbox_inches='tight')
        print(f"Chart saved to: {output_path}")
    
    plt.show()


def extract_errors(results: Dict) -> List[Dict]:
    """Extract all errors from evaluation results."""
    errors = []
    model = results.get("config", {}).get("llm_model", "unknown")
    
    def categorize_error(error_msg: str) -> str:
        """Categorize error into types."""
        if not error_msg:
            return "Results mismatch"
        error_lower = error_msg.lower()
        if "no such column" in error_lower:
            return "Wrong column"
        elif "no such table" in error_lower:
            return "Wrong table"
        elif "syntax error" in error_lower:
            return "SQL syntax error"
        elif "ambiguous column" in error_lower:
            return "Ambiguous column"
        elif "results do not match" in error_lower:
            return "Results mismatch"
        elif "generated sql failed" in error_lower:
            return "SQL execution failed"
        elif "gold sql failed" in error_lower:
            return "Gold SQL failed"
        elif "pipeline error" in error_lower:
            return "Pipeline error"
        elif "illegal" in error_lower or "not a valid" in error_lower:
            return "Blocked by gatekeeper"
        else:
            return "Other"
    
    # Handle multi-dataset results
    if "per_dataset_results" in results:
        for dataset, data in results["per_dataset_results"].items():
            details = data.get("details", [])
            for item in details:
                if "timing" in item and not item.get("passed"):
                    gen_sql = item.get("generated_sql") or ""
                    gold_sql = item.get("gold_sql") or ""
                    errors.append({
                        "model": model,
                        "dataset": dataset,
                        "question": (item.get("question") or "")[:60],
                        "error": item.get("error") or "Results mismatch",
                        "category": categorize_error(item.get("error") or ""),
                        "generated_sql": gen_sql[:100],
                        "gold_sql": gold_sql[:100],
                    })
                elif "turns" in item:
                    for turn in item.get("turns", []):
                        if not turn.get("passed"):
                            gen_sql = turn.get("generated_sql") or ""
                            gold_sql = turn.get("gold_sql") or ""
                            errors.append({
                                "model": model,
                                "dataset": dataset,
                                "question": (turn.get("question") or "")[:60],
                                "error": turn.get("error") or "Results mismatch",
                                "category": categorize_error(turn.get("error") or ""),
                                "generated_sql": gen_sql[:100],
                                "gold_sql": gold_sql[:100],
                            })
    else:
        questions = results.get("questions", [])
        dataset = results.get("config", {}).get("dataset", "unknown")
        for q in questions:
            if not q.get("passed"):
                gen_sql = q.get("generated_sql") or ""
                gold_sql = q.get("gold_sql") or ""
                errors.append({
                    "model": model,
                    "dataset": dataset,
                    "question": (q.get("question") or "")[:60],
                    "error": q.get("error") or "Results mismatch",
                    "category": categorize_error(q.get("error") or ""),
                    "generated_sql": gen_sql[:100],
                    "gold_sql": gold_sql[:100],
                })
    
    return errors


def print_error_analysis(all_results: List[Dict], output_dir: Path = None):
    """Print error analysis table and save to file."""
    from collections import defaultdict
    
    # Collect all errors
    all_errors = []
    for results in all_results:
        all_errors.extend(extract_errors(results))
    
    if not all_errors:
        print("\nNo errors to analyze!")
        return {}, {}
    
    # Group by model and category
    model_categories = defaultdict(lambda: defaultdict(int))
    dataset_categories = defaultdict(lambda: defaultdict(int))
    
    for err in all_errors:
        model_categories[err["model"]][err["category"]] += 1
        dataset_categories[err["dataset"]][err["category"]] += 1
    
    # Get all unique categories
    all_categories = sorted(set(err["category"] for err in all_errors))
    
    # Build output lines
    lines = []
    
    # Error by model
    lines.append("=" * 100)
    lines.append("ERROR ANALYSIS BY MODEL")
    lines.append("=" * 100)
    
    header = f"{'Model':<25}"
    for cat in all_categories:
        header += f" {cat[:12]:<12}"
    header += f" {'Total':<8}"
    lines.append(header)
    lines.append("-" * 100)
    
    for model in sorted(model_categories.keys()):
        row = f"{model[:24]:<25}"
        total = 0
        for cat in all_categories:
            count = model_categories[model][cat]
            total += count
            row += f" {count:<12}"
        row += f" {total:<8}"
        lines.append(row)
    
    # Error by dataset
    lines.append("")
    lines.append("=" * 100)
    lines.append("ERROR ANALYSIS BY DATASET")
    lines.append("=" * 100)
    
    header = f"{'Dataset':<15}"
    for cat in all_categories:
        header += f" {cat[:12]:<12}"
    header += f" {'Total':<8}"
    lines.append(header)
    lines.append("-" * 100)
    
    for dataset in sorted(dataset_categories.keys()):
        row = f"{dataset:<15}"
        total = 0
        for cat in all_categories:
            count = dataset_categories[dataset][cat]
            total += count
            row += f" {count:<12}"
        row += f" {total:<8}"
        lines.append(row)
    
    # Sample errors
    lines.append("")
    lines.append("=" * 100)
    lines.append("SAMPLE ERRORS (first 10)")
    lines.append("=" * 100)
    
    for i, err in enumerate(all_errors[:10]):
        lines.append(f"\n[{i+1}] {err['model'][:20]} | {err['dataset']} | {err['category']}")
        lines.append(f"    Q: {err['question']}...")
        lines.append(f"    Error: {err['error'][:80]}")
        if err['generated_sql']:
            lines.append(f"    Gen SQL: {err['generated_sql'][:80]}...")
    
    lines.append("")
    lines.append("=" * 100)
    
    # Print to console
    print("\n" + "\n".join(lines))
    
    # Save to file
    if output_dir:
        report_path = output_dir / "error_analysis.txt"
        with open(report_path, "w") as f:
            f.write("\n".join(lines))
        print(f"Error analysis saved to: {report_path}")
    
    return dict(model_categories), dict(dataset_categories)


def plot_error_analysis(model_categories: Dict, dataset_categories: Dict, output_path: Path = None):
    """Create error analysis charts."""
    if not model_categories or not dataset_categories:
        return
    
    # Get all categories
    all_categories = set()
    for cats in model_categories.values():
        all_categories.update(cats.keys())
    all_categories = sorted(all_categories)
    
    # Shorten model names
    def shorten_model(name):
        if "gpt-4o-mini" in name:
            return "GPT-4o-mini"
        elif "gpt-4o" in name:
            return "GPT-4o"
        elif "claude-sonnet-4" in name:
            return "Claude Sonnet 4"
        elif "claude-3-5-haiku" in name:
            return "Claude Haiku"
        return name[:15]
    
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    fig.suptitle("Error Analysis", fontsize=14, fontweight="bold")
    
    # 1. Errors by model (stacked bar)
    ax1 = axes[0]
    models = sorted(model_categories.keys())
    model_names = [shorten_model(m) for m in models]
    x = np.arange(len(models))
    width = 0.6
    
    bottom = np.zeros(len(models))
    colors = plt.cm.Set3(np.linspace(0, 1, len(all_categories)))
    
    for i, cat in enumerate(all_categories):
        values = [model_categories[m].get(cat, 0) for m in models]
        ax1.bar(x, values, width, label=cat, bottom=bottom, color=colors[i])
        bottom += values
    
    ax1.set_ylabel("Error Count")
    ax1.set_title("Errors by Model")
    ax1.set_xticks(x)
    ax1.set_xticklabels(model_names, rotation=45, ha="right")
    ax1.legend(loc="upper right", fontsize=8)
    
    # 2. Errors by dataset (stacked bar)
    ax2 = axes[1]
    datasets = sorted(dataset_categories.keys())
    x = np.arange(len(datasets))
    
    bottom = np.zeros(len(datasets))
    for i, cat in enumerate(all_categories):
        values = [dataset_categories[d].get(cat, 0) for d in datasets]
        ax2.bar(x, values, width, label=cat, bottom=bottom, color=colors[i])
        bottom += values
    
    ax2.set_ylabel("Error Count")
    ax2.set_title("Errors by Dataset")
    ax2.set_xticks(x)
    ax2.set_xticklabels([d.upper() for d in datasets], rotation=45, ha="right")
    ax2.legend(loc="upper right", fontsize=8)
    
    plt.tight_layout()
    
    if output_path:
        plt.savefig(output_path, dpi=150, bbox_inches="tight")
        print(f"Error analysis chart saved to: {output_path}")
    
    plt.show()


def print_summary(metrics_list: List[Dict]):
    """Print summary table to console."""
    print("\n" + "="*80)
    print("MODEL COMPARISON SUMMARY")
    print("="*80)
    print(f"{'Model':<25} {'Queries':<10} {'Passed':<10} {'Accuracy':<12} {'Agent Time':<15} {'SQL Time':<12}")
    print("-"*80)
    
    for m in metrics_list:
        model = m["model"][:24]
        print(f"{model:<25} {m['total_queries']:<10} {m['passed']:<10} {m['success_rate']*100:>6.1f}%     {m['agent_time_avg']:>8.0f}ms     {m['gen_sql_time_avg']:>6.1f}ms")
    
    print("="*80)


def main():
    parser = argparse.ArgumentParser(description="Analyze and compare model evaluation results")
    parser.add_argument("files", nargs="*", help="Result JSON files to analyze")
    parser.add_argument("--all", action="store_true", help="Analyze all result files in results/")
    parser.add_argument("--output", "-o", type=str, help="Output path for chart image")
    args = parser.parse_args()
    
    # Collect files
    if args.all:
        files = list(RESULTS_DIR.glob("eval_*.json"))
    elif args.files:
        files = [Path(f) for f in args.files]
    else:
        print("Usage: python -m evaluation.analyze --all")
        print("   or: python -m evaluation.analyze results/file1.json results/file2.json")
        return
    
    if not files:
        print("No result files found.")
        return
    
    print(f"Analyzing {len(files)} result files...")
    
    # Load and extract metrics
    metrics_list = []
    all_results = []
    for f in sorted(files):
        try:
            results = load_results(f)
            all_results.append(results)
            metrics = extract_metrics(results)
            metrics_list.append(metrics)
            print(f"  ✓ {f.name}: {metrics['model']}")
        except Exception as e:
            print(f"  ✗ {f.name}: {e}")
    
    if not metrics_list:
        print("No valid results to analyze.")
        return
    
    # Print summary
    print_summary(metrics_list)
    
    # Print error analysis and save to file
    model_cats, dataset_cats = print_error_analysis(all_results, output_dir=RESULTS_DIR)
    
    # Generate comparison chart
    output_path = Path(args.output) if args.output else RESULTS_DIR / "comparison.png"
    plot_comparison(metrics_list, output_path)
    
    # Generate error analysis chart
    error_chart_path = RESULTS_DIR / "error_analysis.png"
    plot_error_analysis(model_cats, dataset_cats, error_chart_path)


if __name__ == "__main__":
    main()
