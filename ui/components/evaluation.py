"""
Evaluation viewer component for browsing CLI evaluation results.
"""

import json
from pathlib import Path
from typing import Dict, List, Optional

import chainlit as cl

RESULTS_FOLDER = Path("results")


async def render_evaluation_view():
    """Render the evaluation results viewer."""
    results_files = list(RESULTS_FOLDER.glob("eval_*.json"))
    
    if not results_files:
        await cl.Message(
            content="📊 **Evaluation Results**\n\nNo evaluation results found. Run evaluations first:\n```bash\n./venv/bin/python -m evaluation.runner --datasets spider --samples 10\n```"
        ).send()
        return
    
    # Build summary of available results
    content = "📊 **Evaluation Results**\n\n"
    content += "Available experiments:\n\n"
    
    for f in sorted(results_files):
        try:
            data = json.loads(f.read_text())
            model = data.get("config", {}).get("llm_model", "unknown")
            
            # Get summary stats
            if "per_dataset_results" in data:
                total = sum(d.get("total", 0) for d in data["per_dataset_results"].values())
                passed = sum(d.get("passed", 0) for d in data["per_dataset_results"].values())
            else:
                total = data.get("summary", {}).get("total", 0)
                passed = data.get("summary", {}).get("passed", 0)
            
            accuracy = (passed / total * 100) if total > 0 else 0
            
            content += f"- **{f.stem}** ({model}): {passed}/{total} ({accuracy:.1f}%)\n"
        except Exception as e:
            content += f"- **{f.stem}**: Error loading - {e}\n"
    
    content += "\n*Click on an experiment name to view details (coming soon)*"
    
    await cl.Message(content=content).send()


async def load_evaluation_results(experiment_name: str) -> Optional[Dict]:
    """Load evaluation results for a specific experiment."""
    file_path = RESULTS_FOLDER / f"{experiment_name}.json"
    if not file_path.exists():
        return None
    
    return json.loads(file_path.read_text())


async def render_evaluation_detail(experiment_name: str):
    """Render detailed view of an evaluation experiment."""
    data = await load_evaluation_results(experiment_name)
    if not data:
        await cl.Message(content=f"❌ Experiment '{experiment_name}' not found.").send()
        return
    
    config = data.get("config", {})
    model = config.get("llm_model", "unknown")
    
    content = f"## {experiment_name}\n\n"
    content += f"**Model**: {model}\n\n"
    
    # Per-dataset breakdown
    if "per_dataset_results" in data:
        content += "### Results by Dataset\n\n"
        for dataset, results in data["per_dataset_results"].items():
            passed = results.get("passed", 0)
            total = results.get("total", 0)
            accuracy = (passed / total * 100) if total > 0 else 0
            content += f"- **{dataset}**: {passed}/{total} ({accuracy:.1f}%)\n"
    
    await cl.Message(content=content).send()
