# Evaluation Module

Batch evaluation runner for Text-to-SQL datasets. Uses the same agent graph as interactive chat.

## Overview

```
                    ┌─────────────────┐
                    │   Agent Graph   │
                    │   (LangGraph)   │
                    └────────┬────────┘
                             │
              ┌──────────────┴──────────────┐
              │                             │
              ▼                             ▼
     ┌────────────────┐           ┌────────────────┐
     │  Chainlit UI   │           │   CLI Eval     │  ◄── This module
     │  (interactive) │           │ (batch, JSON)  │
     └────────────────┘           └────────────────┘
```

The evaluation module:
1. Loads questions/conversations from datasets (Spider, BIRD, CoSQL, SParC)
2. Runs each turn through the **same agent graph** used by interactive chat
3. Executes gold SQL for comparison
4. Computes metrics
5. Saves results to JSON for review in UI

## How It Works

### Same Infrastructure as Chat

Evaluation doesn't use a separate pipeline. Each dataset turn is injected as a `HumanMessage`, exactly like a user typing in chat:

```python
for conversation in dataset:
    thread_id = f"eval:{conversation.id}"
    
    for turn in conversation.turns:
        # Inject turn as user message (same as chat)
        result = graph.invoke(
            {"messages": [HumanMessage(content=turn.text)]},
            config={"configurable": {"thread_id": thread_id, "db_file": turn.db_file}}
        )
        
        # Compare against gold SQL
        gold_result = safe_query(conn, turn.gold_sql, timeout_ms)
        match = compare_results(result, gold_result)
```

### Multi-turn Support

CoSQL and SParC have multi-turn conversations. These run **with prior context**, just like a normal chat:

```
Turn 1: "Show me all customers"           → agent sees: [turn1]
Turn 2: "Filter by California"            → agent sees: [turn1, turn2]
Turn 3: "Now sort by order count"         → agent sees: [turn1, turn2, turn3]
```

The agent uses conversation history to resolve references like "filter that" or "now sort".

## Metrics

| Metric | Description |
|--------|-------------|
| **Execution Accuracy** | % where agent results == gold results (order-insensitive) |
| **Valid SQL Rate** | % where agent SQL executed without error |
| **Exact Match** | % where agent SQL == gold SQL (strict) |
| **Avg Latency** | Mean wall-clock time per question |
| **Avg Query Time** | Mean SQL execution time (agent vs gold) |
| **Error Breakdown** | Count by error type (syntax, timeout, wrong results) |

## Output Structure

```
results/
└── <experiment_name>/
    ├── config.yaml           # Experiment configuration
    ├── results.json          # Full details per turn
    └── summary.json          # Aggregate metrics
```

### results.json (per item)

```json
{
  "turn_uid": "spider:train:0",
  "conversation_id": "spider:train:concert_singer:0",
  "turn_index": 0,
  "question": "How many singers do we have?",
  "db_file": "data/external/.../concert_singer/concert_singer.sqlite",
  
  "agent": {
    "steps": {
      "guardrails": {"is_in_scope": true},
      "organizer": {"tables": ["singer"], "fields": ["*"], "joins": []},
      "planner": "Count all rows in the singer table",
      "writer": "SELECT COUNT(*) FROM singer",
      "execute": {"success": true, "elapsed_ms": 1.2},
      "analysis": "There are 5 singers in the database."
    },
    "final_sql": "SELECT COUNT(*) FROM singer",
    "final_answer": "There are 5 singers in the database.",
    "results": [[5]],
    "wall_ms": 450
  },
  
  "gold": {
    "sql": "SELECT count(*) FROM singer",
    "execution": {"success": true, "results": [[5]], "elapsed_ms": 0.8}
  },
  
  "comparison": {
    "results_match": true,
    "exact_sql_match": false
  }
}
```

### summary.json

```json
{
  "experiment_name": "spider_dev_gpt4o_mini",
  "started_at": "2024-01-15T10:30:00Z",
  "finished_at": "2024-01-15T11:45:00Z",
  "config": { ... },
  
  "data_summary": {
    "total_turns": 1034,
    "datasets": {"spider": 1034},
    "unique_dbs": 20
  },
  
  "metrics": {
    "execution_accuracy": 0.847,
    "valid_sql_rate": 0.923,
    "exact_match": 0.312,
    "avg_latency_ms": 1250,
    "avg_agent_query_ms": 2.3,
    "avg_gold_query_ms": 1.8
  },
  
  "error_breakdown": {
    "syntax_error": 45,
    "timeout": 3,
    "wrong_results": 112
  }
}
```

## Usage

### CLI

```bash
# Run with default config
python -m evaluation.main

# Run with custom config
python -m evaluation.main evaluation/config.yaml

# Run specific dataset/split
python -m evaluation.main --source spider --split dev --limit 100
```

### Config File

```yaml
experiment_name: spider_dev_run_1

data:
  source: spider          # spider, bird, cosql, sparc, or null for all
  split: dev              # train, dev, or null for all
  limit: null             # max turns to evaluate, null for all
  min_turn_index: 0       # for multi-turn: start from turn N

agent:
  model: gpt-4o-mini
  msx_ms: 30000           # SQL execution timeout
  max_retries: 4          # max error recovery attempts

eval:
  compare_order_insensitive: true
  save_agent_steps: true  # include full step details in results

output_dir: results
```

## Directory Structure

```
evaluation/
├── README.md             # This file
├── __init__.py
├── main.py               # CLI entrypoint
├── runner.py             # Core evaluation loop
├── metrics.py            # Metric computation
├── comparison.py         # Result comparison logic
└── config.yaml           # Default configuration
```

## Viewing Results

Results are viewable in the **Evaluation tab** of the UI (read-only). See `ui/README.md`.
