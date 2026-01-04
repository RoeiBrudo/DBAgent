# UI Module

Chainlit-based interface with custom wrapper for chat, history, and evaluation viewing.

## Overview

The UI wraps Chainlit with a simple custom layout providing three main sections:

```
┌─────────────────────────────────────────────────────────────────┐
│  [Chat]  [History]  [Evaluation]              [DB: dropdown ▼]  │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│                      Main Content Area                          │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## Sections

### 1. Chat (Interactive)

Interactive Q&A with any database.

```
┌─────────────────────────────────────────────────────────────┐
│  DB: [dropdown ▼] or [Upload DB]                            │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  User: How many orders were placed in 2017?                 │
│                                                             │
│  ┌─ [▼ Steps] ────────────────────────────────────────┐    │
│  │ ✓ Guardrails: in_scope                             │    │
│  │ ✓ Organizer: tables=[orders], fields=[order_id]    │    │
│  │ ✓ Planner: Count orders where year = 2017          │    │
│  │ ✓ Writer: SELECT COUNT(*) FROM orders WHERE...     │    │
│  │ ✓ Execute: 1.2ms, 1 row                            │    │
│  │ ✓ Analysis: done                                   │    │
│  └────────────────────────────────────────────────────┘    │
│                                                             │
│  Assistant: There were 45,321 orders placed in 2017.        │
│                                                             │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ 📊 [Interactive Plotly Chart]                        │  │
│  └──────────────────────────────────────────────────────┘  │
│                                                             │
│  [Type your question...]                                    │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

**Features:**
- Database selection via dropdown (from `data/` folder) or file upload
- Collapsible debug steps above each answer
- Interactive Plotly visualizations when applicable
- Full conversation context (multi-turn)

### 2. History (Read-only)

Browse past conversations, grouped by database.

```
┌──────────────┬──────────────────────────────────────────────┐
│              │                                              │
│  Databases   │   Conversation: db1.db / chat_1              │
│              │                                              │
│  ▼ db1.db    │   User: How many customers?                  │
│    • chat_1  │   Assistant: There are 99,441 customers...   │
│    • chat_2  │                                              │
│              │   User: Show top 5 by orders                 │
│  ▶ db2.db    │   Assistant: The top 5 customers are...      │
│              │                                              │
│  ▶ db3.db    │   [View full conversation]                   │
│              │                                              │
└──────────────┴──────────────────────────────────────────────┘
```

**Features:**
- Sidebar with databases and their conversations
- Click to view full conversation
- Read-only (no editing)
- Data from SQLite checkpointer

### 3. Evaluation (Read-only)

Browse dataset evaluation results from CLI runs.

```
┌─────────────────────────────────────────────────────────────────┐
│  Experiment: spider_dev_gpt4o_mini                              │
│  Accuracy: 84.7% | Valid SQL: 92.3% | Turns: 1034               │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Filter: [Dataset ▼] [Split ▼] [Match ▼] [Search...]           │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ #1 spider:dev:0                              ✓ Match    │   │
│  │ Q: How many singers do we have?                         │   │
│  │ A: There are 5 singers in the database.                 │   │
│  │ [▼ Details]                                             │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ #2 spider:dev:1                              ✗ No Match │   │
│  │ Q: List all singer names                                │   │
│  │ A: The singers are: John, Jane, ...                     │   │
│  │ [▼ Details]                                             │   │
│  │   Agent SQL: SELECT name FROM singer                    │   │
│  │   Gold SQL:  SELECT Name FROM singer ORDER BY Age       │   │
│  │   Agent: [["John"], ["Jane"], ...]                      │   │
│  │   Gold:  [["Jane"], ["John"], ...]                      │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

**Features:**
- Load results from `results/<experiment>/` JSON files
- Filter by dataset, split, match/no-match
- Expandable details showing agent steps, SQL comparison, result diff
- Metrics summary at top

## Collapsible Debug Steps

Each assistant response has a collapsible "Steps" section showing:

| Step | Shows |
|------|-------|
| **Guardrails** | `in_scope` / `out_of_scope` / `greeting` |
| **Organizer** | Tables, fields, joins identified |
| **Planner** | Plain English SQL plan |
| **Writer** | Generated SQL query |
| **Execute** | Success/error, elapsed time, row count |
| **Error Agent** | (if triggered) Retry attempt, fix applied |
| **Analysis** | Final answer generation |
| **Viz Decision** | Whether graph was needed, type |
| **Viz Agent** | (if triggered) Graph generation status |

## Tech Stack

- **Chainlit**: Core chat framework
- **Custom wrapper**: HTML/CSS layout for tabs and sidebar
- **SQLite checkpointer**: Conversation persistence
- **Plotly**: Interactive visualizations

## Directory Structure

```
ui/
├── README.md             # This file
├── app.py                # Chainlit app entry point
├── layout.py             # Custom wrapper layout
├── components/
│   ├── chat.py           # Chat section
│   ├── history.py        # History browser
│   └── evaluation.py     # Evaluation viewer
├── static/
│   └── style.css         # Custom styles
└── chainlit.md           # Chainlit welcome message
```

## Usage

### Start the UI

```bash
chainlit run ui/app.py
```

Opens at `http://localhost:8000`

### Configuration

Environment variables:
- `OPENAI_API_KEY`: Required for agent
- `DB_FOLDER`: Path to databases (default: `data/`)
- `RESULTS_FOLDER`: Path to evaluation results (default: `results/`)
