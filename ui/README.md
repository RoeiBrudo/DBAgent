# UI Module

Chainlit-based UI with a custom top toolbar for database selection, upload, and SQL/Text mode.

## Overview

The UI wraps Chainlit and adds a fixed toolbar injected via Chainlit `custom_js` / `custom_css`.

**Important:** Chainlit reads its config from `ui/.chainlit/config.toml` (this folder is auto-created by Chainlit). The toolbar only appears if `custom_js` and `custom_css` are enabled in that file.

```
┌──────────────────────────────────────────────────────────────────────────────┐
│  DBAgent | Dataset ▼ | Database ▼ | Upload | Current DB | Text/SQL toggle     │
├──────────────────────────────────────────────────────────────────────────────┤
│  Chainlit chat UI (messages + input)                                          │
└──────────────────────────────────────────────────────────────────────────────┘
```

## Current Status (Working)

- **Toolbar renders** at the top of the page.
- **Dataset dropdown** selects `Spider`, `Bird`, or `Upload Custom`.
- **Database dropdown** populates from the server and allows switching DB.
- **Upload** accepts a local `.db` / `.sqlite` file and switches to it.
- **SQL/Text mode** is controlled via a toggle in the toolbar.
- **No chat artifacts**: switching DB / upload / toggling SQL does not create chat messages or “new conversation” prompts.

## Sections

### 1. Chat (Interactive)

Interactive Q&A with any database.

```
┌─────────────────────────────────────────────────────────────┐
│  Toolbar (above): dataset/db selection + upload + SQL toggle │
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
- Database selection + upload via the top toolbar (no chat messages)
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
- **Custom JS/CSS toolbar**: injected via `ui/.chainlit/config.toml` (`custom_js`, `custom_css`)
- **FastAPI endpoints**: mounted on Chainlit’s FastAPI app for toolbar actions
- **SQLite checkpointer**: Conversation persistence
- **Plotly**: Interactive visualizations

## Architecture / Data Flow

### Frontend (Toolbar)

The toolbar is implemented in:

- `ui/public/custom.js`
- `ui/public/custom.css`

Chainlit loads them via `.chainlit/config.toml`:
- When running Chainlit from `ui/` (recommended), Chainlit reads `ui/.chainlit/config.toml`.

- `custom_js = "/public/custom.js"`
- `custom_css = "/public/custom.css"`

The toolbar **does not send chat messages** for UI actions. Instead it calls HTTP endpoints (below) using `fetch(..., { credentials: "include" })`.

### Backend (Chainlit + API)

- `ui/app.py` is the Chainlit entry point.
- It also registers `/api/dbagent/*` endpoints on Chainlit’s FastAPI app (`chainlit.server.app`).

The endpoints update Chainlit’s in-memory session store (`chainlit.user_session.user_sessions`) keyed by the Chainlit session id.

### Session Model

- The browser stores a cookie named `X-Chainlit-Session-id`.
- The toolbar endpoints read that cookie and update `user_sessions[session_id]`.
- On the next user message, `cl.user_session.get(...)` reads from the same `user_sessions` dict (for that session id), so the agent sees the selected DB and mode.

Keys written by the toolbar endpoints:

- `db_name`: e.g. `spider/concert_singer` or `uploaded/my.db`
- `db_path`: absolute path to the SQLite file
- `sql_mode`: boolean
- `conn`: reset to `None` so the agent will reconnect as needed

### HTTP API (Toolbar)

Implemented in `ui/app.py`:

- `GET /api/dbagent/datasets`
  - Returns: `{ "spider": [..], "bird": [..] }`
- `GET /api/dbagent/state`
  - Returns: `{ "db_name": str|null, "db_path": str|null, "sql_mode": bool }`
- `POST /api/dbagent/switch-db`
  - Body: `{ "dataset": "spider"|"bird", "db": "<db_name>" }`
  - Side effects: sets `db_name`, `db_path`
- `POST /api/dbagent/sql-mode`
  - Body: `{ "enabled": true|false }`
  - Side effects: sets `sql_mode`
- `POST /api/dbagent/upload-db`
  - Multipart: `file=@your.db`
  - Side effects: saves to `/tmp/dbagent_uploads/<filename>` and sets `db_name`, `db_path`

## Important Implementation Detail (Catch-all Route)

Chainlit registers a catch-all route:

- `GET /{full_path:path}`

If that route is registered before `/api/dbagent/*`, it will return the Chainlit HTML page for API requests and the toolbar will get stuck on `Loading...`.

To prevent this, `ui/app.py` calls `_move_chainlit_catchall_to_end()` which moves the catch-all route to the end of the route list.

## Directory Structure

```
ui/
├── README.md             # This file
├── app.py                # Chainlit app entry point
├── config.toml            # Reference copy (not used by Chainlit automatically)
├── .chainlit/
│   └── config.toml        # Canonical Chainlit config used at runtime
├── components/
│   ├── chat.py           # Chat section
│   ├── history.py        # History browser
│   └── evaluation.py     # Evaluation viewer
├── public/
│   ├── custom.js          # Toolbar logic
│   └── custom.css         # Toolbar styles
├── static/
│   └── style.css         # Additional UI styles
└── chainlit.md           # Chainlit welcome message
```

## Usage

### Start the UI

```bash
cd ui
chainlit run app.py
```

Opens at `http://localhost:8000`

### Configuration

Environment variables:
- `OPENAI_API_KEY`: Required for agent
- `DB_FOLDER`: Path to databases (default: `data/`)
- `RESULTS_FOLDER`: Path to evaluation results (default: `results/`)

## Debugging

### Toolbar missing

If the toolbar disappears after a restart, Chainlit likely regenerated `ui/.chainlit/config.toml` and commented out:

- `custom_js = "/public/custom.js"`
- `custom_css = "/public/custom.css"`

Re-enable them and restart.

### Verify endpoints

If the DB dropdown is stuck on `Loading...`:

```bash
curl -i http://localhost:8000/api/dbagent/datasets
```

Expected `content-type: application/json`.

### Verify session cookie

The endpoints require `X-Chainlit-Session-id`. The toolbar retries automatically if it briefly gets `401` during initial load.
