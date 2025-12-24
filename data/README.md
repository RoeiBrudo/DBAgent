# Text2SQL Data Module

## Overview

This directory contains the data access module, preparation scripts, and testing utilities for Text2SQL datasets.

## Quick Start

### 1. Optional Manual Downloads

Spider, BIRD, and SParC are auto-downloaded when missing. CoSQL requires local JSON files if you want it included.

#### CoSQL Dataset (optional)
Place the CoSQL JSONs here if you want CoSQL turns:
```
data/external/cosql_dataset/
└── sql_state_tracking/
    ├── cosql_train.json
    ├── cosql_dev.json
    └── ...
```

### 2. Prepare the Unified Dataset

Run the pipeline from the project root:

```bash
python3 -m data.create
```

This will:
- Load datasets (Spider, BIRD, SParC, and CoSQL if present)
- Normalize into a unified format
- Add database file paths stored relative to the project root
- Save to `data/normalized/turns.db`

**Expected output:**
- `normalized/turns.db` - SQLite database with ~27,112 unified turns
- Test reports showing 100% gold SQL and database availability

### 3. Verify the Data

Run the focused tests from the project root:

```bash
python3 -m data.test
```

This prints:
- Gold SQL presence across all turns
- Databases OK (connect + schema) over unique `db_file`s

## Main Module

### `main.py`
Core data access module providing clean API for loading and working with the unified dataset.

**Functions:**
- `load_data(source=None, split=None, limit=None)` - Load data from unified dataset
- `get_turn(turn_uid)` - Get a single turn by its unique identifier
- `get_conversation(conversation_id)` - Get all turns in a conversation
- `get_db_connection(turn)` - Get SQLite connection for a turn's database
- `get_database_schema(turn)` - Get schema of a turn's database

**Turn Schema:**
Each turn contains:
- `turn_uid` - Unique identifier for the turn
- `dataset` - Source dataset ('spider', 'bird', 'cosql', 'sparc')
- `split` - Data split ('train' or 'validation')
- `conversation_id` - Identifier for the conversation
- `turn_index` - Index of turn in conversation (0 for single-turn)
- `db_id` - Database identifier
- `db_file` - Path to SQLite database file
- `dialect` - SQL dialect (always 'sqlite')
- `text` - Natural language question/utterance
- `context` - List of previous utterances in conversation
- `context_gold_sql` - List of gold SQL from previous turns
- `gold_sql` - Gold SQL query for this turn
- `difficulty` - Difficulty level (if available)

**Usage:**
```python
from data.main import load_data, get_turn, get_conversation, get_db_connection

# Load all Spider training data
spider_train = load_data(source='spider', split='train')

# Load first 100 turns from all datasets
sample = load_data(limit=100)

# Get a specific turn by ID
turn = get_turn('spider:train:spider:train:0:0')
if turn:
    print(f"Question: {turn['text']}")
    print(f"SQL: {turn['gold_sql']}")

# Get all turns in a conversation
conversation = get_conversation('cosql:train:0')
for turn in conversation:
    print(f"Turn {turn['turn_index']}: {turn['text']}")
    if turn['context']:
        print(f"  Previous: {turn['context'][-1]}")
    if turn['context_gold_sql']:
        print(f"  Previous SQL: {turn['context_gold_sql'][-1][:50]}...")

# Get database connection for a turn
conn = get_db_connection(spider_train[0])
cursor = conn.cursor()
cursor.execute(spider_train[0]['gold_sql'])
results = cursor.fetchall()
conn.close()
```

## Scripts

### `scripts/prepare_datasets.py`
Main data preparation pipeline that:
1. **Phase 1**: Loads and tests individual datasets (Spider, BIRD, CoSQL, SParC)
2. **Phase 2**: Normalizes all datasets into a unified format and tests the result

**Datasets processed:**
- **Spider** (8,034 single-turn examples) - Automatically downloaded from HuggingFace
- **BIRD Mini-Dev** (500 single-turn examples) - Requires manual download (see Quick Start)
- **CoSQL** (8,350 multi-turn conversation turns) - Requires manual download (see Quick Start)
- **SParC** (10,228 multi-turn conversation turns) - Automatically downloaded from HuggingFace

**Key Features:**
- Unified schema across all datasets
- Context tracking for multi-turn conversations
- Previous SQL queries stored in `context_gold_sql` for multi-turn datasets
- Database file paths for direct SQL execution

**Usage:**
```bash
cd helpers
python prepare.py
```

**Output:**
- Creates `normalized/turns.db` - SQLite database with ~27,112 unified turns
- Comprehensive test reports for each phase

### `scripts/test_datasets.py`
Standalone testing and example script for validating datasets.

**Usage:**
```bash
cd helpers

# Run all tests + show examples
python test.py

# Test only individual datasets
python test.py individual

# Test only unified dataset
python test.py unified

# Show example turns from each source
python test.py examples
```

**Tests performed:**

#### Individual Dataset Tests
For each dataset (Spider, BIRD, CoSQL, SParC):
- ✓ Gold SQL availability (% of rows/turns with valid SQL)
- ✓ Database availability (% of unique databases accessible)

#### Unified Dataset Tests
- ✓ Gold SQL availability across all datasets
- ✓ Database availability across all datasets
- ✓ Context conversion analysis:
  - Single-turn (no context) - Spider, BIRD
  - Multi-turn with context - CoSQL, SParC
  - Context depth statistics (average and max turns)
  - Context gold SQL tracking for previous turns

#### Examples
- Shows sample turns from each dataset source
- Displays single-turn and multi-turn conversation examples
- Shows database schema information

## Requirements

Install dependencies:
```bash
pip install datasets huggingface_hub
```

## Directory Structure

```
data/
├── main.py                  # Core data access module
├── scripts/
│   ├── prepare_datasets.py  # Main pipeline
│   ├── test_datasets.py     # Testing & examples
│   └── README.md            # This file
├── normalized/
│   └── turns.db             # Output: unified dataset (created by prepare_datasets.py)
├── external/                # Manual downloads go here
│   ├── prem-research_spider/  # Auto-downloaded by prepare_datasets.py
│   ├── bird_mini_dev/         # ⚠️ MANUAL DOWNLOAD REQUIRED
│   │   └── MINIDEV/
│   │       └── dev_databases/
│   └── cosql/                 # ⚠️ MANUAL DOWNLOAD REQUIRED
│       └── sql_state_tracking/
└── _hf_cache/               # HuggingFace cache (auto-created)
```