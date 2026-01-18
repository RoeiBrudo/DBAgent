"""
Chat component for interactive Q&A with databases.
"""

import json
import sqlite3
import time
from pathlib import Path
from typing import Optional, Dict

import chainlit as cl
from chainlit.input_widget import Select, TextInput

# Import from data package
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from data.main import get_datasets, get_all_dbs, get_database_schema

# Available models
AVAILABLE_MODELS = [
    "gpt-4o",
    "gpt-4o-mini", 
    "gpt-4-turbo",
    "claude-3-5-sonnet-20241022",
    "claude-3-5-haiku-20241022",
]


async def on_chat_start():
    """Initialize chat session."""
    # Get all databases using data package function
    datasets = get_datasets()
    spider_dbs = datasets.get('spider', [])
    bird_dbs = datasets.get('bird', [])
    
    # Build db_map for path lookup
    all_dbs = get_all_dbs()
    db_map = {name: info["path"] for name, info in all_dbs.items()}
    cl.user_session.set("db_map", db_map)
    cl.user_session.set("spider_dbs", spider_dbs)
    cl.user_session.set("bird_dbs", bird_dbs)
    
    # Initialize state
    cl.user_session.set("conversation_history", [])
    cl.user_session.set("conn", None)
    cl.user_session.set("db_path", None)
    cl.user_session.set("db_name", None)
    cl.user_session.set("sql_mode", False)
    
    # Setup settings panel for model/API keys only
    await cl.ChatSettings([
        Select(
            id="model",
            label="🤖 Model",
            values=AVAILABLE_MODELS,
            initial_value="gpt-4o",
        ),
        TextInput(
            id="openai_key",
            label="OpenAI API Key (optional)",
            placeholder="sk-...",
        ),
        TextInput(
            id="anthropic_key", 
            label="Anthropic API Key (optional)",
            placeholder="sk-ant-...",
        ),
    ]).send()
    
    # No welcome message - toolbar has all the info


async def on_settings_update(settings):
    """Handle settings changes (model, API keys)."""
    model = settings.get("model")
    if model:
        cl.user_session.set("selected_model", model)
    
    openai_key = settings.get("openai_key")
    if openai_key:
        cl.user_session.set("openai_api_key", openai_key)
    
    anthropic_key = settings.get("anthropic_key")
    if anthropic_key:
        cl.user_session.set("anthropic_api_key", anthropic_key)


async def switch_database(db_name: str, db_path: str):
    """Switch to a new database."""
    old_conn = cl.user_session.get("conn")
    if old_conn:
        try:
            old_conn.close()
        except:
            pass
    
    cl.user_session.set("db_path", db_path)
    cl.user_session.set("db_name", db_name)
    cl.user_session.set("conn", None)
    # Keep conversation history; only switch DB context
    cl.user_session.set("current_db", None)
    
    # Silently connect and validate - no chat message
    try:
        conn = sqlite3.connect(db_path)
        schema = get_database_schema(conn)
        conn.close()
        # Connection successful - toolbar updated via __CURRENT__ message
    except Exception as e:
        # Only show error if connection fails
        await cl.Message(content=f"❌ Error connecting: {e}").send()


async def handle_upload():
    """Handle database upload."""
    files = await cl.AskFileMessage(
        content="📤 Upload a SQLite database file (.db or .sqlite):",
        accept=["application/x-sqlite3", ".db", ".sqlite"],
        max_size_mb=100,
    ).send()
    
    if files:
        file = files[0]
        import shutil
        temp_dir = Path("/tmp/dbagent_uploads")
        temp_dir.mkdir(exist_ok=True)
        dest_path = temp_dir / file.name
        shutil.copy(file.path, dest_path)
        await switch_database(f"uploaded/{file.name}", str(dest_path))


async def execute_sql(sql_query: str) -> str:
    """Execute raw SQL and return formatted results."""
    db_path = cl.user_session.get("db_path")
    if not db_path:
        return "❌ No database connected."
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        start_time = time.time()
        cursor.execute(sql_query)
        rows = cursor.fetchall()
        elapsed_ms = (time.time() - start_time) * 1000
        
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        conn.close()
        
        if not rows:
            return f"✅ Query executed in {elapsed_ms:.1f}ms. No results."
        
        result = f"✅ **{len(rows)} rows** in {elapsed_ms:.1f}ms\n\n"
        result += "| " + " | ".join(columns) + " |\n"
        result += "| " + " | ".join(["---"] * len(columns)) + " |\n"
        
        for row in rows[:20]:
            result += "| " + " | ".join(str(v)[:50] for v in row) + " |\n"
        
        if len(rows) > 20:
            result += f"\n*... and {len(rows) - 20} more rows*"
        
        return result
        
    except Exception as e:
        return f"❌ SQL Error: {e}"


async def handle_chat_message(message: cl.Message):
    """Process user message and generate response."""
    user_query = message.content.strip()
    db_path = cl.user_session.get("db_path")
    sql_mode = cl.user_session.get("sql_mode", False)
    
    # Toolbar no longer sends chat commands; it uses HTTP endpoints in ui/app.py
    
    # Handle "use <database>" command
    if user_query.lower().startswith("use "):
        db_search = user_query[4:].strip()
        db_map = cl.user_session.get("db_map", {})
        
        # Try exact match first
        if db_search in db_map:
            await switch_database(db_search, db_map[db_search])
            return
        
        # Try partial match
        matches = [(n, p) for n, p in db_map.items() if db_search.lower() in n.lower()]
        if len(matches) == 1:
            await switch_database(matches[0][0], matches[0][1])
        elif len(matches) > 1:
            await cl.Message(content=f"Multiple matches: {', '.join(m[0] for m in matches[:10])}").send()
        else:
            await cl.Message(content=f"❌ Database '{db_search}' not found.").send()
        return
    
    # Check if database is connected
    if not db_path:
        await cl.Message(content="⚠️ Please select a database first using the toolbar above.").send()
        return
    
    # SQL mode - execute directly
    if sql_mode:
        result = await execute_sql(user_query)
        await cl.Message(content=result).send()
        return
    
    # Natural language mode - run agent pipeline
    msg = cl.Message(content="")
    await msg.send()
    try:
        result = await run_agent_pipeline(user_query, db_path, msg)
        msg.content = result.get("answer", "No answer generated.")
        await msg.update()
    except Exception as e:
        msg.content = f"❌ Error: {str(e)}"
        await msg.update()


@cl.action_callback("select_db")
async def on_select_db(action: cl.Action):
    """Handle database selection from action buttons."""
    db_name = action.payload.get("db")
    dataset_type = action.payload.get("type")
    
    full_name = f"{dataset_type}/{db_name}"
    db_map = cl.user_session.get("db_map", {})
    db_path = db_map.get(full_name)
    
    if db_path:
        await switch_database(full_name, db_path)


@cl.action_callback("back")
async def on_back(action: cl.Action):
    """Go back - just acknowledge."""
    pass


async def run_agent_pipeline(query: str, db_path: str, msg: cl.Message) -> dict:
    """
    Run the DBAgent pipeline and stream steps to UI.
    
    Returns dict with:
        - answer: Final natural language answer
        - sql: Generated SQL query
        - results: Query results
        - steps: List of step details
    """
    from agent.graph import create_graph
    from agent.state import AgentState
    from langchain_core.messages import HumanMessage
    
    # Get or create connection
    conn = cl.user_session.get("conn")
    if conn is None or cl.user_session.get("current_db") != db_path:
        if conn:
            conn.close()
        conn = sqlite3.connect(db_path)
        cl.user_session.set("conn", conn)
        cl.user_session.set("current_db", db_path)
    
    # Get conversation history
    history = cl.user_session.get("conversation_history", [])
    
    # Build initial state
    messages = history + [HumanMessage(content=query)]
    
    # Get schema
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    schema = {}
    for (table_name,) in tables:
        cursor.execute(f"PRAGMA table_info({table_name});")
        columns = [row[1] for row in cursor.fetchall()]
        schema[table_name] = columns
    
    initial_state: AgentState = {
        "messages": messages,
        "conn": conn,
        "schema": schema,
        "iteration": 0,
    }
    
    # Create and run graph
    graph = create_graph()
    
    # Track steps for UI
    steps = []
    step_elements = []
    
    async def on_step(step_name: str, data: dict):
        """Callback for each pipeline step."""
        status = "✓" if not data.get("error") else "✗"
        latency = data.get("latency_ms", 0)
        
        step_info = f"{status} **{step_name}**: "
        
        if step_name == "gatekeeper":
            step_info += f"{'in_scope' if data.get('is_legal') else 'out_of_scope'}"
        elif step_name == "organizer":
            ds = data.get("data_sources", {})
            tables = ds.get("tables", [])
            step_info += f"tables={tables}"
        elif step_name == "planner":
            step_info += f"{data.get('logic_plan', '')[:50]}..."
        elif step_name == "writer":
            sql = data.get("sql_query", "")
            step_info += f"`{sql[:60]}...`" if len(sql) > 60 else f"`{sql}`"
        elif step_name == "execute":
            step_info += f"{latency}ms"
        elif step_name == "analysis":
            step_info += "done"
        
        steps.append({"name": step_name, "info": step_info, "data": data})
    
    # Run the graph
    final_state = None
    async for event in graph.astream(initial_state):
        for node_name, node_output in event.items():
            await on_step(node_name, node_output)
            final_state = node_output
    
    # Build steps element
    steps_content = "\n".join([s["info"] for s in steps])
    
    # Add collapsible steps
    async with cl.Step(name="Pipeline Steps", type="tool") as step:
        step.output = steps_content
    
    # Update conversation history
    if final_state:
        history.append(HumanMessage(content=query))
        from langchain_core.messages import AIMessage
        history.append(AIMessage(content=final_state.get("final_answer", "")))
        cl.user_session.set("conversation_history", history)
    
    return {
        "answer": final_state.get("final_answer", "No answer generated.") if final_state else "Pipeline failed.",
        "sql": final_state.get("sql_query", "") if final_state else "",
        "results": final_state.get("query_result", "") if final_state else "",
        "steps": steps,
    }
