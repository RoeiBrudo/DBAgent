"""
Chainlit-based UI for DBAgent.

Entry point for the UI application.
Run with: chainlit run ui/app.py
"""

import os
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import chainlit as cl
from chainlit.input_widget import Select
from fastapi import File, HTTPException, Request, UploadFile
from pydantic import BaseModel

from ui.components.chat import (
    handle_chat_message, 
    on_chat_start, 
    on_settings_update,
    on_select_db,
    on_back,
)
from ui.components.history import render_history_sidebar
from ui.components.evaluation import render_evaluation_view

from chainlit.server import app as fastapi_app
from chainlit.user_session import user_sessions

from data.main import get_all_dbs, get_datasets

# Configuration
DATA_FOLDER = Path(os.environ.get("DB_FOLDER", "data"))
RESULTS_FOLDER = Path(os.environ.get("RESULTS_FOLDER", "results"))


def _get_session_id(request: Request) -> str:
    session_id = request.cookies.get("X-Chainlit-Session-id")
    if not session_id:
        raise HTTPException(status_code=401, detail="Missing Chainlit session cookie")
    return session_id


@fastapi_app.get("/api/dbagent/datasets")
async def dbagent_datasets():
    return get_datasets()


@fastapi_app.get("/api/dbagent/state")
async def dbagent_state(request: Request):
    session_id = _get_session_id(request)
    s = user_sessions.get(session_id, {})
    return {
        "db_name": s.get("db_name"),
        "db_path": s.get("db_path"),
        "sql_mode": bool(s.get("sql_mode", False)),
    }


class SwitchDbBody(BaseModel):
    dataset: str
    db: str


@fastapi_app.post("/api/dbagent/switch-db")
async def dbagent_switch_db(body: SwitchDbBody, request: Request):
    session_id = _get_session_id(request)
    all_dbs = get_all_dbs()
    key = f"{body.dataset}/{body.db}"
    info = all_dbs.get(key)
    if not info:
        raise HTTPException(status_code=404, detail=f"Database not found: {key}")

    if session_id not in user_sessions:
        user_sessions[session_id] = {}

    user_sessions[session_id]["db_name"] = key
    user_sessions[session_id]["db_path"] = info["path"]
    user_sessions[session_id]["conn"] = None
    return {"ok": True, "db_name": key}


class SqlModeBody(BaseModel):
    enabled: bool


@fastapi_app.post("/api/dbagent/sql-mode")
async def dbagent_sql_mode(body: SqlModeBody, request: Request):
    session_id = _get_session_id(request)
    if session_id not in user_sessions:
        user_sessions[session_id] = {}
    user_sessions[session_id]["sql_mode"] = bool(body.enabled)
    return {"ok": True, "sql_mode": bool(body.enabled)}


@fastapi_app.post("/api/dbagent/upload-db")
async def dbagent_upload_db(request: Request, file: UploadFile = File(...)):
    session_id = _get_session_id(request)

    filename = file.filename or "uploaded.sqlite"
    temp_dir = Path("/tmp/dbagent_uploads")
    temp_dir.mkdir(exist_ok=True)
    dest_path = temp_dir / filename

    content = await file.read()
    dest_path.write_bytes(content)

    if session_id not in user_sessions:
        user_sessions[session_id] = {}

    db_name = f"uploaded/{filename}"
    user_sessions[session_id]["db_name"] = db_name
    user_sessions[session_id]["db_path"] = str(dest_path)
    user_sessions[session_id]["conn"] = None
    return {"ok": True, "db_name": db_name}


def _move_chainlit_catchall_to_end() -> None:
    """Chainlit registers a catch-all GET route (/{full_path:path}).

    If we add routes after it, the catch-all will swallow our requests.
    Move the catch-all to the end so /api/dbagent/* stays reachable.
    """
    routes = fastapi_app.router.routes
    for i, r in enumerate(list(routes)):
        path = getattr(r, "path", None)
        if path == "/{full_path:path}":
            routes.append(routes.pop(i))
            break


_move_chainlit_catchall_to_end()


@cl.on_chat_start
async def start():
    """Initialize chat session."""
    await on_chat_start()


@cl.on_message
async def main(message: cl.Message):
    """Handle incoming chat messages."""
    await handle_chat_message(message)


@cl.action_callback("switch_tab")
async def switch_tab(action: cl.Action):
    """Handle tab switching."""
    tab = action.value
    if tab == "history":
        await render_history_sidebar()
    elif tab == "evaluation":
        await render_evaluation_view()
    # Chat is the default, no special handling needed


@cl.on_settings_update
async def settings_update(settings):
    """Handle settings changes."""
    await on_settings_update(settings)


@cl.action_callback("select_db")
async def select_db_action(action: cl.Action):
    """Handle database selection from action buttons."""
    await on_select_db(action)


@cl.action_callback("back")
async def back_action(action: cl.Action):
    """Go back."""
    await on_back(action)
