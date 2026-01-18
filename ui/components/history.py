"""
History component for browsing past conversations.
"""

import json
from pathlib import Path
from typing import Dict, List

import chainlit as cl


async def render_history_sidebar():
    """Render the history sidebar with past conversations."""
    # TODO: Implement history loading from SQLite checkpointer
    # For now, show placeholder
    await cl.Message(
        content="📜 **Conversation History**\n\nHistory viewing coming soon. Past conversations will be loaded from the SQLite checkpointer."
    ).send()


async def load_conversations_by_db() -> Dict[str, List[dict]]:
    """
    Load conversations grouped by database.
    
    Returns:
        Dict mapping db_name -> list of conversation summaries
    """
    # TODO: Implement actual loading from checkpointer
    # Structure: {
    #     "db1.db": [
    #         {"id": "chat_1", "preview": "How many customers?", "turns": 3},
    #         {"id": "chat_2", "preview": "Show top products", "turns": 5},
    #     ],
    #     "db2.db": [...],
    # }
    return {}


async def load_conversation(conversation_id: str) -> List[dict]:
    """
    Load a specific conversation by ID.
    
    Returns:
        List of messages: [{"role": "user"|"assistant", "content": "..."}]
    """
    # TODO: Implement actual loading from checkpointer
    return []
