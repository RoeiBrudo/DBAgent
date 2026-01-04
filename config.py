"""
Central configuration for the DBAgent project.

All configurable values should be defined here.
Secrets (API keys) come from .env, everything else is configured here.
"""

import os
from utils import load_dotenv

# Load .env file on import
load_dotenv()


# --- LLM Configuration ---
LLM_MODEL = "gpt-5"  # Testing GPT-5
LLM_TEMPERATURE = 0
LLM_TEMPERATURE_CREATIVE = 0.3  # For analysis/natural language responses

# Models that don't support temperature parameter (reasoning models)
REASONING_MODELS = ["o1", "o1-mini", "o1-preview", "o3", "o3-mini", "gpt-5"]


def get_llm_kwargs(model: str = None, temperature: float = None) -> dict:
    """
    Get LLM kwargs that are compatible with the specified model.
    Reasoning models (o1, o3, gpt-5) don't support temperature.
    """
    model = model or LLM_MODEL
    kwargs = {"model": model}
    
    # Only add temperature for non-reasoning models
    if not any(model.startswith(rm) for rm in REASONING_MODELS):
        kwargs["temperature"] = temperature if temperature is not None else LLM_TEMPERATURE
    
    return kwargs


# --- SQL Execution ---
SQL_TIMEOUT_MS = 30000  # 30 seconds
SQL_MAX_RETRIES = 4


# --- Visualization ---
VIZ_MAX_ROWS = 100  # Max rows to pass to viz agent


# --- API Keys (from .env) ---
def get_openai_api_key() -> str:
    """Get OpenAI API key from environment."""
    key = os.environ.get("OPENAI_API_KEY")
    if not key:
        raise ValueError("OPENAI_API_KEY not found in environment. Add it to .env file.")
    return key
