"""
Central configuration for the DBAgent project.

All configurable values should be defined here.
Secrets (API keys) come from .env, everything else is configured here.

Supported models:
  OpenAI: gpt-4o, gpt-4o-mini, o3-mini, gpt-5, gpt-5-mini
  Anthropic: claude-sonnet-4-20250514, claude-3-5-sonnet-20241022, claude-3-5-haiku-20241022
"""

import os
from utils import load_dotenv

# Load .env file on import
load_dotenv()


# --- LLM Configuration ---
LLM_MODEL = "claude-3-5-haiku-20241022"  # Options: gpt-4o, claude-sonnet-4-20250514, o3-mini, etc.
LLM_TEMPERATURE = 0
LLM_TEMPERATURE_CREATIVE = 0.3  # For analysis/natural language responses

# Models that don't support temperature parameter (reasoning models)
REASONING_MODELS = ["o1", "o1-mini", "o1-preview", "o3", "o3-mini", "gpt-5"]

# Claude/Anthropic models
ANTHROPIC_MODELS = ["claude"]


def is_anthropic_model(model: str) -> bool:
    """Check if model is an Anthropic/Claude model."""
    return any(model.startswith(prefix) for prefix in ANTHROPIC_MODELS)


def get_llm(model: str = None, temperature: float = None):
    """
    Get LLM instance for the specified model.
    Automatically selects OpenAI or Anthropic based on model name.
    
    Returns:
        ChatOpenAI or ChatAnthropic instance
    """
    model = model or LLM_MODEL
    temp = temperature if temperature is not None else LLM_TEMPERATURE
    
    if is_anthropic_model(model):
        from langchain_anthropic import ChatAnthropic
        return ChatAnthropic(model=model, temperature=temp)
    else:
        from langchain_openai import ChatOpenAI
        # Reasoning models don't support temperature
        if any(model.startswith(rm) for rm in REASONING_MODELS):
            return ChatOpenAI(model=model)
        return ChatOpenAI(model=model, temperature=temp)


def get_llm_kwargs(model: str = None, temperature: float = None) -> dict:
    """
    Get LLM kwargs that are compatible with the specified model.
    Reasoning models (o1, o3, gpt-5) don't support temperature.
    
    DEPRECATED: Use get_llm() instead for automatic provider selection.
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


def get_anthropic_api_key() -> str:
    """Get Anthropic API key from environment."""
    key = os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        raise ValueError("ANTHROPIC_API_KEY not found in environment. Add it to .env file.")
    return key
