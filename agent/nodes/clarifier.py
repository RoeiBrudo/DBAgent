"""
Clarifier node: analyzes question semantics before SQL generation.

Determines what the question is really asking for:
- Output type (single value, list, ratio, comparison)
- Expected columns to return
- Required calculations
"""

import json
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage

from agent.state import AgentState
from config import get_llm_kwargs


CLARIFIER_PROMPT = """You are a question analyzer for a Text-to-SQL system.

Analyze the user's question and determine EXACTLY what output is expected.

Question: {question}

Database schema:
{schema}

Analyze and respond with JSON:
{{
    "output_type": "single_value" | "single_row" | "list" | "ratio" | "comparison" | "boolean",
    "output_description": "Brief description of what should be returned",
    "columns_to_return": ["column1", "column2"],  // ONLY columns explicitly needed
    "calculation_needed": "none" | "ratio" | "difference" | "sum" | "count" | "min" | "max" | "avg",
    "calculation_details": "If calculation needed, describe it (e.g., 'count of X divided by count of Y')",
    "key_constraints": ["any filters or conditions to apply"]
}}

ANALYSIS RULES:
- "ratio of X against Y" → output_type="ratio", calculation_needed="ratio"
- "is it true that more X than Y" → output_type="single_value", calculation_needed="difference" (positive = yes)
- "who/which had least/most X" → output_type="single_row", columns_to_return=[identifier only, NOT the value]
- "how many" → output_type="single_value", calculation_needed="count"
- "list all" → output_type="list"

Be precise about what columns to return - do NOT add extra "helpful" columns.
"""


def clarifier_node(state: AgentState) -> dict:
    """
    Analyze question semantics to guide SQL generation.
    
    Determines output type, required columns, and calculations.
    
    Returns:
        dict with clarification containing output expectations
    """
    messages = state.get("messages", [])
    schema = state.get("schema", {})
    
    # Get the question
    question = ""
    if messages:
        for msg in reversed(messages):
            if msg.type == "human":
                question = msg.content
                break
    
    if not question:
        return {"clarification": None}
    
    schema_str = json.dumps(schema, indent=2) if schema else "No schema available"
    
    llm = ChatOpenAI(**get_llm_kwargs())
    
    prompt = CLARIFIER_PROMPT.format(
        question=question,
        schema=schema_str,
    )
    
    response = llm.invoke([HumanMessage(content=prompt)])
    content = response.content
    
    # Strip markdown code blocks if present
    if "```json" in content:
        content = content.split("```json")[1].split("```")[0]
    elif "```" in content:
        content = content.split("```")[1].split("```")[0]
    content = content.strip()
    
    try:
        clarification = json.loads(content)
        return {"clarification": clarification}
    except json.JSONDecodeError:
        # If parsing fails, return raw response as description
        return {
            "clarification": {
                "output_type": "unknown",
                "output_description": response.content,
                "columns_to_return": [],
                "calculation_needed": "none",
            }
        }
