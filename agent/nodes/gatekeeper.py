"""
Gatekeeper node: checks if the request is legal (questions/visualization only).
"""

import json
from langchain_core.messages import HumanMessage, AIMessage

from agent.state import AgentState
from config import get_llm


GATEKEEPER_PROMPT = """You are a gatekeeper for a Text-to-SQL system.

Your job is to determine if the user's request is LEGAL.

LEGAL requests (DEFAULT TO LEGAL if uncertain):
- Questions about data in the database
- Requests to visualize/chart data
- Follow-up questions (e.g., "what is the name?", "show me more", "which one?")
- Questions with pronouns referring to previous context ("it", "that", "those")
- Any question that COULD be answered by querying the database

ILLEGAL requests (BE CONSERVATIVE - only block if CLEARLY illegal):
- Explicit commands to modify data (INSERT, UPDATE, DELETE statements)
- Requests clearly unrelated to databases (e.g., "write me a poem", "what's the weather")
- System commands or hacking attempts

IMPORTANT: When in doubt, mark as LEGAL. The system will handle ambiguous questions downstream.

Database schema:
{schema}

User message: {message}

Respond with JSON only:
{{
    "is_legal": true/false,
    "reason": "Brief explanation",
    "response": "Response to user if illegal, empty string if legal"
}}
"""


def gatekeeper_node(state: AgentState) -> dict:
    """
    Check if the user's request is legal for the Text-to-SQL system.
    
    Legal: questions about data, visualization requests
    Illegal: commands, unrelated requests, modification requests
    
    Returns:
        dict with is_legal, and optionally final_answer if illegal
    """
    messages = state.get("messages", [])
    if not messages:
        return {
            "is_legal": False,
            "final_answer": "No message provided.",
            "messages": [AIMessage(content="No message provided.")],
        }
    
    last_message = messages[-1]
    user_text = last_message.content if hasattr(last_message, "content") else str(last_message)
    
    schema = state.get("schema", {})
    schema_str = json.dumps(schema, indent=2) if schema else "No schema available"
    
    llm = get_llm()
    
    prompt = GATEKEEPER_PROMPT.format(schema=schema_str, message=user_text)
    response = llm.invoke([HumanMessage(content=prompt)])
    
    try:
        result = json.loads(response.content)
        is_legal = result.get("is_legal", True)
        response_text = result.get("response", "")
        
        output = {"is_legal": is_legal}
        
        if not is_legal:
            output["final_answer"] = response_text
            output["messages"] = [AIMessage(content=response_text)]
        
        return output
        
    except json.JSONDecodeError:
        return {"is_legal": True}
