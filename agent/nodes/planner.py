"""
Planner node: generates a plain English SQL execution plan.
"""

import json
from langchain_core.messages import HumanMessage

from agent.state import AgentState
from config import get_llm


PLANNER_PROMPT = """You are a SQL query planner for a Text-to-SQL system.

Given the user's question, data sources identified, and database schema, create a clear step-by-step plan for how to write the SQL query.

Database schema:
{schema}

Data sources identified:
- Tables: {tables}
- Fields: {fields}
- Joins: {joins}

Conversation history:
{history}

Current question: {question}

Write a clear, numbered execution plan in plain English. Include:
1. What to SELECT (which fields, any aggregations)
2. FROM which table(s)
3. JOIN conditions if needed
4. WHERE filters if needed
5. GROUP BY if needed
6. ORDER BY if needed
7. LIMIT if needed

Be concise but complete. Output only the plan, no code.
"""


def planner_node(state: AgentState) -> dict:
    """
    Generate a plain English SQL execution plan.
    
    Takes the data sources from organizer and creates a step-by-step plan
    that the writer node will use to generate SQL.
    
    Returns:
        dict with logic_plan (plain text execution plan)
    """
    messages = state.get("messages", [])
    schema = state.get("schema", {})
    data_sources = state.get("data_sources", {"tables": [], "fields": [], "joins": []})
    
    # Get current question
    question = ""
    if messages:
        last_msg = messages[-1]
        question = last_msg.content if hasattr(last_msg, "content") else str(last_msg)
    
    # Build conversation history
    history_parts = []
    for msg in messages[:-1]:
        role = "User" if msg.type == "human" else "Assistant"
        content = msg.content if hasattr(msg, "content") else str(msg)
        history_parts.append(f"{role}: {content}")
    history = "\n".join(history_parts) if history_parts else "No previous conversation"
    
    # Format joins for display
    joins_str = "None"
    if data_sources.get("joins"):
        joins_list = []
        for j in data_sources["joins"]:
            joins_list.append(
                f"{j['join_type']} JOIN {j['left_table']}.{j['left_field']} = {j['right_table']}.{j['right_field']}"
            )
        joins_str = "\n".join(joins_list)
    
    schema_str = json.dumps(schema, indent=2)
    
    llm = get_llm()
    
    prompt = PLANNER_PROMPT.format(
        schema=schema_str,
        tables=", ".join(data_sources.get("tables", [])),
        fields=", ".join(data_sources.get("fields", [])),
        joins=joins_str,
        history=history,
        question=question,
    )
    
    response = llm.invoke([HumanMessage(content=prompt)])
    
    return {"logic_plan": response.content}
