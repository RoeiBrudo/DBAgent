"""
Organizer node: identifies tables, fields, and joins needed for the query.
"""

import json
from langchain_core.messages import HumanMessage

from agent.state import AgentState, DataSources, JoinInfo
from config import get_llm


ORGANIZER_PROMPT = """You are a database schema analyzer for a Text-to-SQL system.

Given the user's question and database schema, identify:
1. Which tables are needed
2. Which fields/columns are relevant
3. What joins are required (if any)

Database schema:
{schema}

Conversation history:
{history}

Current question: {question}

Respond with JSON only:
{{
    "tables": ["table1", "table2"],
    "fields": ["table1.column1", "table2.column2"],
    "joins": [
        {{
            "join_type": "INNER",
            "left_table": "table1",
            "right_table": "table2", 
            "left_field": "foreign_key",
            "right_field": "primary_key"
        }}
    ]
}}

CRITICAL RULES:
- MINIMIZE tables and joins. Use the FEWEST tables possible to answer the question.
- Do NOT add a JOIN if the required data is already in one table.
- Example: If "instructor" table has "dept_name" column, do NOT join with "department" table just to get dept_name.
- Only include tables that exist in the schema
- Use "table.column" format for fields
- join_type can be: INNER, LEFT, RIGHT, CROSS
- If no joins needed, use empty array for joins
- Consider conversation history for context (e.g., "those customers" refers to previous query)
"""


def organizer_node(state: AgentState) -> dict:
    """
    Analyze the question and schema to identify required data sources.
    
    Returns:
        dict with data_sources containing tables, fields, and joins
    """
    messages = state.get("messages", [])
    schema = state.get("schema", {})
    
    # Get current question
    question = ""
    if messages:
        last_msg = messages[-1]
        question = last_msg.content if hasattr(last_msg, "content") else str(last_msg)
    
    # Build conversation history (exclude current message)
    history_parts = []
    for msg in messages[:-1]:
        role = "User" if msg.type == "human" else "Assistant"
        content = msg.content if hasattr(msg, "content") else str(msg)
        history_parts.append(f"{role}: {content}")
    history = "\n".join(history_parts) if history_parts else "No previous conversation"
    
    schema_str = json.dumps(schema, indent=2)
    
    llm = get_llm()
    
    prompt = ORGANIZER_PROMPT.format(
        schema=schema_str,
        history=history,
        question=question,
    )
    
    response = llm.invoke([HumanMessage(content=prompt)])
    
    try:
        result = json.loads(response.content)
        
        data_sources: DataSources = {
            "tables": result.get("tables", []),
            "fields": result.get("fields", []),
            "joins": [
                JoinInfo(
                    join_type=j.get("join_type", "INNER"),
                    left_table=j.get("left_table", ""),
                    right_table=j.get("right_table", ""),
                    left_field=j.get("left_field", ""),
                    right_field=j.get("right_field", ""),
                )
                for j in result.get("joins", [])
            ],
        }
        
        return {"data_sources": data_sources}
        
    except json.JSONDecodeError:
        return {"data_sources": {"tables": [], "fields": [], "joins": []}}
