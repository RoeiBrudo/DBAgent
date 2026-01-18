"""
Analysis node: generates natural language answer from query results.
"""

import json
from langchain_core.messages import HumanMessage

from agent.state import AgentState
from config import get_llm


ANALYSIS_PROMPT = """You are a data analyst assistant. Given the user's question, the SQL query that was executed, and the results, provide a clear and helpful natural language answer.

User's question: {question}

SQL query executed:
{sql_query}

Query results (as JSON):
{results}

Rules:
- Answer the question directly and concisely
- Include specific numbers and data from the results
- If results are empty, explain that no data was found
- Format numbers nicely (e.g., use commas for thousands)
- Keep the answer conversational but informative
- Do not mention SQL or technical details unless relevant to the answer

Answer:
"""


def analysis_node(state: AgentState) -> dict:
    """
    Generate a natural language answer from the query results.
    
    Takes the original question, SQL query, and results to produce
    a human-friendly response.
    
    Returns:
        dict with final_answer
    """
    messages = state.get("messages", [])
    sql_query = state.get("sql_query", "")
    query_result = state.get("query_result", "")
    
    # Get the user's original question from messages
    question = ""
    for msg in reversed(messages):
        if hasattr(msg, "type") and msg.type == "human":
            question = msg.content
            break
        elif hasattr(msg, "content") and not hasattr(msg, "type"):
            question = msg.content
            break
    
    # Parse results
    try:
        results = json.loads(query_result) if query_result else []
    except json.JSONDecodeError:
        results = []
    
    # Format results for prompt (limit size for large result sets)
    if len(results) > 20:
        results_str = json.dumps(results[:20], indent=2) + f"\n... and {len(results) - 20} more rows"
    else:
        results_str = json.dumps(results, indent=2)
    
    llm = get_llm()
    
    prompt = ANALYSIS_PROMPT.format(
        question=question,
        sql_query=sql_query,
        results=results_str,
    )
    
    response = llm.invoke([HumanMessage(content=prompt)])
    
    return {"final_answer": response.content}
