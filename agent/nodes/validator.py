"""
Validator node: checks query results and SQL for common issues.

If issues are found (e.g., wrong date format, empty results from valid query),
returns feedback to trigger a retry with corrected approach.
"""

import json
import re
from langchain_core.messages import HumanMessage

from agent.state import AgentState
from config import get_llm


VALIDATOR_PROMPT = """You are a SQL result validator. Analyze the query and results to detect issues.

User question: {question}

Generated SQL: {sql}

Query results: {results}

Database schema (relevant tables):
{schema}

Common issues to check:
1. EMPTY RESULTS when data should exist - often caused by wrong date format
   - Date columns might be YYYYMM format (e.g., '201201') not standard dates
   - Check if strftime() was used on non-date strings
   
2. WRONG AGGREGATION - MIN vs SUM vs AVG confusion
   - "least/lowest total" means SUM then ORDER BY ASC
   - "minimum value" means MIN
   
3. WRONG CALCULATION - ratio/percentage issues
   - "ratio of X against Y" means X/Y as single value, not GROUP BY

Respond with JSON:
{{
    "is_valid": true/false,
    "issue_type": "none" | "date_format" | "aggregation" | "calculation" | "other",
    "issue_description": "description of the problem",
    "fix_suggestion": "specific fix for the writer node"
}}

If results look correct for the question, set is_valid=true.
"""


def validator_node(state: AgentState) -> dict:
    """
    Validate query results and detect common issues.
    
    If issues are found, returns error and fix suggestion to trigger retry.
    
    Returns:
        dict with validation_passed, or error + fix_suggestion for retry
    """
    messages = state.get("messages", [])
    sql_query = state.get("sql_query", "")
    execution = state.get("execution")
    schema = state.get("schema", {})
    
    # Get the question
    question = ""
    if messages:
        for msg in reversed(messages):
            if msg.type == "human":
                question = msg.content
                break
    
    # Get results
    results = []
    if execution and execution.success:
        results = execution.results
    
    # Skip validation if execution failed (already has error)
    if execution and not execution.success:
        return {}
    
    # Skip if no results to validate
    if not sql_query:
        return {}
    
    # Build schema string for relevant tables
    schema_str = json.dumps(schema, indent=2)
    
    llm = get_llm()
    
    prompt = VALIDATOR_PROMPT.format(
        question=question,
        sql=sql_query,
        results=json.dumps(results[:10]) if results else "[]",  # Limit results
        schema=schema_str,
    )
    
    response = llm.invoke([HumanMessage(content=prompt)])
    
    try:
        result = json.loads(response.content)
        
        if result.get("is_valid", True):
            # Results look good
            return {"validation_passed": True}
        
        # Issue detected - trigger retry
        issue_type = result.get("issue_type", "unknown")
        issue_desc = result.get("issue_description", "Unknown issue")
        fix_suggestion = result.get("fix_suggestion", "")
        
        error_msg = f"Validation failed ({issue_type}): {issue_desc}"
        if fix_suggestion:
            error_msg += f"\nFix: {fix_suggestion}"
        
        iteration = state.get("iteration", 0)
        
        return {
            "validation_passed": False,
            "error": error_msg,
            "iteration": iteration + 1,
        }
        
    except json.JSONDecodeError:
        # Can't parse response, assume valid
        return {"validation_passed": True}
