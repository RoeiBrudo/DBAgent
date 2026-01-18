"""
Writer node: converts the execution plan into SQL.
"""

import json
import re
import sqlparse
from langchain_core.messages import HumanMessage

from agent.state import AgentState, SQLExecution
from agent.tools.db_tools import format_enrichment_for_prompt
from config import get_llm


WRITER_PROMPT = """You are a SQL writer for a Text-to-SQL system using SQLite.

Given the execution plan and output expectations, write a clean, efficient SQL query.

Database schema:
{schema}

{data_samples}

Execution plan:
{plan}

{clarification_context}

{retry_context}

RULES:
- Use EXACT column values as shown in the data samples (e.g., 'M' not 'Male', 'CZK' not 'Czech koruna')
- Follow the output expectations EXACTLY - return only the specified columns
- Use valid SQLite syntax
- Use CASE WHEN for conditional counting (not FILTER)
- Use CAST(x AS FLOAT) for decimal division
- Output ONLY the SQL query, no explanation, no markdown

SQL:
"""


def clean_sql(sql: str) -> str:
    """Remove markdown code fences and extra whitespace from SQL."""
    sql = re.sub(r"```sql\s*", "", sql)
    sql = re.sub(r"```\s*", "", sql)
    return sql.strip()


def validate_sql(sql: str) -> tuple[bool, str]:
    """
    Validate SQL syntax using sqlparse.
    
    Returns:
        (is_valid, error_message)
    """
    if not sql or not sql.strip():
        return False, "Empty SQL query"
    
    try:
        parsed = sqlparse.parse(sql)
        if not parsed:
            return False, "Failed to parse SQL"
        
        stmt = parsed[0]
        
        # Check for basic statement type
        if stmt.get_type() == 'UNKNOWN':
            # Could still be valid, just unknown type
            pass
        
        # Check for unbalanced parentheses
        open_parens = sql.count('(')
        close_parens = sql.count(')')
        if open_parens != close_parens:
            return False, f"Unbalanced parentheses: {open_parens} '(' vs {close_parens} ')'"
        
        # Check for unbalanced quotes
        single_quotes = sql.count("'") - sql.count("\\'")
        if single_quotes % 2 != 0:
            return False, "Unbalanced single quotes"
        
        return True, ""
        
    except Exception as e:
        return False, f"SQL parse error: {str(e)}"


def writer_node(state: AgentState) -> dict:
    """
    Generate SQL query from the execution plan.
    
    Takes the logic_plan and clarification to generate SQL.
    Includes retry context if previous execution failed.
    
    Returns:
        dict with sql_query
    """
    schema = state.get("schema", {})
    logic_plan = state.get("logic_plan", "")
    clarification = state.get("clarification", {})
    enrichment = state.get("schema_enrichment", {})
    
    # Build data samples context
    data_samples = ""
    if enrichment:
        data_samples = "DATA SAMPLES (use exact values shown):" + format_enrichment_for_prompt(enrichment)
    
    # Build clarification context
    clarification_context = ""
    if clarification:
        clarification_context = f"""
OUTPUT EXPECTATIONS (follow these EXACTLY):
- Output type: {clarification.get('output_type', 'unknown')}
- Description: {clarification.get('output_description', '')}
- Columns to return: {clarification.get('columns_to_return', [])}
- Calculation needed: {clarification.get('calculation_needed', 'none')}
- Calculation details: {clarification.get('calculation_details', '')}
"""
    
    # Build retry context if we're retrying
    retry_context = ""
    error = state.get("error", "")
    prev_sql = state.get("sql_query", "")
    iteration = state.get("iteration", 0)
    
    if error and prev_sql and iteration > 0:
        retry_context = f"""
PREVIOUS ATTEMPT FAILED:
Previous SQL: {prev_sql}
Error: {error}

Fix the error and write a corrected query.
"""
    
    schema_str = json.dumps(schema, indent=2)
    
    llm = get_llm()
    
    prompt = WRITER_PROMPT.format(
        schema=schema_str,
        data_samples=data_samples,
        plan=logic_plan,
        clarification_context=clarification_context,
        retry_context=retry_context,
    )
    
    response = llm.invoke([HumanMessage(content=prompt)])
    sql = clean_sql(response.content)
    
    # Validate SQL syntax
    is_valid, error_msg = validate_sql(sql)
    
    if not is_valid:
        # Return with error so retry loop can handle it
        return {
            "sql_query": sql,
            "error": f"SQL syntax error: {error_msg}",
            "execution": SQLExecution(
                executed=False,
                success=False,
                status="SyntaxError",
                elapsed_ms=None,
                results=[],
                error=error_msg,
            ),
            "iteration": iteration + 1,
        }
    
    return {"sql_query": sql}
