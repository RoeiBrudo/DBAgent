"""
Execute node: runs SQL query safely against the database.
"""

import json
from agent.state import AgentState, SQLExecution
from agent.tools.db_tools import safe_query
from config import SQL_TIMEOUT_MS


def execute_node(state: AgentState) -> dict:
    """
    Execute the SQL query safely using read-only connection.
    
    Uses safe_query with:
    - Read-only whitelist check
    - Timeout protection
    - Connection is already read-only from state (opened with mode=ro)
    
    Returns:
        dict with execution results, query_result JSON, and error if failed
    """
    conn = state.get("conn")
    sql_query = state.get("sql_query", "")
    iteration = state.get("iteration", 0)
    
    if not conn:
        return {
            "execution": SQLExecution(
                executed=False,
                success=False,
                status="NoConnection",
                elapsed_ms=None,
                results=[],
                error="No database connection available",
            ),
            "error": "No database connection available",
            "iteration": iteration + 1,
        }
    
    if not sql_query:
        return {
            "execution": SQLExecution(
                executed=False,
                success=False,
                status="NoQuery",
                elapsed_ms=None,
                results=[],
                error="No SQL query to execute",
            ),
            "error": "No SQL query to execute",
            "iteration": iteration + 1,
        }
    
    # Execute query with timeout
    result = safe_query(conn, sql_query, SQL_TIMEOUT_MS)
    
    execution = SQLExecution(
        executed=True,
        success=result["success"],
        status=result["status"],
        elapsed_ms=result["elapsed_ms"],
        results=result["results"],
        error=result["error"],
    )
    
    output = {"execution": execution}
    
    if result["success"]:
        # Convert results to JSON for downstream nodes
        output["query_result"] = json.dumps(result["results"])
        output["error"] = ""
    else:
        output["query_result"] = ""
        output["error"] = result["error"] or "Query execution failed"
        output["iteration"] = iteration + 1
    
    return output
