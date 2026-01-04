"""
Agent state schema for the Text-to-SQL LangGraph pipeline.

This module defines the TypedDict state that flows through all agent nodes.
"""

import sqlite3
from dataclasses import dataclass
from typing import Annotated, Any, Dict, List, Optional
from typing_extensions import TypedDict
from langgraph.graph.message import add_messages
from langchain_core.messages import BaseMessage


@dataclass(frozen=True)
class SQLExecution:
    """
    Result of a SQL query execution.
    
    Attributes:
        executed: Whether execution was attempted
        success: Whether execution succeeded
        status: Status code (success, NoSafe, TimeOut, ExecFailed)
        elapsed_ms: Execution time in milliseconds
        results: Query result rows
        error: Error message if execution failed
    """
    executed: bool
    success: bool
    status: str
    elapsed_ms: Optional[float]
    results: List[Any]
    error: Optional[str] = None


class JoinInfo(TypedDict):
    """Describes a single join operation."""
    join_type: str        # INNER, LEFT, RIGHT, CROSS
    left_table: str
    right_table: str
    left_field: str
    right_field: str


class DataSources(TypedDict):
    """
    Output of the organizer node: identified tables, fields, and joins.
    
    Attributes:
        tables: List of table names needed for the query
        fields: List of field names in format "table.column" or "column"
        joins: List of JoinInfo describing how tables connect
    """
    tables: List[str]
    fields: List[str]
    joins: List[JoinInfo]


class AgentState(TypedDict, total=False):
    """
    State that flows through the Text-to-SQL agent graph.
    
    Conversation:
        messages: Full conversation history (LangGraph add_messages reducer)
    
    DB Context (set once per conversation):
        conn: SQLite database connection
        schema: Database schema {table_name: [column_names]}
    
    SQL Pipeline Outputs:
        data_sources: Tables, fields, joins identified by organizer
        logic_plan: Plain English SQL execution plan from planner
        sql_query: Final SQL query from writer
    
    Execution:
        query_result: JSON string of query results
        execution: SQLExecution dataclass with status, timing, results
        final_answer: Natural language answer for the user
    
    Control Flow:
        error: Error message if SQL execution failed
        iteration: Retry counter for error recovery
        is_legal: Whether the request is legal (questions/visualization only, no commands)
    
    Visualization:
        needs_graph: Whether a visualization should be generated
        graph_type: Type of graph (bar, line, pie, scatter)
        graph_json: Plotly figure as JSON string
    """
    
    # Conversation
    messages: Annotated[List[BaseMessage], add_messages]
    
    # DB context (set once per conversation)
    conn: sqlite3.Connection
    schema: Dict[str, List[str]]
    schema_enrichment: dict  # Data samples and column enumerations
    
    # SQL pipeline outputs
    data_sources: DataSources
    logic_plan: str
    clarification: dict  # Output expectations from clarifier node
    sql_query: str
    
    # Execution
    query_result: str
    execution: SQLExecution
    final_answer: str
    
    # Control flow
    error: str
    iteration: int
    is_legal: bool
    validation_passed: bool
    
    # Visualization
    needs_graph: bool
    graph_type: str
    graph_json: str


def create_initial_state(conn: sqlite3.Connection, schema: Dict[str, List[str]]) -> AgentState:
    """
    Create an initial state for a new conversation.
    
    Args:
        conn: SQLite database connection from connect_to_db()
        schema: Database schema from connect_to_db()
    
    Returns:
        AgentState with DB context initialized and defaults set
    """
    return AgentState(
        messages=[],
        conn=conn,
        schema=schema,
        data_sources={"tables": [], "fields": [], "joins": []},
        logic_plan="",
        sql_query="",
        query_result="",
        execution=None,
        final_answer="",
        error="",
        iteration=0,
        is_legal=True,
        needs_graph=False,
        graph_type="",
        graph_json="",
    )
