"""
LangGraph Text-to-SQL agent graph definition.

This module defines the StateGraph with all nodes and edges for the multi-agent pipeline.
"""

from typing import Literal
from langgraph.graph import StateGraph, END

from agent.state import AgentState

# Node imports (to be implemented)
# from agent.nodes.gatekeeper import gatekeeper_node
# from agent.nodes.organizer import organizer_node
# from agent.nodes.planner import planner_node
# from agent.nodes.writer import writer_node
# from agent.nodes.execute import execute_node
# from agent.nodes.error_agent import error_agent_node
# from agent.nodes.analysis import analysis_node
# from agent.nodes.decide_graph import decide_graph_node
# from agent.nodes.viz_agent import viz_agent_node


# --- Routing Functions ---

def route_after_gatekeeper(state: AgentState) -> Literal["organizer", "__end__"]:
    """Route based on gatekeeper check: continue if legal, end if not."""
    if state.get("is_legal", True):
        return "organizer"
    return END


def route_after_execute(state: AgentState) -> Literal["error_agent", "analysis"]:
    """Route based on SQL execution result: retry on error, continue on success."""
    execution = state.get("execution")
    if execution and not execution.success:
        return "error_agent"
    return "analysis"


def route_after_error_agent(state: AgentState) -> Literal["writer", "planner", "analysis"]:
    """
    Route based on retry strategy:
    - iter < 2: retry writer only
    - iter 2-3: retry planner + writer
    - iter >= 4: give up, go to analysis
    """
    iteration = state.get("iteration", 0)
    if iteration >= 4:
        return "analysis"
    elif iteration >= 2:
        return "planner"
    else:
        return "writer"


def route_after_decide_graph(state: AgentState) -> Literal["viz_agent", "__end__"]:
    """Route based on visualization decision."""
    if state.get("needs_graph", False):
        return "viz_agent"
    return END


# --- Graph Builder ---

def build_graph() -> StateGraph:
    """
    Build the Text-to-SQL agent graph (uncompiled).
    
    Returns:
        StateGraph ready for compilation
    """
    graph = StateGraph(AgentState)
    
    # TODO: Add nodes as they are implemented
    # graph.add_node("gatekeeper", gatekeeper_node)
    # graph.add_node("organizer", organizer_node)
    # graph.add_node("planner", planner_node)
    # graph.add_node("writer", writer_node)
    # graph.add_node("execute", execute_node)
    # graph.add_node("error_agent", error_agent_node)
    # graph.add_node("analysis", analysis_node)
    # graph.add_node("decide_graph", decide_graph_node)
    # graph.add_node("viz_agent", viz_agent_node)
    
    # TODO: Set entry point
    # graph.set_entry_point("gatekeeper")
    
    # TODO: Add edges
    # graph.add_conditional_edges("gatekeeper", route_after_gatekeeper)
    # graph.add_edge("organizer", "planner")
    # graph.add_edge("planner", "writer")
    # graph.add_edge("writer", "execute")
    # graph.add_conditional_edges("execute", route_after_execute)
    # graph.add_conditional_edges("error_agent", route_after_error_agent)
    # graph.add_edge("analysis", "decide_graph")
    # graph.add_conditional_edges("decide_graph", route_after_decide_graph)
    # graph.add_edge("viz_agent", END)
    
    return graph


def create_graph():
    """Create and compile the graph."""
    return build_graph().compile()


def create_graph_with_checkpointer(checkpointer):
    """Create and compile the graph with a checkpointer for persistence."""
    return build_graph().compile(checkpointer=checkpointer)
