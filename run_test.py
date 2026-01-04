"""
Test runner: runs tests and compares to expected results.
Run: ./venv/bin/python run_test.py
"""

from langchain_core.messages import HumanMessage

from agent.state import AgentState
from agent.tools.db_tools import connect_to_db
from agent.nodes.gatekeeper import gatekeeper_node
from agent.nodes.organizer import organizer_node
from agent.nodes.planner import planner_node
from agent.nodes.writer import writer_node, validate_sql
from agent.nodes.execute import execute_node
from agent.nodes.analysis import analysis_node
from tests_config import TEST_DB, GATEKEEPER_TESTS, ORGANIZER_TESTS, PLANNER_TESTS, WRITER_TESTS, ANALYSIS_TESTS, EXECUTE_TESTS, SQL_VALIDATION_TESTS


def run_gatekeeper_tests(conn, schema):
    print("\n" + "="*50)
    print("GATEKEEPER TESTS")
    print("="*50)
    
    passed = 0
    failed = 0
    
    for test in GATEKEEPER_TESTS:
        state = AgentState(
            messages=[HumanMessage(content=test["message"])],
            schema=schema,
            conn=conn,
        )
        result = gatekeeper_node(state)
        is_legal = result.get("is_legal", True)
        
        success = is_legal == test["expected_legal"]
        status = "✓ PASS" if success else "✗ FAIL"
        
        print(f"\n{status} [{test['name']}]")
        print(f"  Message: {test['message']}")
        print(f"  Expected: is_legal={test['expected_legal']}")
        print(f"  Got:      is_legal={is_legal}")
        
        if success:
            passed += 1
        else:
            failed += 1
    
    return passed, failed


def run_organizer_tests(conn, schema):
    print("\n" + "="*50)
    print("ORGANIZER TESTS")
    print("="*50)
    
    passed = 0
    failed = 0
    
    for test in ORGANIZER_TESTS:
        state = AgentState(
            messages=[HumanMessage(content=test["message"])],
            schema=schema,
            conn=conn,
        )
        result = organizer_node(state)
        data_sources = result.get("data_sources", {})
        
        tables = data_sources.get("tables", [])
        joins = data_sources.get("joins", [])
        
        # Check tables (all expected tables should be present)
        tables_match = all(t in tables for t in test["expected_tables"])
        
        # Check joins (exact count or minimum count)
        if "expected_joins_count" in test:
            joins_match = len(joins) == test["expected_joins_count"]
            joins_expected = f"== {test['expected_joins_count']}"
        else:
            joins_match = len(joins) >= test["min_joins_count"]
            joins_expected = f">= {test['min_joins_count']}"
        
        success = tables_match and joins_match
        status = "✓ PASS" if success else "✗ FAIL"
        
        print(f"\n{status} [{test['name']}]")
        print(f"  Message: {test['message']}")
        print(f"  Expected tables: {test['expected_tables']}")
        print(f"  Got tables:      {tables}")
        print(f"  Expected joins:  {joins_expected}")
        print(f"  Got joins:       {len(joins)}")
        if joins:
            for j in joins:
                print(f"    - {j['left_table']}.{j['left_field']} -> {j['right_table']}.{j['right_field']}")
        
        if success:
            passed += 1
        else:
            failed += 1
    
    return passed, failed


def run_planner_tests(conn, schema):
    print("\n" + "="*50)
    print("PLANNER TESTS (display only)")
    print("="*50)
    
    for test in PLANNER_TESTS:
        state = AgentState(
            messages=[HumanMessage(content=test["message"])],
            schema=schema,
            conn=conn,
            data_sources=test["data_sources"],
        )
        result = planner_node(state)
        plan = result.get("logic_plan", "")
        
        print(f"\n--- [{test['name']}] ---")
        print(f"Question: {test['message']}")
        print(f"Plan:\n{plan}")


def run_writer_tests(conn, schema):
    print("\n" + "="*50)
    print("WRITER TESTS (display only)")
    print("="*50)
    
    for test in WRITER_TESTS:
        state = AgentState(
            schema=schema,
            conn=conn,
            logic_plan=test["logic_plan"],
        )
        result = writer_node(state)
        sql = result.get("sql_query", "")
        
        print(f"\n--- [{test['name']}] ---")
        print(f"Plan: {test['logic_plan'][:50]}...")
        print(f"SQL:\n{sql}")


def run_analysis_tests():
    print("\n" + "="*50)
    print("ANALYSIS TESTS (display only)")
    print("="*50)
    
    for test in ANALYSIS_TESTS:
        state = AgentState(
            messages=[HumanMessage(content=test["question"])],
            sql_query=test["sql"],
            query_result=test["results"],
        )
        result = analysis_node(state)
        answer = result.get("final_answer", "")
        
        print(f"\n--- [{test['name']}] ---")
        print(f"Question: {test['question']}")
        print(f"Results: {test['results'][:50]}...")
        print(f"Answer:\n{answer}")


def run_execute_tests(conn, schema):
    print("\n" + "="*50)
    print("EXECUTE TESTS")
    print("="*50)
    
    passed = 0
    failed = 0
    
    for test in EXECUTE_TESTS:
        state = AgentState(
            schema=schema,
            conn=conn,
            sql_query=test["sql"],
        )
        result = execute_node(state)
        execution = result.get("execution")
        
        success = execution.success == test["expected_success"]
        status_icon = "✓ PASS" if success else "✗ FAIL"
        
        print(f"\n{status_icon} [{test['name']}]")
        print(f"  SQL: {test['sql'][:60]}{'...' if len(test['sql']) > 60 else ''}")
        print(f"  Expected success: {test['expected_success']}")
        print(f"  Got success:      {execution.success}")
        print(f"  Status: {execution.status}")
        if execution.error:
            print(f"  Error: {execution.error}")
        if execution.success and execution.results:
            print(f"  Results: {execution.results[:3]}{'...' if len(execution.results) > 3 else ''}")
        
        if success:
            passed += 1
        else:
            failed += 1
    
    return passed, failed


def run_sql_validation_tests():
    print("\n" + "="*50)
    print("SQL VALIDATION TESTS")
    print("="*50)
    
    passed = 0
    failed = 0
    
    for test in SQL_VALIDATION_TESTS:
        is_valid, error_msg = validate_sql(test["sql"])
        
        success = is_valid == test["expected_valid"]
        status = "✓ PASS" if success else "✗ FAIL"
        
        print(f"\n{status} [{test['name']}]")
        print(f"  SQL: {test['sql'][:50]}{'...' if len(test['sql']) > 50 else ''}")
        print(f"  Expected valid: {test['expected_valid']}")
        print(f"  Got valid:      {is_valid}")
        if error_msg:
            print(f"  Error: {error_msg}")
        
        if success:
            passed += 1
        else:
            failed += 1
    
    return passed, failed


def main():
    print("Loading database...")
    conn, schema = connect_to_db(TEST_DB)
    print(f"Schema tables: {list(schema.keys())}")
    
    total_passed = 0
    total_failed = 0
    
    p, f = run_gatekeeper_tests(conn, schema)
    total_passed += p
    total_failed += f
    
    p, f = run_organizer_tests(conn, schema)
    total_passed += p
    total_failed += f
    
    run_planner_tests(conn, schema)
    run_writer_tests(conn, schema)
    run_analysis_tests()
    
    p, f = run_execute_tests(conn, schema)
    total_passed += p
    total_failed += f
    
    p, f = run_sql_validation_tests()
    total_passed += p
    total_failed += f
    
    conn.close()
    
    print("\n" + "="*50)
    print(f"SUMMARY: {total_passed} passed, {total_failed} failed")
    print("="*50)
    
    return total_failed == 0


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
