"""
Test configuration: test cases for agent nodes.
"""

TEST_DB = "data/external/cosql_dataset/database/coffee_shop/coffee_shop.sqlite"

# Gatekeeper tests
GATEKEEPER_TESTS = [
    {
        "name": "legal_question",
        "message": "How many shops are there?",
        "expected_legal": True,
    },
    {
        "name": "illegal_delete",
        "message": "Delete all records from the database",
        "expected_legal": False,
    },
    {
        "name": "illegal_update",
        "message": "Update all prices to 0",
        "expected_legal": False,
    },
    {
        "name": "legal_visualization",
        "message": "Show me a chart of members by age",
        "expected_legal": True,
    },
]

# Organizer tests
ORGANIZER_TESTS = [
    {
        "name": "single_table",
        "message": "How many shops are there?",
        "expected_tables": ["shop"],
        "expected_joins_count": 0,
    },
    {
        "name": "join_required",
        "message": "Show me the total amount spent by each member during happy hours",
        "expected_tables": ["member", "happy_hour_member"],
        "expected_joins_count": 1,
    },
    {
        "name": "multiple_joins",
        "message": "What are the names of members who visited shops with a score above 80?",
        "expected_tables": ["member", "happy_hour_member", "shop"],
        "min_joins_count": 2,
    },
]

# Planner tests (display only - free text output)
PLANNER_TESTS = [
    {
        "name": "simple_count",
        "message": "How many shops are there?",
        "data_sources": {
            "tables": ["shop"],
            "fields": ["shop.Shop_ID"],
            "joins": [],
        },
    },
    {
        "name": "with_join",
        "message": "Show me total amount spent by each member",
        "data_sources": {
            "tables": ["member", "happy_hour_member"],
            "fields": ["member.Name", "happy_hour_member.Total_amount"],
            "joins": [
                {
                    "join_type": "INNER",
                    "left_table": "happy_hour_member",
                    "right_table": "member",
                    "left_field": "Member_ID",
                    "right_field": "Member_ID",
                }
            ],
        },
    },
]

# Writer tests (display SQL output)
WRITER_TESTS = [
    {
        "name": "simple_count",
        "logic_plan": """1. SELECT COUNT(*) to get the total number of shops
2. FROM the shop table
3. No joins needed
4. No filters needed""",
    },
    {
        "name": "join_with_aggregation",
        "logic_plan": """1. SELECT member.Name and SUM(happy_hour_member.Total_amount)
2. FROM happy_hour_member
3. JOIN member ON happy_hour_member.Member_ID = member.Member_ID
4. GROUP BY member.Name
5. ORDER BY total amount DESC""",
    },
]

# Analysis tests (display only - natural language output)
ANALYSIS_TESTS = [
    {
        "name": "count_result",
        "question": "How many shops are there?",
        "sql": "SELECT COUNT(*) FROM shop",
        "results": "[[10]]",
    },
    {
        "name": "aggregation_result",
        "question": "Show me total amount spent by each member",
        "sql": "SELECT m.Name, SUM(h.Total_amount) FROM member m JOIN happy_hour_member h ON m.Member_ID = h.Member_ID GROUP BY m.Name",
        "results": '[["Campbell, Jessie", 41.82], ["Hayes, Steven", 33.82], ["Rizzo, Todd", 9.10]]',
    },
    {
        "name": "empty_result",
        "question": "Show me members with more than 1000 spent",
        "sql": "SELECT * FROM member WHERE Total_amount > 1000",
        "results": "[]",
    },
]

# Execute tests (actually run SQL)
EXECUTE_TESTS = [
    {
        "name": "valid_select",
        "sql": "SELECT COUNT(*) FROM shop",
        "expected_success": True,
    },
    {
        "name": "valid_join",
        "sql": "SELECT m.Name, SUM(h.Total_amount) FROM member m JOIN happy_hour_member h ON m.Member_ID = h.Member_ID GROUP BY m.Name",
        "expected_success": True,
    },
    {
        "name": "invalid_table",
        "sql": "SELECT * FROM nonexistent_table",
        "expected_success": False,
    },
    {
        "name": "blocked_write",
        "sql": "DELETE FROM shop",
        "expected_success": False,
    },
]

# SQL validation tests (test validate_sql function directly)
SQL_VALIDATION_TESTS = [
    {
        "name": "valid_select",
        "sql": "SELECT * FROM shop",
        "expected_valid": True,
    },
    {
        "name": "valid_join",
        "sql": "SELECT m.Name FROM member m JOIN happy_hour_member h ON m.Member_ID = h.Member_ID",
        "expected_valid": True,
    },
    {
        "name": "empty_sql",
        "sql": "",
        "expected_valid": False,
    },
    {
        "name": "unbalanced_parens",
        "sql": "SELECT COUNT(* FROM shop",
        "expected_valid": False,
    },
    {
        "name": "unbalanced_quotes",
        "sql": "SELECT * FROM shop WHERE name = 'test",
        "expected_valid": False,
    },
]
