"""
Unit tests for validatex.integrations.duckdb.ValidateXDuckDB
"""

import pytest
import validatex as vx

def test_duckdb_validation():
    try:
        import duckdb
    except ImportError:
        pytest.skip("duckdb not installed")

    from validatex.integrations.duckdb import ValidateXDuckDB

    conn = duckdb.connect()
    conn.execute("CREATE TABLE users AS SELECT 1 AS user_id, 25 AS age, 'active' AS status")

    suite = (
        vx.ExpectationSuite("duckdb_test")
        .add("expect_column_to_not_be_null", column="user_id")
        .add("expect_column_values_to_be_between", column="age", min_value=18, max_value=80)
    )

    result = ValidateXDuckDB.validate_table(conn, "users", suite)
    assert result.success is True
    assert result.total_expectations == 2
