"""
ValidateX Integration — DuckDB Support.

Provides seamless validation for DuckDB tables, relations, and SQL query results.
"""

from __future__ import annotations

from typing import Any, Optional, Union
import validatex as vx
from validatex.core.result import ValidationResult
from validatex.core.suite import ExpectationSuite


class ValidateXDuckDB:
    """
    Helper for validating DuckDB tables or queries with ValidateX.

    Examples
    --------
    >>> import duckdb
    >>> conn = duckdb.connect()
    >>> conn.execute("CREATE TABLE users AS SELECT 1 AS user_id, 'alice@test.com' AS email")
    >>> result = ValidateXDuckDB.validate_table(conn, "users", suite)
    """

    @staticmethod
    def validate_table(
        conn: Any,
        table_name: str,
        suite: ExpectationSuite,
        engine: str = "polars",
    ) -> ValidationResult:
        """
        Validate a DuckDB table by fetching a arrow/polars/pandas dataframe.

        Parameters
        ----------
        conn : duckdb.DuckDBPyConnection
            Active DuckDB connection.
        table_name : str
            Name of the table to validate.
        suite : ExpectationSuite
            The validation rules to execute.
        engine : str, default "polars"
            Underlying engine to use for evaluation ("polars" or "pandas").
        """
        if engine.lower() == "polars":
            try:
                df = conn.query(f"SELECT * FROM {table_name}").pl()
                return vx.validate(df, suite, engine="polars", data_source=f"duckdb://{table_name}")
            except Exception:
                pass

        df = conn.query(f"SELECT * FROM {table_name}").df()
        return vx.validate(df, suite, engine="pandas", data_source=f"duckdb://{table_name}")

    @staticmethod
    def validate_query(
        conn: Any,
        sql_query: str,
        suite: ExpectationSuite,
        engine: str = "polars",
    ) -> ValidationResult:
        """Validate the result of a custom DuckDB SQL query."""
        if engine.lower() == "polars":
            try:
                df = conn.query(sql_query).pl()
                return vx.validate(df, suite, engine="polars", data_source="duckdb://custom_query")
            except Exception:
                pass

        df = conn.query(sql_query).df()
        return vx.validate(df, suite, engine="pandas", data_source="duckdb://custom_query")
