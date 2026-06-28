"""
ValidateX Integration — Snowflake Data Warehouse Support.

Provides push-down SQL validation for Snowflake tables and queries using SQLAlchemy.
"""

from __future__ import annotations

from typing import Any, Optional
import validatex as vx
from validatex.core.result import ValidationResult
from validatex.core.suite import ExpectationSuite


class ValidateXSnowflake:
    """
    Helper for validating Snowflake tables or custom queries with zero data loading.

    Examples
    --------
    >>> from sqlalchemy import create_engine
    >>> engine = create_engine("snowflake://user:pass@account/db/schema?warehouse=wh")
    >>> result = ValidateXSnowflake.validate_table(engine, "prod_customers", suite)
    """

    @staticmethod
    def validate_table(
        sql_engine: Any,
        table_name: str,
        suite: ExpectationSuite,
    ) -> ValidationResult:
        """
        Validate a Snowflake table directly inside the database via Push-Down SQL.

        Parameters
        ----------
        sql_engine : sqlalchemy.engine.Engine
            SQLAlchemy connection engine configured for Snowflake.
        table_name : str
            Target table name.
        suite : ExpectationSuite
            The expectations to evaluate.
        """
        return vx.validate(
            data=table_name,
            suite=suite,
            engine="sql",
            sql_engine=sql_engine,
            data_source=f"snowflake://{table_name}",
        )
