"""
ValidateX Integration — Google BigQuery Support.

Provides push-down SQL validation for BigQuery datasets and tables using SQLAlchemy.
"""

from __future__ import annotations

from typing import Any, Optional
import validatex as vx
from validatex.core.result import ValidationResult
from validatex.core.suite import ExpectationSuite


class ValidateXBigQuery:
    """
    Helper for validating Google BigQuery tables with Push-Down SQL execution.

    Examples
    --------
    >>> from sqlalchemy import create_engine
    >>> engine = create_engine("bigquery://my-project/my_dataset")
    >>> result = ValidateXBigQuery.validate_table(engine, "users", suite)
    """

    @staticmethod
    def validate_table(
        sql_engine: Any,
        table_name: str,
        suite: ExpectationSuite,
    ) -> ValidationResult:
        """Validate a BigQuery table directly using push-down SQL queries."""
        return vx.validate(
            data=table_name,
            suite=suite,
            engine="sql",
            sql_engine=sql_engine,
            data_source=f"bigquery://{table_name}",
        )
