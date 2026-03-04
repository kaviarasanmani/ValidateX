"""
Table-level expectations.

These expectations operate on the DataFrame as a whole rather than
on individual columns.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from validatex.core.expectation import Expectation, register_expectation
from validatex.core.result import ExpectationResult

# ---------------------------------------------------------------------------
# 1. expect_table_row_count_to_equal
# ---------------------------------------------------------------------------


@register_expectation
@dataclass
class ExpectTableRowCountToEqual(Expectation):
    """Expect the total number of rows to equal an exact count."""

    expectation_type: str = field(init=False, default="expect_table_row_count_to_equal")

    def _validate_pandas(self, df: pd.DataFrame) -> ExpectationResult:
        expected = self.kwargs.get("value", 0)
        actual = len(df)
        return self._build_result(
            success=(actual == expected),
            observed_value=actual,
            element_count=actual,
            details={"expected_count": expected, "actual_count": actual},
        )

    def _validate_spark(self, df: Any) -> ExpectationResult:
        expected = self.kwargs.get("value", 0)
        actual = df.count()
        return self._build_result(
            success=(actual == expected),
            observed_value=actual,
            element_count=actual,
            details={"expected_count": expected, "actual_count": actual},
        )


    def _validate_sql(self, sql_source: Any) -> ExpectationResult:
        from sqlalchemy import text

        engine, query_or_table = sql_source
        expected = self.kwargs.get("value", 0)

        # Wrap the original query/table in a COUNT(*)
        query = f"SELECT COUNT(*) FROM ({query_or_table}) AS subquery"
        with engine.connect() as conn:
            actual = conn.execute(text(query)).scalar()

        return self._build_result(
            success=(actual == expected),
            observed_value=actual,
            element_count=actual,
            details={"expected_count": expected, "actual_count": actual},
        )


@register_expectation
@dataclass
class ExpectTableRowCountToBeBetween(Expectation):
    """Expect the row count to be within [min_value, max_value]."""

    expectation_type: str = field(
        init=False, default="expect_table_row_count_to_be_between"
    )

    def _validate_pandas(self, df: pd.DataFrame) -> ExpectationResult:
        min_val = self.kwargs.get("min_value", 0)
        max_val = self.kwargs.get("max_value", float("inf"))
        actual = len(df)
        success = min_val <= actual <= max_val
        return self._build_result(
            success=success,
            observed_value=actual,
            element_count=actual,
            details={
                "min_value": min_val,
                "max_value": max_val,
                "actual_count": actual,
            },
        )

    def _validate_spark(self, df: Any) -> ExpectationResult:
        min_val = self.kwargs.get("min_value", 0)
        max_val = self.kwargs.get("max_value", float("inf"))
        actual = df.count()
        success = min_val <= actual <= max_val
        return self._build_result(
            success=success,
            observed_value=actual,
            element_count=actual,
            details={
                "min_value": min_val,
                "max_value": max_val,
                "actual_count": actual,
            },
        )

    def _validate_sql(self, sql_source: Any) -> ExpectationResult:
        from sqlalchemy import text

        engine, query_or_table = sql_source
        min_val = self.kwargs.get("min_value", 0)
        max_val = self.kwargs.get("max_value", float("inf"))

        query = f"SELECT COUNT(*) FROM ({query_or_table}) AS subquery"
        with engine.connect() as conn:
            actual = conn.execute(text(query)).scalar()

        success = True
        if min_val is not None:
            success = success and (actual >= min_val)
        if max_val is not None:
            success = success and (actual <= max_val)

        return self._build_result(
            success=success,
            observed_value=actual,
            element_count=actual,
            details={"min_value": min_val, "max_value": max_val, "actual_count": actual},
        )


# ---------------------------------------------------------------------------
# 3. expect_table_columns_to_match_ordered_list
# ---------------------------------------------------------------------------


@register_expectation
@dataclass
class ExpectTableColumnsToMatchOrderedList(Expectation):
    """Expect the columns of a DataFrame to match an exact ordered list."""

    expectation_type: str = field(
        init=False, default="expect_table_columns_to_match_ordered_list"
    )

    def _validate_pandas(self, df: pd.DataFrame) -> ExpectationResult:
        expected_columns = self.kwargs.get("column_list", [])
        actual_columns = list(df.columns)
        success = actual_columns == expected_columns
        return self._build_result(
            success=success,
            observed_value=actual_columns,
            details={
                "expected_columns": expected_columns,
                "actual_columns": actual_columns,
                "missing": [c for c in expected_columns if c not in actual_columns],
                "extra": [c for c in actual_columns if c not in expected_columns],
            },
        )

    def _validate_spark(self, df: Any) -> ExpectationResult:
        expected_columns = self.kwargs.get("column_list", [])
        actual_columns = df.columns
        success = actual_columns == expected_columns
        return self._build_result(
            success=success,
            observed_value=actual_columns,
            details={
                "expected_columns": expected_columns,
                "actual_columns": actual_columns,
                "missing": [c for c in expected_columns if c not in actual_columns],
                "extra": [c for c in actual_columns if c not in expected_columns],
            },
        )


# ---------------------------------------------------------------------------
# 4. expect_table_columns_to_match_set
# ---------------------------------------------------------------------------


@register_expectation
@dataclass
class ExpectTableColumnsToMatchSet(Expectation):
    """Expect the set of column names to match (order-independent)."""

    expectation_type: str = field(
        init=False, default="expect_table_columns_to_match_set"
    )

    def _validate_pandas(self, df: pd.DataFrame) -> ExpectationResult:
        expected_set = set(self.kwargs.get("column_set", []))
        actual_set = set(df.columns)
        exact = self.kwargs.get("exact_match", True)

        if exact:
            success = actual_set == expected_set
        else:
            success = expected_set.issubset(actual_set)

        return self._build_result(
            success=success,
            observed_value=sorted(actual_set),
            details={
                "expected": sorted(expected_set),
                "missing": sorted(expected_set - actual_set),
                "extra": sorted(actual_set - expected_set),
            },
        )

    def _validate_spark(self, df: Any) -> ExpectationResult:
        expected_set = set(self.kwargs.get("column_set", []))
        actual_set = set(df.columns)
        exact = self.kwargs.get("exact_match", True)

        if exact:
            success = actual_set == expected_set
        else:
            success = expected_set.issubset(actual_set)

        return self._build_result(
            success=success,
            observed_value=sorted(actual_set),
            details={
                "expected": sorted(expected_set),
                "missing": sorted(expected_set - actual_set),
                "extra": sorted(actual_set - expected_set),
            },
        )


# ---------------------------------------------------------------------------
# 5. expect_table_column_count_to_equal
# ---------------------------------------------------------------------------


@register_expectation
@dataclass
class ExpectTableColumnCountToEqual(Expectation):
    """Expect the number of columns to match an exact value."""

    expectation_type: str = field(
        init=False, default="expect_table_column_count_to_equal"
    )

    def _validate_pandas(self, df: pd.DataFrame) -> ExpectationResult:
        expected = self.kwargs.get("value", 0)
        actual = len(df.columns)
        return self._build_result(
            success=(actual == expected),
            observed_value=actual,
            details={"expected_count": expected, "actual_count": actual},
        )

    def _validate_spark(self, df: Any) -> ExpectationResult:
        expected = self.kwargs.get("value", 0)
        actual = len(df.columns)
        return self._build_result(
            success=(actual == expected),
            observed_value=actual,
            details={"expected_count": expected, "actual_count": actual},
        )
