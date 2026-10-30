"""
ValidateX — Cross-Row & Custom Python Expectations.

Row-level conditional checks: If column A has value X,
then column B must satisfy condition Y. Plus user-defined
Python lambda expectations.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Optional

import pandas as pd

from validatex.core.expectation import Expectation, register_expectation
from validatex.core.result import ExpectationResult
from validatex.core.utils import format_sql_from_clause, quote_sql_identifier

# ---------------------------------------------------------------------------
# 1. expect_column_values_to_be_null_when (conditional null)
# ---------------------------------------------------------------------------


@register_expectation
@dataclass
class ExpectColumnValuesToBeNullWhen(Expectation):
    """
    Expect a column to be null when a condition column equals a specific value.

    Example: "refund_amount should be null when status != 'refunded'"

    kwargs:
        condition_column (str): The column to check the condition against.
        condition_value (any): The value of condition_column that triggers the rule.
        when (str): 'equal' (default) or 'not_equal'
    """

    expectation_type: str = field(init=False, default="expect_column_values_to_be_null_when")

    def _validate_pandas(self, df: pd.DataFrame) -> ExpectationResult:
        cond_col = self.kwargs.get("condition_column")
        cond_val = self.kwargs.get("condition_value")
        when = self.kwargs.get("when", "equal")

        if when == "equal":
            condition_mask = df[cond_col] == cond_val
        else:
            condition_mask = df[cond_col] != cond_val

        # Rows where condition is TRUE but target column is NOT null
        scoped = df[condition_mask]
        total = len(scoped)
        unexpected_mask = scoped[self.column].notnull()
        unexpected_count = int(unexpected_mask.sum())
        pct = (unexpected_count / total * 100) if total > 0 else 0.0

        return self._build_result(
            success=(unexpected_count == 0),
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            unexpected_values=scoped.loc[unexpected_mask, self.column].tolist()[:20],
            details={"condition": f"{cond_col} {when} {cond_val!r}"},
        )

    def _validate_spark(self, df: Any) -> ExpectationResult:
        from pyspark.sql import functions as F

        cond_col = str(self.kwargs.get("condition_column"))
        cond_val = self.kwargs.get("condition_value")
        when = self.kwargs.get("when", "equal")
        target_col = str(self.column)

        if when == "equal":
            scoped = df.filter(F.col(cond_col) == cond_val)
        else:
            scoped = df.filter(F.col(cond_col) != cond_val)

        total = scoped.count()
        unexpected_df = scoped.filter(F.col(target_col).isNotNull())
        unexpected_count = unexpected_df.count()
        pct = (unexpected_count / total * 100) if total > 0 else 0.0
        unexpected_vals = [r[0] for r in unexpected_df.select(target_col).limit(20).collect()]

        return self._build_result(
            success=(unexpected_count == 0),
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            unexpected_values=unexpected_vals,
            details={"condition": f"{cond_col} {when} {cond_val!r}"},
        )

    def _validate_polars(self, df: Any) -> ExpectationResult:
        cond_col = self.kwargs.get("condition_column")
        cond_val = self.kwargs.get("condition_value")
        when = self.kwargs.get("when", "equal")

        if when == "equal":
            condition_mask = df[cond_col] == cond_val
        else:
            condition_mask = df[cond_col] != cond_val

        scoped = df.filter(condition_mask)
        total = len(scoped)
        unexpected_mask = scoped[self.column].is_not_null()
        unexpected_count = int(unexpected_mask.sum())
        pct = (unexpected_count / total * 100) if total > 0 else 0.0

        return self._build_result(
            success=(unexpected_count == 0),
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            unexpected_values=scoped[self.column].filter(unexpected_mask).to_list()[:20],
            details={"condition": f"{cond_col} {when} {cond_val!r}"},
        )

    def _validate_sql(self, sql_source: Any) -> ExpectationResult:
        from sqlalchemy import text

        engine, query_or_table = sql_source
        cond_col = self.kwargs.get("condition_column")
        cond_val = self.kwargs.get("condition_value")
        when = self.kwargs.get("when", "equal")

        q_col = quote_sql_identifier(self.column)
        q_cond = quote_sql_identifier(cond_col)
        from_clause = format_sql_from_clause(query_or_table)
        op = "=" if when == "equal" else "!="

        query = (
            f"SELECT COUNT(*) AS total, "  # nosec B608
            f"COALESCE(SUM(CASE WHEN {q_col} IS NOT NULL THEN 1 ELSE 0 END), 0) AS unexpected "
            f"FROM {from_clause} WHERE {q_cond} {op} :cond_val"
        )
        with engine.connect() as conn:
            row = conn.execute(text(query), {"cond_val": cond_val}).fetchone()
            total = int(row.total) if row and row.total else 0
            unexpected_count = int(row.unexpected) if row and row.unexpected else 0

        pct = (unexpected_count / total * 100) if total > 0 else 0.0
        return self._build_result(
            success=(unexpected_count == 0),
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            details={"condition": f"{cond_col} {when} {cond_val!r}"},
        )


# ---------------------------------------------------------------------------
# 2. expect_column_values_to_be_not_null_when (conditional not-null)
# ---------------------------------------------------------------------------


@register_expectation
@dataclass
class ExpectColumnValuesToBeNotNullWhen(Expectation):
    """
    Expect a column to NOT be null when a condition is met.

    Example: "shipping_address must not be null when order_type == 'physical'"

    kwargs:
        condition_column (str): The column whose value triggers this rule.
        condition_value (any): The trigger value.
        when (str): 'equal' (default) or 'not_equal'
    """

    expectation_type: str = field(init=False, default="expect_column_values_to_be_not_null_when")

    def _validate_pandas(self, df: pd.DataFrame) -> ExpectationResult:
        cond_col = self.kwargs.get("condition_column")
        cond_val = self.kwargs.get("condition_value")
        when = self.kwargs.get("when", "equal")

        if when == "equal":
            condition_mask = df[cond_col] == cond_val
        else:
            condition_mask = df[cond_col] != cond_val

        scoped = df[condition_mask]
        total = len(scoped)
        unexpected_mask = scoped[self.column].isnull()
        unexpected_count = int(unexpected_mask.sum())
        pct = (unexpected_count / total * 100) if total > 0 else 0.0

        return self._build_result(
            success=(unexpected_count == 0),
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            details={"condition": f"{cond_col} {when} {cond_val!r}"},
        )

    def _validate_spark(self, df: Any) -> ExpectationResult:
        from pyspark.sql import functions as F

        cond_col = str(self.kwargs.get("condition_column"))
        cond_val = self.kwargs.get("condition_value")
        when = self.kwargs.get("when", "equal")
        target_col = str(self.column)

        if when == "equal":
            scoped = df.filter(F.col(cond_col) == cond_val)
        else:
            scoped = df.filter(F.col(cond_col) != cond_val)

        total = scoped.count()
        unexpected_count = scoped.filter(F.col(target_col).isNull()).count()
        pct = (unexpected_count / total * 100) if total > 0 else 0.0

        return self._build_result(
            success=(unexpected_count == 0),
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            details={"condition": f"{cond_col} {when} {cond_val!r}"},
        )

    def _validate_polars(self, df: Any) -> ExpectationResult:
        cond_col = self.kwargs.get("condition_column")
        cond_val = self.kwargs.get("condition_value")
        when = self.kwargs.get("when", "equal")

        if when == "equal":
            condition_mask = df[cond_col] == cond_val
        else:
            condition_mask = df[cond_col] != cond_val

        scoped = df.filter(condition_mask)
        total = len(scoped)
        unexpected_mask = scoped[self.column].is_null()
        unexpected_count = int(unexpected_mask.sum())
        pct = (unexpected_count / total * 100) if total > 0 else 0.0

        return self._build_result(
            success=(unexpected_count == 0),
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            details={"condition": f"{cond_col} {when} {cond_val!r}"},
        )

    def _validate_sql(self, sql_source: Any) -> ExpectationResult:
        from sqlalchemy import text

        engine, query_or_table = sql_source
        cond_col = self.kwargs.get("condition_column")
        cond_val = self.kwargs.get("condition_value")
        when = self.kwargs.get("when", "equal")

        q_col = quote_sql_identifier(self.column)
        q_cond = quote_sql_identifier(cond_col)
        from_clause = format_sql_from_clause(query_or_table)
        op = "=" if when == "equal" else "!="

        query = (
            f"SELECT COUNT(*) AS total, "  # nosec B608
            f"COALESCE(SUM(CASE WHEN {q_col} IS NULL THEN 1 ELSE 0 END), 0) AS unexpected "
            f"FROM {from_clause} WHERE {q_cond} {op} :cond_val"
        )
        with engine.connect() as conn:
            row = conn.execute(text(query), {"cond_val": cond_val}).fetchone()
            total = int(row.total) if row and row.total else 0
            unexpected_count = int(row.unexpected) if row and row.unexpected else 0

        pct = (unexpected_count / total * 100) if total > 0 else 0.0
        return self._build_result(
            success=(unexpected_count == 0),
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            details={"condition": f"{cond_col} {when} {cond_val!r}"},
        )


# ---------------------------------------------------------------------------
# 3. expect_column_values_to_satisfy (user-defined lambda)
# ---------------------------------------------------------------------------


@register_expectation
@dataclass
class ExpectColumnValuesToSatisfy(Expectation):
    """
    Expect each non-null column value to satisfy a custom Python callable.

    Usage (Python API only):
        from validatex.expectations.conditional_expectations import ExpectColumnValuesToSatisfy

        exp = ExpectColumnValuesToSatisfy(
            column="price",
            condition=lambda x: x > 0 and x < 1_000_000
        )
    """

    expectation_type: str = field(init=False, default="expect_column_values_to_satisfy")

    def _validate_pandas(self, df: pd.DataFrame) -> ExpectationResult:
        condition: Optional[Callable] = self.kwargs.get("condition")
        if condition is None:
            raise ValueError("You must pass a 'condition' callable to ExpectColumnValuesToSatisfy.")

        series = df[self.column].dropna()
        total = len(series)
        try:
            result_mask = series.apply(condition)
        except Exception as e:
            raise ValueError(f"Your condition raised an error: {e}")

        unexpected_mask = ~result_mask
        unexpected_count = int(unexpected_mask.sum())
        pct = (unexpected_count / total * 100) if total > 0 else 0.0

        return self._build_result(
            success=(unexpected_count == 0),
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            unexpected_values=series[unexpected_mask].tolist()[:20],
            details={"condition": str(condition)},
        )

    def _validate_spark(self, df: Any) -> ExpectationResult:
        from pyspark.sql import functions as F

        condition: Optional[Callable] = self.kwargs.get("condition")
        if condition is None:
            raise ValueError("You must pass a 'condition' callable to ExpectColumnValuesToSatisfy.")

        col_name = str(self.column)
        filtered = df.filter(F.col(col_name).isNotNull())
        total = filtered.count()

        @F.udf(returnType="boolean")
        def check_cond(val):
            return bool(condition(val))

        unexpected_df = filtered.filter(~check_cond(F.col(col_name)))
        unexpected_count = unexpected_df.count()
        pct = (unexpected_count / total * 100) if total > 0 else 0.0
        unexpected_vals = [r[0] for r in unexpected_df.select(col_name).limit(20).collect()]

        return self._build_result(
            success=(unexpected_count == 0),
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            unexpected_values=unexpected_vals,
            details={"condition": str(condition)},
        )

    def _validate_polars(self, df: Any) -> ExpectationResult:
        import polars as pl

        condition: Optional[Callable] = self.kwargs.get("condition")
        if condition is None:
            raise ValueError("You must pass a 'condition' callable to ExpectColumnValuesToSatisfy.")

        series = df[self.column].drop_nulls()
        total = len(series)
        try:
            if hasattr(series, "map_elements"):
                result_mask = series.map_elements(condition, return_dtype=pl.Boolean)
            else:
                result_mask = series.apply(condition, return_dtype=pl.Boolean)
        except Exception as e:
            raise ValueError(f"Your condition raised an error: {e}")

        unexpected_mask = result_mask.not_()
        unexpected_count = int(unexpected_mask.sum())
        pct = (unexpected_count / total * 100) if total > 0 else 0.0

        return self._build_result(
            success=(unexpected_count == 0),
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            unexpected_values=series.filter(unexpected_mask).to_list()[:20],
            details={"condition": str(condition)},
        )
