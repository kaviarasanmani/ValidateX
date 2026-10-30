"""
Aggregate / cross-column expectations.

Expectations that involve relationships between multiple columns
or aggregated computations.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from validatex.core.expectation import Expectation, register_expectation
from validatex.core.result import ExpectationResult
from validatex.core.utils import format_sql_from_clause, quote_sql_identifier

# ---------------------------------------------------------------------------
# 1. expect_column_pair_values_a_to_be_greater_than_b
# ---------------------------------------------------------------------------


@register_expectation
@dataclass
class ExpectColumnPairValuesAToBeGreaterThanB(Expectation):
    """Expect values in column_a to be greater than values in column_b."""

    expectation_type: str = field(
        init=False,
        default="expect_column_pair_values_a_to_be_greater_than_b",
    )

    def _validate_pandas(self, df: pd.DataFrame) -> ExpectationResult:
        col_a = self.kwargs.get("column_a", self.column)
        col_b = self.kwargs.get("column_b")
        or_equal = self.kwargs.get("or_equal", False)

        if col_b is None:
            raise ValueError("column_b is required")

        valid = df[df[col_a].notna() & df[col_b].notna()]
        total = len(valid)

        if or_equal:
            unexpected_mask = valid[col_a] < valid[col_b]
        else:
            unexpected_mask = valid[col_a] <= valid[col_b]

        unexpected_count = int(unexpected_mask.sum())
        pct = (unexpected_count / total * 100) if total > 0 else 0.0

        return self._build_result(
            success=(unexpected_count == 0),
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            details={
                "column_a": col_a,
                "column_b": col_b,
                "or_equal": or_equal,
            },
        )

    def _validate_spark(self, df: Any) -> ExpectationResult:
        from pyspark.sql import functions as F

        raw_col_a = self.kwargs.get("column_a", self.column)
        raw_col_b = self.kwargs.get("column_b")
        or_equal = self.kwargs.get("or_equal", False)

        if raw_col_b is None:
            raise ValueError("column_b is required")

        col_a = str(raw_col_a)
        col_b = str(raw_col_b)

        valid = df.filter(F.col(col_a).isNotNull() & F.col(col_b).isNotNull())
        total = valid.count()

        if or_equal:
            unexpected_count = valid.filter(F.col(col_a) < F.col(col_b)).count()
        else:
            unexpected_count = valid.filter(F.col(col_a) <= F.col(col_b)).count()

        pct = (unexpected_count / total * 100) if total > 0 else 0.0

        return self._build_result(
            success=(unexpected_count == 0),
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            details={"column_a": col_a, "column_b": col_b, "or_equal": or_equal},
        )

    def _validate_polars(self, df: Any) -> ExpectationResult:
        import polars as pl

        raw_col_a = self.kwargs.get("column_a", self.column)
        raw_col_b = self.kwargs.get("column_b")
        or_equal = self.kwargs.get("or_equal", False)

        if raw_col_b is None:
            raise ValueError("column_b is required")

        col_a = str(raw_col_a)
        col_b = str(raw_col_b)

        valid = df.filter(pl.col(col_a).is_not_null() & pl.col(col_b).is_not_null())
        total = len(valid)

        if or_equal:
            unexpected = valid.filter(pl.col(col_a) < pl.col(col_b))
        else:
            unexpected = valid.filter(pl.col(col_a) <= pl.col(col_b))

        unexpected_count = len(unexpected)
        pct = (unexpected_count / total * 100) if total > 0 else 0.0

        return self._build_result(
            success=(unexpected_count == 0),
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            details={"column_a": col_a, "column_b": col_b, "or_equal": or_equal},
        )

    def _validate_sql(self, sql_source: Any) -> ExpectationResult:
        from sqlalchemy import text

        engine, query_or_table = sql_source
        col_a = self.kwargs.get("column_a", self.column)
        col_b = self.kwargs.get("column_b")
        or_equal = self.kwargs.get("or_equal", False)
        if col_b is None:
            raise ValueError("column_b is required")

        qa = quote_sql_identifier(col_a)
        qb = quote_sql_identifier(col_b)
        from_clause = format_sql_from_clause(query_or_table)
        op = "<" if or_equal else "<="
        query = (
            f"SELECT COUNT(*) AS total, "  # nosec B608
            f"SUM(CASE WHEN {qa} {op} {qb} THEN 1 ELSE 0 END) AS unexpected "
            f"FROM {from_clause} WHERE {qa} IS NOT NULL AND {qb} IS NOT NULL"
        )
        with engine.connect() as conn:
            row = conn.execute(text(query)).fetchone()
            total = int(row.total) if row and row.total else 0
            unexpected_count = int(row.unexpected) if row and row.unexpected else 0

        pct = (unexpected_count / total * 100) if total > 0 else 0.0
        return self._build_result(
            success=(unexpected_count == 0),
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            details={"column_a": col_a, "column_b": col_b, "or_equal": or_equal},
        )


# ---------------------------------------------------------------------------
# 2. expect_column_pair_values_to_be_equal
# ---------------------------------------------------------------------------


@register_expectation
@dataclass
class ExpectColumnPairValuesToBeEqual(Expectation):
    """Expect values in two columns to be equal row-wise."""

    expectation_type: str = field(init=False, default="expect_column_pair_values_to_be_equal")

    def _validate_pandas(self, df: pd.DataFrame) -> ExpectationResult:
        col_a = self.kwargs.get("column_a", self.column)
        col_b = self.kwargs.get("column_b")

        if col_b is None:
            raise ValueError("column_b is required")

        valid = df[df[col_a].notna() & df[col_b].notna()]
        total = len(valid)
        unexpected_mask = valid[col_a] != valid[col_b]
        unexpected_count = int(unexpected_mask.sum())
        pct = (unexpected_count / total * 100) if total > 0 else 0.0

        return self._build_result(
            success=(unexpected_count == 0),
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            details={"column_a": col_a, "column_b": col_b},
        )

    def _validate_spark(self, df: Any) -> ExpectationResult:
        from pyspark.sql import functions as F

        raw_col_a = self.kwargs.get("column_a", self.column)
        raw_col_b = self.kwargs.get("column_b")

        if raw_col_b is None:
            raise ValueError("column_b is required")

        col_a = str(raw_col_a)
        col_b = str(raw_col_b)

        valid = df.filter(F.col(col_a).isNotNull() & F.col(col_b).isNotNull())
        total = valid.count()
        unexpected_count = valid.filter(F.col(col_a) != F.col(col_b)).count()
        pct = (unexpected_count / total * 100) if total > 0 else 0.0

        return self._build_result(
            success=(unexpected_count == 0),
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            details={"column_a": col_a, "column_b": col_b},
        )

    def _validate_polars(self, df: Any) -> ExpectationResult:
        import polars as pl

        raw_col_a = self.kwargs.get("column_a", self.column)
        raw_col_b = self.kwargs.get("column_b")

        if raw_col_b is None:
            raise ValueError("column_b is required")

        col_a = str(raw_col_a)
        col_b = str(raw_col_b)

        valid = df.filter(pl.col(col_a).is_not_null() & pl.col(col_b).is_not_null())
        total = len(valid)
        unexpected = valid.filter(pl.col(col_a) != pl.col(col_b))
        unexpected_count = len(unexpected)
        pct = (unexpected_count / total * 100) if total > 0 else 0.0

        return self._build_result(
            success=(unexpected_count == 0),
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            details={"column_a": col_a, "column_b": col_b},
        )

    def _validate_sql(self, sql_source: Any) -> ExpectationResult:
        from sqlalchemy import text

        engine, query_or_table = sql_source
        col_a = self.kwargs.get("column_a", self.column)
        col_b = self.kwargs.get("column_b")
        if col_b is None:
            raise ValueError("column_b is required")

        qa = quote_sql_identifier(col_a)
        qb = quote_sql_identifier(col_b)
        from_clause = format_sql_from_clause(query_or_table)
        query = (
            f"SELECT COUNT(*) AS total, "  # nosec B608
            f"SUM(CASE WHEN {qa} != {qb} THEN 1 ELSE 0 END) AS unexpected "
            f"FROM {from_clause} WHERE {qa} IS NOT NULL AND {qb} IS NOT NULL"
        )
        with engine.connect() as conn:
            row = conn.execute(text(query)).fetchone()
            total = int(row.total) if row and row.total else 0
            unexpected_count = int(row.unexpected) if row and row.unexpected else 0

        pct = (unexpected_count / total * 100) if total > 0 else 0.0
        return self._build_result(
            success=(unexpected_count == 0),
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            details={"column_a": col_a, "column_b": col_b},
        )


# ---------------------------------------------------------------------------
# 3. expect_multicolumn_sum_to_equal
# ---------------------------------------------------------------------------


@register_expectation
@dataclass
class ExpectMulticolumnSumToEqual(Expectation):
    """Expect the sum across multiple columns (row-wise) to equal a target."""

    expectation_type: str = field(init=False, default="expect_multicolumn_sum_to_equal")

    def _validate_pandas(self, df: pd.DataFrame) -> ExpectationResult:
        columns = self.kwargs.get("column_list", [])
        target = self.kwargs.get("sum_total")

        if not columns:
            raise ValueError("column_list is required")

        row_sums = df[columns].sum(axis=1)
        total = len(row_sums)
        unexpected_mask = row_sums != target
        unexpected_count = int(unexpected_mask.sum())
        pct = (unexpected_count / total * 100) if total > 0 else 0.0

        return self._build_result(
            success=(unexpected_count == 0),
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            details={
                "column_list": columns,
                "expected_sum": target,
            },
        )

    def _validate_spark(self, df: Any) -> ExpectationResult:
        from functools import reduce
        from operator import add

        from pyspark.sql import functions as F

        columns = self.kwargs.get("column_list", [])
        target = self.kwargs.get("sum_total")

        if not columns:
            raise ValueError("column_list is required")

        total = df.count()
        sum_expr = reduce(add, [F.coalesce(F.col(str(c)), F.lit(0)) for c in columns])
        unexpected_count = df.filter(sum_expr != target).count()
        pct = (unexpected_count / total * 100) if total > 0 else 0.0

        return self._build_result(
            success=(unexpected_count == 0),
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            details={
                "column_list": columns,
                "expected_sum": target,
            },
        )

    def _validate_polars(self, df: Any) -> ExpectationResult:
        import polars as pl

        columns = self.kwargs.get("column_list", [])
        target = self.kwargs.get("sum_total")

        if not columns:
            raise ValueError("column_list is required")

        row_sums = df.select(pl.sum_horizontal([pl.col(c).fill_null(0) for c in columns])).to_series()
        total = len(df)
        unexpected_count = int((row_sums != target).sum())
        pct = (unexpected_count / total * 100) if total > 0 else 0.0

        return self._build_result(
            success=(unexpected_count == 0),
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            details={
                "column_list": columns,
                "expected_sum": target,
            },
        )

    def _validate_sql(self, sql_source: Any) -> ExpectationResult:
        from sqlalchemy import text

        engine, query_or_table = sql_source
        columns = self.kwargs.get("column_list", [])
        target = self.kwargs.get("sum_total")
        if not columns:
            raise ValueError("column_list is required")

        from_clause = format_sql_from_clause(query_or_table)
        sum_expr = " + ".join(f"COALESCE({quote_sql_identifier(c)}, 0)" for c in columns)
        query = (
            f"SELECT COUNT(*) AS total, "  # nosec B608
            f"SUM(CASE WHEN ({sum_expr}) != :target THEN 1 ELSE 0 END) AS unexpected "
            f"FROM {from_clause}"
        )
        with engine.connect() as conn:
            row = conn.execute(text(query), {"target": target}).fetchone()
            total = int(row.total) if row and row.total else 0
            unexpected_count = int(row.unexpected) if row and row.unexpected else 0

        pct = (unexpected_count / total * 100) if total > 0 else 0.0
        return self._build_result(
            success=(unexpected_count == 0),
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            details={
                "column_list": columns,
                "expected_sum": target,
            },
        )


# ---------------------------------------------------------------------------
# 4. expect_compound_columns_to_be_unique
# ---------------------------------------------------------------------------


@register_expectation
@dataclass
class ExpectCompoundColumnsToBeUnique(Expectation):
    """Expect the combination of values across multiple columns to be unique."""

    expectation_type: str = field(init=False, default="expect_compound_columns_to_be_unique")

    def _validate_pandas(self, df: pd.DataFrame) -> ExpectationResult:
        columns = self.kwargs.get("column_list", [])
        if not columns:
            raise ValueError("column_list is required")

        total = len(df)
        dup_mask = df.duplicated(subset=columns, keep=False)
        dup_count = int(dup_mask.sum())
        distinct_count = len(df[columns].drop_duplicates())
        pct = (dup_count / total * 100) if total > 0 else 0.0

        return self._build_result(
            success=(dup_count == 0),
            observed_value=f"{distinct_count} unique compound keys out of {total}",
            element_count=total,
            unexpected_count=dup_count,
            unexpected_percent=pct,
            details={"column_list": columns, "duplicate_count": dup_count, "distinct_count": distinct_count},
        )

    def _validate_spark(self, df: Any) -> ExpectationResult:
        from pyspark.sql import functions as F

        columns = self.kwargs.get("column_list", [])
        if not columns:
            raise ValueError("column_list is required")

        total = df.count()
        distinct_count = df.select(columns).distinct().count()
        dup_groups = df.groupBy(columns).count().filter(F.col("count") > 1).collect()
        dup_count = sum(int(r["count"]) for r in dup_groups)
        pct = (dup_count / total * 100) if total > 0 else 0.0

        return self._build_result(
            success=(dup_count == 0),
            observed_value=f"{distinct_count} unique compound keys out of {total}",
            element_count=total,
            unexpected_count=dup_count,
            unexpected_percent=pct,
            details={"column_list": columns, "duplicate_count": dup_count, "distinct_count": distinct_count},
        )

    def _validate_polars(self, df: Any) -> ExpectationResult:
        columns = self.kwargs.get("column_list", [])
        if not columns:
            raise ValueError("column_list is required")

        total = len(df)
        dup_mask = df.select(columns).is_duplicated()
        dup_count = int(dup_mask.sum())
        distinct_count = int(df.select(columns).n_unique())
        pct = (dup_count / total * 100) if total > 0 else 0.0

        return self._build_result(
            success=(dup_count == 0),
            observed_value=f"{distinct_count} unique compound keys out of {total}",
            element_count=total,
            unexpected_count=dup_count,
            unexpected_percent=pct,
            details={"column_list": columns, "duplicate_count": dup_count, "distinct_count": distinct_count},
        )

    def _validate_sql(self, sql_source: Any) -> ExpectationResult:
        from sqlalchemy import text

        engine, query_or_table = sql_source
        columns = self.kwargs.get("column_list", [])
        if not columns:
            raise ValueError("column_list is required")

        from_clause = format_sql_from_clause(query_or_table)
        quoted_cols = ", ".join(quote_sql_identifier(c) for c in columns)
        query_stats = (
            f"SELECT COALESCE(SUM(cnt), 0) AS total, "  # nosec B608
            f"COUNT(*) AS distinct_count, "
            f"COALESCE(SUM(CASE WHEN cnt > 1 THEN cnt ELSE 0 END), 0) AS dup_count "
            f"FROM (SELECT COUNT(*) AS cnt FROM {from_clause} GROUP BY {quoted_cols}) AS grp"
        )
        with engine.connect() as conn:
            row = conn.execute(text(query_stats)).fetchone()
            total = int(row.total) if row else 0
            distinct_count = int(row.distinct_count) if row else 0
            dup_count = int(row.dup_count) if row else 0

        pct = (dup_count / total * 100) if total > 0 else 0.0
        return self._build_result(
            success=(dup_count == 0),
            observed_value=f"{distinct_count} unique compound keys out of {total}",
            element_count=total,
            unexpected_count=dup_count,
            unexpected_percent=pct,
            details={"column_list": columns, "duplicate_count": dup_count, "distinct_count": distinct_count},
        )
