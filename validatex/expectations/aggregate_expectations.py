"""
Aggregate / cross-column expectations.

Expectations that involve relationships between multiple columns
or aggregated computations.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import pandas as pd

from validatex.core.expectation import Expectation, register_expectation
from validatex.core.result import ExpectationResult


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

        valid = df[[col_a, col_b]].dropna()
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

        col_a = self.kwargs.get("column_a", self.column)
        col_b = self.kwargs.get("column_b")
        or_equal = self.kwargs.get("or_equal", False)

        if col_b is None:
            raise ValueError("column_b is required")

        valid = df.filter(
            F.col(col_a).isNotNull() & F.col(col_b).isNotNull()
        )
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


# ---------------------------------------------------------------------------
# 2. expect_column_pair_values_to_be_equal
# ---------------------------------------------------------------------------

@register_expectation
@dataclass
class ExpectColumnPairValuesToBeEqual(Expectation):
    """Expect values in two columns to be equal row-wise."""

    expectation_type: str = field(
        init=False, default="expect_column_pair_values_to_be_equal"
    )

    def _validate_pandas(self, df: pd.DataFrame) -> ExpectationResult:
        col_a = self.kwargs.get("column_a", self.column)
        col_b = self.kwargs.get("column_b")

        if col_b is None:
            raise ValueError("column_b is required")

        valid = df[[col_a, col_b]].dropna()
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

        col_a = self.kwargs.get("column_a", self.column)
        col_b = self.kwargs.get("column_b")

        if col_b is None:
            raise ValueError("column_b is required")

        valid = df.filter(
            F.col(col_a).isNotNull() & F.col(col_b).isNotNull()
        )
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


# ---------------------------------------------------------------------------
# 3. expect_multicolumn_sum_to_equal
# ---------------------------------------------------------------------------

@register_expectation
@dataclass
class ExpectMulticolumnSumToEqual(Expectation):
    """Expect the sum across multiple columns (row-wise) to equal a target."""

    expectation_type: str = field(
        init=False, default="expect_multicolumn_sum_to_equal"
    )

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


# ---------------------------------------------------------------------------
# 4. expect_compound_columns_to_be_unique
# ---------------------------------------------------------------------------

@register_expectation
@dataclass
class ExpectCompoundColumnsToBeUnique(Expectation):
    """Expect the combination of values across multiple columns to be unique."""

    expectation_type: str = field(
        init=False, default="expect_compound_columns_to_be_unique"
    )

    def _validate_pandas(self, df: pd.DataFrame) -> ExpectationResult:
        columns = self.kwargs.get("column_list", [])
        if not columns:
            raise ValueError("column_list is required")

        total = len(df)
        dup_mask = df.duplicated(subset=columns, keep=False)
        dup_count = int(dup_mask.sum())
        pct = (dup_count / total * 100) if total > 0 else 0.0

        return self._build_result(
            success=(dup_count == 0),
            observed_value=f"{total - dup_count} unique compound keys out of {total}",
            element_count=total,
            unexpected_count=dup_count,
            unexpected_percent=pct,
            details={"column_list": columns, "duplicate_count": dup_count},
        )

    def _validate_spark(self, df: Any) -> ExpectationResult:
        from pyspark.sql import functions as F

        columns = self.kwargs.get("column_list", [])
        if not columns:
            raise ValueError("column_list is required")

        total = df.count()
        distinct_count = df.select(columns).distinct().count()
        dup_count = total - distinct_count
        pct = (dup_count / total * 100) if total > 0 else 0.0

        return self._build_result(
            success=(dup_count == 0),
            observed_value=f"{distinct_count} unique compound keys out of {total}",
            element_count=total,
            unexpected_count=dup_count,
            unexpected_percent=pct,
            details={"column_list": columns, "duplicate_count": dup_count},
        )
