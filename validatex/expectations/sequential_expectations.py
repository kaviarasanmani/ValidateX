"""
Sequential and Time-Series Expectations.

These expectations allow validation of sorted, ordered, or time-series data,
making them perfect for validating event streams and log append mechanisms.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from validatex.core.expectation import Expectation, register_expectation
from validatex.core.result import ExpectationResult

# ---------------------------------------------------------------------------
# 1. expect_column_values_to_be_increasing
# ---------------------------------------------------------------------------


@register_expectation
@dataclass
class ExpectColumnValuesToBeIncreasing(Expectation):
    """Expect column values (often timestamps or IDs) to be monotonically increasing."""

    expectation_type: str = field(init=False, default="expect_column_values_to_be_increasing")

    def _validate_pandas(self, df: pd.DataFrame) -> ExpectationResult:
        strictly = self.kwargs.get("strictly", False)

        series = df[self.column].dropna()
        total = len(series)

        if total < 2:
            return self._build_result(success=True, element_count=total)

        # Calculate differences between consecutive rows to find out-of-order data
        diffs = series.diff().iloc[1:]

        if strictly:
            unexpected_mask = diffs <= 0
        else:
            unexpected_mask = diffs < 0

        unexpected_count = int(unexpected_mask.sum())
        pct = (unexpected_count / total * 100) if total > 0 else 0.0

        # Get the row indices where the drop occurred
        violation_indices = unexpected_mask[unexpected_mask].index
        unexpected_values = series.loc[violation_indices].tolist()[:10]

        return self._build_result(
            success=(unexpected_count == 0),
            observed_value=f"{total - unexpected_count} ordered elements",
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            unexpected_values=unexpected_values,
            details={"strictly_increasing": strictly},
        )

    def _validate_spark(self, df: Any) -> ExpectationResult:
        from pyspark.sql import Window
        from pyspark.sql import functions as F

        strictly = self.kwargs.get("strictly", False)
        col_name = str(self.column)
        filtered = df.filter(F.col(col_name).isNotNull())
        total = filtered.count()

        if total < 2:
            return self._build_result(success=True, element_count=total)

        w = Window.orderBy(F.monotonically_increasing_id())
        with_prev = filtered.withColumn("_prev_val", F.lag(F.col(col_name)).over(w)).filter(
            F.col("_prev_val").isNotNull()
        )
        if strictly:
            unexpected_df = with_prev.filter(F.col(col_name) <= F.col("_prev_val"))
        else:
            unexpected_df = with_prev.filter(F.col(col_name) < F.col("_prev_val"))

        unexpected_count = unexpected_df.count()
        pct = (unexpected_count / total * 100) if total > 0 else 0.0
        unexpected_values = [r[0] for r in unexpected_df.select(col_name).limit(10).collect()]

        return self._build_result(
            success=(unexpected_count == 0),
            observed_value=f"{total - unexpected_count} ordered elements",
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            unexpected_values=unexpected_values,
            details={"strictly_increasing": strictly},
        )

    def _validate_polars(self, df: Any) -> ExpectationResult:
        strictly = self.kwargs.get("strictly", False)
        series = df[self.column].drop_nulls()
        total = len(series)

        if total < 2:
            return self._build_result(success=True, element_count=total)

        diffs = series.diff()
        if strictly:
            unexpected_mask = (diffs <= 0).fill_null(False)
        else:
            unexpected_mask = (diffs < 0).fill_null(False)

        unexpected_count = int(unexpected_mask.sum())
        pct = (unexpected_count / total * 100) if total > 0 else 0.0
        unexpected_values = series.filter(unexpected_mask).to_list()[:10]

        return self._build_result(
            success=(unexpected_count == 0),
            observed_value=f"{total - unexpected_count} ordered elements",
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            unexpected_values=unexpected_values,
            details={"strictly_increasing": strictly},
        )


# ---------------------------------------------------------------------------
# 2. expect_column_values_to_be_decreasing
# ---------------------------------------------------------------------------


@register_expectation
@dataclass
class ExpectColumnValuesToBeDecreasing(Expectation):
    """Expect column values to be monotonically decreasing."""

    expectation_type: str = field(init=False, default="expect_column_values_to_be_decreasing")

    def _validate_pandas(self, df: pd.DataFrame) -> ExpectationResult:
        strictly = self.kwargs.get("strictly", False)

        series = df[self.column].dropna()
        total = len(series)

        if total < 2:
            return self._build_result(success=True, element_count=total)

        diffs = series.diff().iloc[1:]

        if strictly:
            unexpected_mask = diffs >= 0
        else:
            unexpected_mask = diffs > 0

        unexpected_count = int(unexpected_mask.sum())
        pct = (unexpected_count / total * 100) if total > 0 else 0.0

        violation_indices = unexpected_mask[unexpected_mask].index
        unexpected_values = series.loc[violation_indices].tolist()[:10]

        return self._build_result(
            success=(unexpected_count == 0),
            observed_value=f"{total - unexpected_count} ordered elements",
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            unexpected_values=unexpected_values,
            details={"strictly_decreasing": strictly},
        )

    def _validate_spark(self, df: Any) -> ExpectationResult:
        from pyspark.sql import Window
        from pyspark.sql import functions as F

        strictly = self.kwargs.get("strictly", False)
        col_name = str(self.column)
        filtered = df.filter(F.col(col_name).isNotNull())
        total = filtered.count()

        if total < 2:
            return self._build_result(success=True, element_count=total)

        w = Window.orderBy(F.monotonically_increasing_id())
        with_prev = filtered.withColumn("_prev_val", F.lag(F.col(col_name)).over(w)).filter(
            F.col("_prev_val").isNotNull()
        )
        if strictly:
            unexpected_df = with_prev.filter(F.col(col_name) >= F.col("_prev_val"))
        else:
            unexpected_df = with_prev.filter(F.col(col_name) > F.col("_prev_val"))

        unexpected_count = unexpected_df.count()
        pct = (unexpected_count / total * 100) if total > 0 else 0.0
        unexpected_values = [r[0] for r in unexpected_df.select(col_name).limit(10).collect()]

        return self._build_result(
            success=(unexpected_count == 0),
            observed_value=f"{total - unexpected_count} ordered elements",
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            unexpected_values=unexpected_values,
            details={"strictly_decreasing": strictly},
        )

    def _validate_polars(self, df: Any) -> ExpectationResult:
        strictly = self.kwargs.get("strictly", False)
        series = df[self.column].drop_nulls()
        total = len(series)

        if total < 2:
            return self._build_result(success=True, element_count=total)

        diffs = series.diff()
        if strictly:
            unexpected_mask = (diffs >= 0).fill_null(False)
        else:
            unexpected_mask = (diffs > 0).fill_null(False)

        unexpected_count = int(unexpected_mask.sum())
        pct = (unexpected_count / total * 100) if total > 0 else 0.0
        unexpected_values = series.filter(unexpected_mask).to_list()[:10]

        return self._build_result(
            success=(unexpected_count == 0),
            observed_value=f"{total - unexpected_count} ordered elements",
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            unexpected_values=unexpected_values,
            details={"strictly_decreasing": strictly},
        )
