"""
Advanced Column-level expectations for ValidateX.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from validatex.core.expectation import Expectation, register_expectation
from validatex.core.result import ExpectationResult


# ---------------------------------------------------------------------------
# 1. expect_column_values_to_not_match_regex
# ---------------------------------------------------------------------------

@register_expectation
@dataclass
class ExpectColumnValuesToNotMatchRegex(Expectation):
    """Expect column values to NOT match a given regex."""
    expectation_type: str = field(
        init=False, default="expect_column_values_to_not_match_regex"
    )

    def _validate_pandas(self, df: pd.DataFrame) -> ExpectationResult:
        regex = self.kwargs.get("regex", "")
        series = df[self.column].dropna().astype(str)
        total = len(series)

        pattern = re.compile(regex)
        
        # We want values that DO NOT match. So values that MATCH are "unexpected"
        unexpected_mask = series.apply(lambda x: bool(pattern.search(x)))
        unexpected_count = int(unexpected_mask.sum())
        pct = (unexpected_count / total * 100) if total > 0 else 0.0

        return self._build_result(
            success=(unexpected_count == 0),
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            unexpected_values=series[unexpected_mask].tolist()[:20],
            details={"regex": regex},
        )

    def _validate_spark(self, df: Any) -> ExpectationResult:
        from pyspark.sql import functions as F
        regex = self.kwargs.get("regex", "")
        col = F.col(str(self.column))
        
        filtered = df.filter(col.isNotNull())
        total = filtered.count()
        
        # Matches regex natively in Spark
        unexpected_df = filtered.filter(col.rlike(regex))
        unexpected_count = unexpected_df.count()
        pct = (unexpected_count / total * 100) if total > 0 else 0.0
        
        unexpected_vals = [row[0] for row in unexpected_df.select(self.column).limit(20).collect()]

        return self._build_result(
            success=(unexpected_count == 0),
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            unexpected_values=unexpected_vals,
            details={"regex": regex},
        )


# ---------------------------------------------------------------------------
# 2. expect_column_values_to_be_valid_email
# ---------------------------------------------------------------------------

@register_expectation
@dataclass
class ExpectColumnValuesToBeValidEmail(Expectation):
    """Expect column values to be valid email addresses."""
    expectation_type: str = field(
        init=False, default="expect_column_values_to_be_valid_email"
    )

    def _validate_pandas(self, df: pd.DataFrame) -> ExpectationResult:
        series = df[self.column].dropna().astype(str)
        total = len(series)

        # Basic functional email regex
        pattern = re.compile(r"^[\w\.\+\-]+@[a-zA-Z0-9\-]+\.[a-zA-Z0-9\-\.]+$")
        
        unexpected_mask = ~series.apply(lambda x: bool(pattern.match(x)))
        unexpected_count = int(unexpected_mask.sum())
        pct = (unexpected_count / total * 100) if total > 0 else 0.0

        return self._build_result(
            success=(unexpected_count == 0),
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            unexpected_values=series[unexpected_mask].tolist()[:20],
        )

    def _validate_spark(self, df: Any) -> ExpectationResult:
        from pyspark.sql import functions as F
        col = F.col(str(self.column))
        
        filtered = df.filter(col.isNotNull())
        total = filtered.count()
        
        # Equivalent regex logic for PySpark
        pattern = r"^[\w\.\+\-]+@[a-zA-Z0-9\-]+\.[a-zA-Z0-9\-\.]+$"
        unexpected_df = filtered.filter(~col.rlike(pattern))
        
        unexpected_count = unexpected_df.count()
        pct = (unexpected_count / total * 100) if total > 0 else 0.0
        unexpected_vals = [row[0] for row in unexpected_df.select(self.column).limit(20).collect()]

        return self._build_result(
            success=(unexpected_count == 0),
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            unexpected_values=unexpected_vals,
        )


# ---------------------------------------------------------------------------
# 3. expect_column_values_to_be_json_parseable
# ---------------------------------------------------------------------------

@register_expectation
@dataclass
class ExpectColumnValuesToBeJsonParseable(Expectation):
    """Expect column values to be valid, parseable JSON strings."""
    expectation_type: str = field(
        init=False, default="expect_column_values_to_be_json_parseable"
    )

    def _validate_pandas(self, df: pd.DataFrame) -> ExpectationResult:
        series = df[self.column].dropna().astype(str)
        total = len(series)

        def is_json(val: str) -> bool:
            try:
                json.loads(val)
                return True
            except (ValueError, TypeError):
                return False

        expected_mask = series.apply(is_json)
        unexpected_mask = ~expected_mask
        unexpected_count = int(unexpected_mask.sum())
        pct = (unexpected_count / total * 100) if total > 0 else 0.0

        return self._build_result(
            success=(unexpected_count == 0),
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            unexpected_values=series[unexpected_mask].tolist()[:20],
        )

    def _validate_spark(self, df: Any) -> ExpectationResult:
        from pyspark.sql import functions as F
        from pyspark.sql.types import StringType
        
        col = F.col(str(self.column))
        filtered = df.filter(col.isNotNull())
        total = filtered.count()

        # UDF to evaluate JSON safely in Spark
        @F.udf(returnType="boolean")
        def check_json(val):
            try:
                import json
                json.loads(val)
                return True
            except:
                return False

        # Apply UDF
        unexpected_df = filtered.filter(~check_json(col))
        unexpected_count = unexpected_df.count()
        pct = (unexpected_count / total * 100) if total > 0 else 0.0
        unexpected_vals = [row[0] for row in unexpected_df.select(self.column).limit(20).collect()]

        return self._build_result(
            success=(unexpected_count == 0),
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            unexpected_values=unexpected_vals,
        )


# ---------------------------------------------------------------------------
# 4. expect_column_sum_to_be_between
# ---------------------------------------------------------------------------

@register_expectation
@dataclass
class ExpectColumnSumToBeBetween(Expectation):
    """Expect the sum of a column to be between a min and max value."""
    expectation_type: str = field(
        init=False, default="expect_column_sum_to_be_between"
    )

    def _validate_pandas(self, df: pd.DataFrame) -> ExpectationResult:
        min_val = self.kwargs.get("min_value")
        max_val = self.kwargs.get("max_value")
        
        series = df[self.column].dropna()
        actual_sum = float(series.sum())
        
        success = True
        if min_val is not None:
            success = success and (actual_sum >= min_val)
        if max_val is not None:
            success = success and (actual_sum <= max_val)

        return self._build_result(
            success=success,
            observed_value=actual_sum,
            element_count=len(series),
            details={"min_value": min_val, "max_value": max_val, "actual_sum": actual_sum},
        )

    def _validate_spark(self, df: Any) -> ExpectationResult:
        from pyspark.sql import functions as F
        
        min_val = self.kwargs.get("min_value")
        max_val = self.kwargs.get("max_value")
        
        filtered = df.filter(F.col(str(self.column)).isNotNull())
        total_rows = filtered.count()
        
        if total_rows == 0:
            return self._build_result(success=True, element_count=0)

        row = filtered.select(F.sum(str(self.column)).alias("total_sum")).collect()[0]
        actual_sum = float(row["total_sum"]) if row["total_sum"] is not None else 0.0
        
        success = True
        if min_val is not None:
            success = success and (actual_sum >= min_val)
        if max_val is not None:
            success = success and (actual_sum <= max_val)

        return self._build_result(
            success=success,
            observed_value=actual_sum,
            element_count=total_rows,
            details={"min_value": min_val, "max_value": max_val, "actual_sum": actual_sum},
        )


# ---------------------------------------------------------------------------
# 5. expect_column_median_to_be_between
# ---------------------------------------------------------------------------

@register_expectation
@dataclass
class ExpectColumnMedianToBeBetween(Expectation):
    """Expect the median of a column to be between a min and max value."""
    expectation_type: str = field(
        init=False, default="expect_column_median_to_be_between"
    )

    def _validate_pandas(self, df: pd.DataFrame) -> ExpectationResult:
        min_val = self.kwargs.get("min_value")
        max_val = self.kwargs.get("max_value")
        
        series = df[self.column].dropna()
        actual_median = float(series.median())
        
        success = True
        if min_val is not None:
            success = success and (actual_median >= min_val)
        if max_val is not None:
            success = success and (actual_median <= max_val)

        return self._build_result(
            success=success,
            observed_value=actual_median,
            element_count=len(series),
            details={"min_value": min_val, "max_value": max_val, "actual_median": actual_median},
        )

    def _validate_spark(self, df: Any) -> ExpectationResult:
        from pyspark.sql import functions as F
        
        min_val = self.kwargs.get("min_value")
        max_val = self.kwargs.get("max_value")
        
        filtered = df.filter(F.col(str(self.column)).isNotNull())
        total_rows = filtered.count()
        
        if total_rows == 0:
            return self._build_result(success=True, element_count=0)

        # Approximate median using approxQuantile in PySpark
        actual_median = filtered.approxQuantile(str(self.column), [0.5], 0.001)[0]
        actual_median = float(actual_median)
        
        success = True
        if min_val is not None:
            success = success and (actual_median >= min_val)
        if max_val is not None:
            success = success and (actual_median <= max_val)

        return self._build_result(
            success=success,
            observed_value=actual_median,
            element_count=total_rows,
            details={"min_value": min_val, "max_value": max_val, "actual_median": actual_median},
        )


# ---------------------------------------------------------------------------
# 6. expect_column_value_lengths_to_equal
# ---------------------------------------------------------------------------

@register_expectation
@dataclass
class ExpectColumnValueLengthsToEqual(Expectation):
    """Expect string column value lengths to exactly equal a specific length."""
    expectation_type: str = field(
        init=False, default="expect_column_value_lengths_to_equal"
    )

    def _validate_pandas(self, df: pd.DataFrame) -> ExpectationResult:
        value = self.kwargs.get("value")
        if value is None:
            raise ValueError("Missing 'value' argument")
            
        series = df[self.column].dropna().astype(str)
        total = len(series)
        
        lengths = series.apply(len)
        unexpected_mask = lengths != value
        unexpected_count = int(unexpected_mask.sum())
        pct = (unexpected_count / total * 100) if total > 0 else 0.0

        return self._build_result(
            success=(unexpected_count == 0),
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            unexpected_values=series[unexpected_mask].tolist()[:20],
            details={"expected_length": value},
        )

    def _validate_spark(self, df: Any) -> ExpectationResult:
        from pyspark.sql import functions as F
        
        value = self.kwargs.get("value")
        if value is None:
            raise ValueError("Missing 'value' argument")
            
        col = F.col(str(self.column))
        filtered = df.filter(col.isNotNull())
        total = filtered.count()
        
        # Length check
        unexpected_df = filtered.filter(F.length(col.cast("string")) != value)
        
        unexpected_count = unexpected_df.count()
        pct = (unexpected_count / total * 100) if total > 0 else 0.0
        unexpected_vals = [row[0] for row in unexpected_df.select(self.column).limit(20).collect()]

        return self._build_result(
            success=(unexpected_count == 0),
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            unexpected_values=unexpected_vals,
            details={"expected_length": value},
        )
