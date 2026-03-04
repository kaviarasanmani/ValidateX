"""
Column-level expectations.

Each class is a self-contained expectation that validates properties
of individual columns in a DataFrame.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from validatex.core.expectation import Expectation, register_expectation
from validatex.core.result import ExpectationResult

# ---------------------------------------------------------------------------
# 1. expect_column_to_exist
# ---------------------------------------------------------------------------


@register_expectation
@dataclass
class ExpectColumnToExist(Expectation):
    """Expect a column to exist in the DataFrame."""

    expectation_type: str = field(init=False, default="expect_column_to_exist")

    def _validate_pandas(self, df: pd.DataFrame) -> ExpectationResult:
        exists = self.column in df.columns
        return self._build_result(
            success=exists,
            observed_value=list(df.columns),
            details={"column_exists": exists},
        )

    def _validate_spark(self, df: Any) -> ExpectationResult:
        exists = self.column in df.columns
        return self._build_result(
            success=exists,
            details={"column_exists": exists},
        )

    def _validate_sql(self, sql_source: Any) -> ExpectationResult:
        from sqlalchemy import text

        engine, query_or_table = sql_source
        
        # Fast query to just get column headers
        query = f"SELECT * FROM ({query_or_table}) AS subquery LIMIT 1"
        with engine.connect() as conn:
            result = conn.execute(text(query))
            exists = str(self.column) in result.keys()
                
        return self._build_result(
            success=exists,
            details={"column_exists": exists},
        )


# ---------------------------------------------------------------------------
# 2. expect_column_to_not_be_null
# ---------------------------------------------------------------------------


@register_expectation
@dataclass
class ExpectColumnToNotBeNull(Expectation):
    """Expect a column to contain no null values."""

    expectation_type: str = field(init=False, default="expect_column_to_not_be_null")

    def _validate_pandas(self, df: pd.DataFrame) -> ExpectationResult:
        null_count = int(df[self.column].isnull().sum())
        total = len(df)
        pct = (null_count / total * 100) if total > 0 else 0.0
        return self._build_result(
            success=(null_count == 0),
            observed_value=null_count,
            element_count=total,
            unexpected_count=null_count,
            unexpected_percent=pct,
            details={"null_count": null_count, "total_count": total},
        )

    def _validate_spark(self, df: Any) -> ExpectationResult:
        from pyspark.sql import functions as F

        total = df.count()
        null_count = df.filter(F.col(str(self.column)).isNull()).count()
        pct = (null_count / total * 100) if total > 0 else 0.0
        return self._build_result(
            success=(null_count == 0),
            observed_value=null_count,
            element_count=total,
            unexpected_count=null_count,
            unexpected_percent=pct,
            details={"null_count": null_count, "total_count": total},
        )

    def _validate_sql(self, sql_source: Any) -> ExpectationResult:
        from sqlalchemy import text
        engine, query_or_table = sql_source
        
        col = str(self.column)
        query = f"SELECT COUNT(*) as total, SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) as nulls FROM ({query_or_table}) AS subquery"
        with engine.connect() as conn:
            row = conn.execute(text(query)).fetchone()
            total = int(row.total) if row and row.total else 0
            null_count = int(row.nulls) if row and row.nulls else 0
            
        pct = (null_count / total * 100) if total > 0 else 0.0
        return self._build_result(
            success=(null_count == 0),
            observed_value=null_count,
            element_count=total,
            unexpected_count=null_count,
            unexpected_percent=pct,
            details={"null_count": null_count, "total_count": total},
        )


# ---------------------------------------------------------------------------
# 3. expect_column_values_to_be_unique
# ---------------------------------------------------------------------------


@register_expectation
@dataclass
class ExpectColumnValuesToBeUnique(Expectation):
    """Expect all values in a column to be unique (no duplicates)."""

    expectation_type: str = field(
        init=False, default="expect_column_values_to_be_unique"
    )

    def _validate_pandas(self, df: pd.DataFrame) -> ExpectationResult:
        total = len(df)
        dup_mask = df[self.column].duplicated(keep=False)
        dup_count = int(dup_mask.sum())
        pct = (dup_count / total * 100) if total > 0 else 0.0
        dup_values = df.loc[dup_mask, self.column].unique().tolist()[:20]
        return self._build_result(
            success=(dup_count == 0),
            observed_value=f"{total - dup_count} unique out of {total}",
            element_count=total,
            unexpected_count=dup_count,
            unexpected_percent=pct,
            unexpected_values=dup_values,
            unexpected_percent=pct,
            details={"duplicate_count": dup_count},
        )

    def _validate_sql(self, sql_source: Any) -> ExpectationResult:
        from sqlalchemy import text
        engine, query_or_table = sql_source
        
        col = str(self.column)
        query = f"SELECT COUNT({col}) as total, COUNT(DISTINCT {col}) as distinct_count FROM ({query_or_table}) AS subquery"
        with engine.connect() as conn:
            row = conn.execute(text(query)).fetchone()
            total = int(row.total) if row and row.total else 0
            distinct = int(row.distinct_count) if row and row.distinct_count else 0
            
        dup_count = total - distinct
        pct = (dup_count / total * 100) if total > 0 else 0.0

        return self._build_result(
            success=(dup_count == 0),
            observed_value=f"{distinct} unique values out of {total}",
            element_count=total,
            unexpected_count=dup_count,
            unexpected_percent=pct,
            details={"duplicate_count": dup_count},
        )

    def _validate_spark(self, df: Any) -> ExpectationResult:
        total = df.count()
        distinct_count = df.select(self.column).distinct().count()
        dup_count = total - distinct_count
        pct = (dup_count / total * 100) if total > 0 else 0.0
        return self._build_result(
            success=(dup_count == 0),
            observed_value=f"{distinct_count} unique out of {total}",
            element_count=total,
            unexpected_count=dup_count,
            unexpected_percent=pct,
            details={"duplicate_count": dup_count, "distinct_count": distinct_count},
        )


# ---------------------------------------------------------------------------
# 4. expect_column_values_to_be_between
# ---------------------------------------------------------------------------


@register_expectation
@dataclass
class ExpectColumnValuesToBeBetween(Expectation):
    """Expect column values to fall within [min_value, max_value]."""

    expectation_type: str = field(
        init=False, default="expect_column_values_to_be_between"
    )

    def _validate_pandas(self, df: pd.DataFrame) -> ExpectationResult:
        min_val = self.kwargs.get("min_value")
        max_val = self.kwargs.get("max_value")
        strict_min = self.kwargs.get("strict_min", False)
        strict_max = self.kwargs.get("strict_max", False)

        series = df[self.column].dropna()
        total = len(series)

        if strict_min:
            mask_low = (
                series <= min_val
                if min_val is not None
                else pd.Series(False, index=series.index)
            )
        else:
            mask_low = (
                series < min_val
                if min_val is not None
                else pd.Series(False, index=series.index)
            )

        if strict_max:
            mask_high = (
                series >= max_val
                if max_val is not None
                else pd.Series(False, index=series.index)
            )
        else:
            mask_high = (
                series > max_val
                if max_val is not None
                else pd.Series(False, index=series.index)
            )

        unexpected_mask = mask_low | mask_high
        unexpected_count = int(unexpected_mask.sum())
        pct = (unexpected_count / total * 100) if total > 0 else 0.0
        unexpected_vals = series[unexpected_mask].tolist()[:20]

        return self._build_result(
            success=(unexpected_count == 0),
            observed_value={
                "min": series.min() if total > 0 else None,
                "max": series.max() if total > 0 else None,
            },
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            unexpected_values=unexpected_vals,
            details={
                "min_value": min_val,
                "max_value": max_val,
                "strict_min": strict_min,
                "strict_max": strict_max,
            },
        )

    def _validate_spark(self, df: Any) -> ExpectationResult:
        from pyspark.sql import functions as F

        min_val = self.kwargs.get("min_value")
        max_val = self.kwargs.get("max_value")
        strict_min = self.kwargs.get("strict_min", False)
        strict_max = self.kwargs.get("strict_max", False)

        col = F.col(str(self.column))
        filtered = df.filter(col.isNotNull())
        total = filtered.count()

        conditions = []
        if min_val is not None:
            conditions.append(col <= min_val if strict_min else col < min_val)
        if max_val is not None:
            conditions.append(col >= max_val if strict_max else col > max_val)

        if conditions:
            from functools import reduce
            import operator

            combined = reduce(operator.__or__, conditions)
            unexpected_count = filtered.filter(combined).count()
        else:
            unexpected_count = 0

        pct = (unexpected_count / total * 100) if total > 0 else 0.0
        stats = filtered.select(
            F.min(str(self.column)), F.max(str(self.column))
        ).first()

        return self._build_result(
            success=(unexpected_count == 0),
            observed_value={"min": stats[0], "max": stats[1]},
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            details={"min_value": min_val, "max_value": max_val},
        )


# ---------------------------------------------------------------------------
# 5. expect_column_values_to_be_in_set
# ---------------------------------------------------------------------------


@register_expectation
@dataclass
class ExpectColumnValuesToBeInSet(Expectation):
    """Expect every value in a column to be a member of a given set."""

    expectation_type: str = field(
        init=False, default="expect_column_values_to_be_in_set"
    )

    def _validate_pandas(self, df: pd.DataFrame) -> ExpectationResult:
        value_set = set(self.kwargs.get("value_set", []))
        series = df[self.column].dropna()
        total = len(series)
        unexpected_mask = ~series.isin(value_set)
        unexpected_count = int(unexpected_mask.sum())
        pct = (unexpected_count / total * 100) if total > 0 else 0.0
        unexpected_vals = series[unexpected_mask].unique().tolist()[:20]

        return self._build_result(
            success=(unexpected_count == 0),
            observed_value={"unique_values": series.nunique()},
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            unexpected_values=unexpected_vals,
            details={"value_set": list(value_set)},
        )

    def _validate_spark(self, df: Any) -> ExpectationResult:
        from pyspark.sql import functions as F

        value_set = list(self.kwargs.get("value_set", []))
        col = F.col(str(self.column))
        filtered = df.filter(col.isNotNull())
        total = filtered.count()
        unexpected_count = filtered.filter(~col.isin(value_set)).count()
        pct = (unexpected_count / total * 100) if total > 0 else 0.0

        return self._build_result(
            success=(unexpected_count == 0),
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            details={"value_set": value_set},
        )


# ---------------------------------------------------------------------------
# 6. expect_column_values_to_not_be_in_set
# ---------------------------------------------------------------------------


@register_expectation
@dataclass
class ExpectColumnValuesToNotBeInSet(Expectation):
    """Expect no value in a column to be a member of the given set."""

    expectation_type: str = field(
        init=False, default="expect_column_values_to_not_be_in_set"
    )

    def _validate_pandas(self, df: pd.DataFrame) -> ExpectationResult:
        forbidden = set(self.kwargs.get("value_set", []))
        series = df[self.column].dropna()
        total = len(series)
        unexpected_mask = series.isin(forbidden)
        unexpected_count = int(unexpected_mask.sum())
        pct = (unexpected_count / total * 100) if total > 0 else 0.0
        unexpected_vals = series[unexpected_mask].unique().tolist()[:20]

        return self._build_result(
            success=(unexpected_count == 0),
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            unexpected_values=unexpected_vals,
            details={"forbidden_set": list(forbidden)},
        )

    def _validate_spark(self, df: Any) -> ExpectationResult:
        from pyspark.sql import functions as F

        forbidden = list(self.kwargs.get("value_set", []))
        col = F.col(str(self.column))
        filtered = df.filter(col.isNotNull())
        total = filtered.count()
        unexpected_count = filtered.filter(col.isin(forbidden)).count()
        pct = (unexpected_count / total * 100) if total > 0 else 0.0

        return self._build_result(
            success=(unexpected_count == 0),
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            details={"forbidden_set": forbidden},
        )


# ---------------------------------------------------------------------------
# 7. expect_column_values_to_match_regex
# ---------------------------------------------------------------------------


@register_expectation
@dataclass
class ExpectColumnValuesToMatchRegex(Expectation):
    """Expect column values to match a given regular expression."""

    expectation_type: str = field(
        init=False, default="expect_column_values_to_match_regex"
    )

    def _validate_pandas(self, df: pd.DataFrame) -> ExpectationResult:
        regex = self.kwargs.get("regex", ".*")
        series = df[self.column].dropna().astype(str)
        total = len(series)
        pattern = re.compile(regex)
        match_mask = series.apply(lambda x: bool(pattern.search(x)))
        unexpected_count = int((~match_mask).sum())
        pct = (unexpected_count / total * 100) if total > 0 else 0.0
        unexpected_vals = series[~match_mask].tolist()[:20]

        return self._build_result(
            success=(unexpected_count == 0),
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            unexpected_values=unexpected_vals,
            details={"regex": regex},
        )

    def _validate_spark(self, df: Any) -> ExpectationResult:
        from pyspark.sql import functions as F

        regex = self.kwargs.get("regex", ".*")
        col = F.col(str(self.column))
        filtered = df.filter(col.isNotNull())
        total = filtered.count()
        unexpected_count = filtered.filter(~col.cast("string").rlike(regex)).count()
        pct = (unexpected_count / total * 100) if total > 0 else 0.0

        return self._build_result(
            success=(unexpected_count == 0),
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            details={"regex": regex},
        )


# ---------------------------------------------------------------------------
# 8. expect_column_values_to_be_of_type
# ---------------------------------------------------------------------------


@register_expectation
@dataclass
class ExpectColumnValuesToBeOfType(Expectation):
    """Expect a column's dtype to match the expected type string."""

    expectation_type: str = field(
        init=False, default="expect_column_values_to_be_of_type"
    )

    def _validate_pandas(self, df: pd.DataFrame) -> ExpectationResult:
        expected_type = self.kwargs.get("expected_type", "")
        actual_type = str(df[self.column].dtype)
        success = expected_type.lower() in actual_type.lower()
        return self._build_result(
            success=success,
            observed_value=actual_type,
            details={"expected_type": expected_type, "actual_type": actual_type},
        )

    def _validate_spark(self, df: Any) -> ExpectationResult:
        expected_type = self.kwargs.get("expected_type", "")
        actual_type = str(df.schema[self.column].dataType)
        success = expected_type.lower() in actual_type.lower()
        return self._build_result(
            success=success,
            observed_value=actual_type,
            details={"expected_type": expected_type, "actual_type": actual_type},
        )


# ---------------------------------------------------------------------------
# 9. expect_column_values_to_be_dateutil_parseable
# ---------------------------------------------------------------------------


@register_expectation
@dataclass
class ExpectColumnValuesToBeDateutilParseable(Expectation):
    """Expect column values to be parseable as dates."""

    expectation_type: str = field(
        init=False, default="expect_column_values_to_be_dateutil_parseable"
    )

    def _validate_pandas(self, df: pd.DataFrame) -> ExpectationResult:
        series = df[self.column].dropna()
        total = len(series)
        converted = pd.to_datetime(series, errors="coerce")
        null_after = int(converted.isnull().sum())
        pct = (null_after / total * 100) if total > 0 else 0.0
        bad_vals = series[converted.isnull()].tolist()[:20]

        return self._build_result(
            success=(null_after == 0),
            element_count=total,
            unexpected_count=null_after,
            unexpected_percent=pct,
            unexpected_values=bad_vals,
            details={"unparseable_count": null_after},
        )


# ---------------------------------------------------------------------------
# 10. expect_column_value_lengths_to_be_between
# ---------------------------------------------------------------------------


@register_expectation
@dataclass
class ExpectColumnValueLengthsToBeBetween(Expectation):
    """Expect string lengths in a column to be within [min_value, max_value]."""

    expectation_type: str = field(
        init=False, default="expect_column_value_lengths_to_be_between"
    )

    def _validate_pandas(self, df: pd.DataFrame) -> ExpectationResult:
        min_len = self.kwargs.get("min_value", 0)
        max_len = self.kwargs.get("max_value", float("inf"))
        series = df[self.column].dropna().astype(str)
        total = len(series)
        lengths = series.str.len()
        unexpected_mask = (lengths < min_len) | (lengths > max_len)
        unexpected_count = int(unexpected_mask.sum())
        pct = (unexpected_count / total * 100) if total > 0 else 0.0
        unexpected_vals = series[unexpected_mask].tolist()[:20]

        return self._build_result(
            success=(unexpected_count == 0),
            observed_value={
                "min_length": int(lengths.min()) if total > 0 else None,
                "max_length": int(lengths.max()) if total > 0 else None,
            },
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            unexpected_values=unexpected_vals,
            details={"min_value": min_len, "max_value": max_len},
        )

    def _validate_spark(self, df: Any) -> ExpectationResult:
        from pyspark.sql import functions as F

        min_len = self.kwargs.get("min_value", 0)
        max_len = self.kwargs.get("max_value", float("inf"))
        col = F.col(str(self.column))
        filtered = df.filter(col.isNotNull())
        total = filtered.count()
        length_col = F.length(col.cast("string"))
        unexpected_count = filtered.filter(
            (length_col < min_len) | (length_col > max_len)
        ).count()
        pct = (unexpected_count / total * 100) if total > 0 else 0.0

        return self._build_result(
            success=(unexpected_count == 0),
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            details={"min_value": min_len, "max_value": max_len},
        )


# ---------------------------------------------------------------------------
# 11. expect_column_max_to_be_between
# ---------------------------------------------------------------------------


@register_expectation
@dataclass
class ExpectColumnMaxToBeBetween(Expectation):
    """Expect the maximum value of a column to be between min_value and max_value."""

    expectation_type: str = field(init=False, default="expect_column_max_to_be_between")

    def _validate_pandas(self, df: pd.DataFrame) -> ExpectationResult:
        min_val = self.kwargs.get("min_value")
        max_val = self.kwargs.get("max_value")
        col_max = df[self.column].max()

        success = True
        if min_val is not None and col_max < min_val:
            success = False
        if max_val is not None and col_max > max_val:
            success = False

        return self._build_result(
            success=success,
            observed_value=col_max,
            details={"min_value": min_val, "max_value": max_val, "column_max": col_max},
        )

    def _validate_spark(self, df: Any) -> ExpectationResult:
        from pyspark.sql import functions as F

        min_val = self.kwargs.get("min_value")
        max_val = self.kwargs.get("max_value")
        col_max = df.agg(F.max(str(self.column))).first()[0]

        success = True
        if min_val is not None and col_max < min_val:
            success = False
        if max_val is not None and col_max > max_val:
            success = False

        return self._build_result(
            success=success,
            observed_value=col_max,
            details={"min_value": min_val, "max_value": max_val, "column_max": col_max},
        )


# ---------------------------------------------------------------------------
# 12. expect_column_min_to_be_between
# ---------------------------------------------------------------------------


@register_expectation
@dataclass
class ExpectColumnMinToBeBetween(Expectation):
    """Expect the minimum value of a column to be between min_value and max_value."""

    expectation_type: str = field(init=False, default="expect_column_min_to_be_between")

    def _validate_pandas(self, df: pd.DataFrame) -> ExpectationResult:
        min_val = self.kwargs.get("min_value")
        max_val = self.kwargs.get("max_value")
        col_min = df[self.column].min()

        success = True
        if min_val is not None and col_min < min_val:
            success = False
        if max_val is not None and col_min > max_val:
            success = False

        return self._build_result(
            success=success,
            observed_value=col_min,
            details={"min_value": min_val, "max_value": max_val, "column_min": col_min},
        )

    def _validate_spark(self, df: Any) -> ExpectationResult:
        from pyspark.sql import functions as F

        min_val = self.kwargs.get("min_value")
        max_val = self.kwargs.get("max_value")
        col_min = df.agg(F.min(str(self.column))).first()[0]

        success = True
        if min_val is not None and col_min < min_val:
            success = False
        if max_val is not None and col_min > max_val:
            success = False

        return self._build_result(
            success=success,
            observed_value=col_min,
            details={"min_value": min_val, "max_value": max_val, "column_min": col_min},
        )


# ---------------------------------------------------------------------------
# 13. expect_column_mean_to_be_between
# ---------------------------------------------------------------------------


@register_expectation
@dataclass
class ExpectColumnMeanToBeBetween(Expectation):
    """Expect the mean value of a numeric column to fall within bounds."""

    expectation_type: str = field(
        init=False, default="expect_column_mean_to_be_between"
    )

    def _validate_pandas(self, df: pd.DataFrame) -> ExpectationResult:
        min_val = self.kwargs.get("min_value")
        max_val = self.kwargs.get("max_value")
        col_mean = float(df[self.column].mean())

        success = True
        if min_val is not None and col_mean < min_val:
            success = False
        if max_val is not None and col_mean > max_val:
            success = False

        return self._build_result(
            success=success,
            observed_value=round(col_mean, 4),
            details={
                "min_value": min_val,
                "max_value": max_val,
                "column_mean": round(col_mean, 4),
            },
        )

    def _validate_spark(self, df: Any) -> ExpectationResult:
        from pyspark.sql import functions as F

        min_val = self.kwargs.get("min_value")
        max_val = self.kwargs.get("max_value")
        col_mean = df.agg(F.mean(str(self.column))).first()[0]

        success = True
        if min_val is not None and col_mean < min_val:
            success = False
        if max_val is not None and col_mean > max_val:
            success = False

        return self._build_result(
            success=success,
            observed_value=round(col_mean, 4) if col_mean else None,
            details={"min_value": min_val, "max_value": max_val},
        )


# ---------------------------------------------------------------------------
# 14. expect_column_stdev_to_be_between
# ---------------------------------------------------------------------------


@register_expectation
@dataclass
class ExpectColumnStdevToBeBetween(Expectation):
    """Expect the standard deviation of a column to fall within bounds."""

    expectation_type: str = field(
        init=False, default="expect_column_stdev_to_be_between"
    )

    def _validate_pandas(self, df: pd.DataFrame) -> ExpectationResult:
        min_val = self.kwargs.get("min_value")
        max_val = self.kwargs.get("max_value")
        col_std = float(df[self.column].std())

        success = True
        if min_val is not None and col_std < min_val:
            success = False
        if max_val is not None and col_std > max_val:
            success = False

        return self._build_result(
            success=success,
            observed_value=round(col_std, 4),
            details={
                "min_value": min_val,
                "max_value": max_val,
                "column_stdev": round(col_std, 4),
            },
        )


# ---------------------------------------------------------------------------
# 15. expect_column_distinct_values_to_be_in_set
# ---------------------------------------------------------------------------


@register_expectation
@dataclass
class ExpectColumnDistinctValuesToBeInSet(Expectation):
    """Expect all distinct values in a column to be in the given set."""

    expectation_type: str = field(
        init=False, default="expect_column_distinct_values_to_be_in_set"
    )

    def _validate_pandas(self, df: pd.DataFrame) -> ExpectationResult:
        value_set = set(self.kwargs.get("value_set", []))
        actual_values = set(df[self.column].dropna().unique().tolist())
        unexpected = actual_values - value_set
        total_distinct = len(actual_values)

        return self._build_result(
            success=(len(unexpected) == 0),
            observed_value={"distinct_values": list(actual_values)[:20]},
            element_count=total_distinct,
            unexpected_count=len(unexpected),
            unexpected_percent=(
                (len(unexpected) / total_distinct * 100) if total_distinct > 0 else 0.0
            ),
            unexpected_values=list(unexpected)[:20],
            details={"value_set": list(value_set)},
        )

    def _validate_spark(self, df: Any) -> ExpectationResult:
        pass

        value_set = set(self.kwargs.get("value_set", []))
        row_list = df.select(self.column).distinct().collect()
        actual_values = {row[0] for row in row_list if row[0] is not None}
        unexpected = actual_values - value_set

        return self._build_result(
            success=(len(unexpected) == 0),
            observed_value={"distinct_values": list(actual_values)[:20]},
            unexpected_count=len(unexpected),
            unexpected_values=list(unexpected)[:20],
            details={"value_set": list(value_set)},
        )


# ---------------------------------------------------------------------------
# 16. expect_column_proportion_of_unique_values_to_be_between
# ---------------------------------------------------------------------------


@register_expectation
@dataclass
class ExpectColumnProportionOfUniqueValuesToBeBetween(Expectation):
    """Expect the proportion of unique values in a column to fall within bounds."""

    expectation_type: str = field(
        init=False,
        default="expect_column_proportion_of_unique_values_to_be_between",
    )

    def _validate_pandas(self, df: pd.DataFrame) -> ExpectationResult:
        min_val = self.kwargs.get("min_value", 0.0)
        max_val = self.kwargs.get("max_value", 1.0)
        series = df[self.column].dropna()
        total = len(series)
        unique_count = series.nunique()
        proportion = (unique_count / total) if total > 0 else 0.0

        success = min_val <= proportion <= max_val

        return self._build_result(
            success=success,
            observed_value=round(proportion, 4),
            element_count=total,
            details={
                "unique_count": unique_count,
                "total_count": total,
                "proportion": round(proportion, 4),
                "min_value": min_val,
                "max_value": max_val,
            },
        )
