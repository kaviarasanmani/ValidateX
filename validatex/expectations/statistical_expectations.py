"""
ValidateX - Numeric & Statistical Expectations.

Advanced quantile, IQR, correlation, null percentage and custom row-level
condition expectations for precision data profiling.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, List, Optional

import pandas as pd

from validatex.core.expectation import Expectation, register_expectation
from validatex.core.result import ExpectationResult
from validatex.core.utils import format_sql_from_clause, quote_sql_identifier

# ---------------------------------------------------------------------------
# 1. expect_column_quantile_values_to_be_between
# ---------------------------------------------------------------------------


@register_expectation
@dataclass
class ExpectColumnQuantileValuesToBeBetween(Expectation):
    """
    Expect column quantile values to be within given bounds.

    Example usage::

        expect_column_quantile_values_to_be_between(
            column="age",
            quantiles=[0.25, 0.50, 0.75],
            value_ranges=[[0, 30], [20, 50], [50, 80]]
        )
    """

    expectation_type: str = field(init=False, default="expect_column_quantile_values_to_be_between")

    def _validate_pandas(self, df: pd.DataFrame) -> ExpectationResult:
        quantiles: List[float] = self.kwargs.get("quantiles", [])
        value_ranges: List[List[Optional[float]]] = self.kwargs.get("value_ranges", [])

        if len(quantiles) != len(value_ranges):
            raise ValueError("'quantiles' and 'value_ranges' must have the same length.")

        series = df[self.column].dropna()
        total = len(series)
        if total == 0:
            return self._build_result(
                success=False,
                observed_value=None,
                element_count=0,
                details={"quantile_checks": [], "failed_quantiles": list(quantiles)},
            )

        computed = series.quantile(quantiles)

        failures = []
        rows = []
        for q, (lo, hi), actual in zip(quantiles, value_ranges, computed):
            ok = True
            if lo is not None and actual < lo:
                ok = False
            if hi is not None and actual > hi:
                ok = False
            if not ok:
                failures.append(q)
            rows.append(
                {
                    "quantile": q,
                    "value": round(float(actual), 6),
                    "min_value": lo,
                    "max_value": hi,
                    "success": ok,
                }
            )

        return self._build_result(
            success=len(failures) == 0,
            observed_value={str(r["quantile"]): r["value"] for r in rows},
            element_count=total,
            details={"quantile_checks": rows, "failed_quantiles": failures},
        )

    def _validate_spark(self, df: Any) -> ExpectationResult:
        from pyspark.sql import functions as F

        quantiles: List[float] = [float(q) for q in self.kwargs.get("quantiles", [])]
        value_ranges: List[List[Optional[float]]] = self.kwargs.get("value_ranges", [])

        if len(quantiles) != len(value_ranges):
            raise ValueError("'quantiles' and 'value_ranges' must have the same length.")

        col_name = str(self.column)
        filtered = df.filter(F.col(col_name).isNotNull())
        total = filtered.count()
        if total == 0:
            return self._build_result(
                success=False,
                observed_value=None,
                element_count=0,
                details={"quantile_checks": [], "failed_quantiles": list(quantiles)},
            )

        computed = filtered.approxQuantile(col_name, quantiles, 0.0001)

        failures = []
        rows = []
        for q, (lo, hi), actual in zip(quantiles, value_ranges, computed):
            ok = True
            if lo is not None and actual < lo:
                ok = False
            if hi is not None and actual > hi:
                ok = False
            if not ok:
                failures.append(q)
            rows.append(
                {
                    "quantile": q,
                    "value": round(float(actual), 6),
                    "min_value": lo,
                    "max_value": hi,
                    "success": ok,
                }
            )

        return self._build_result(
            success=len(failures) == 0,
            observed_value={str(r["quantile"]): r["value"] for r in rows},
            element_count=total,
            details={"quantile_checks": rows, "failed_quantiles": failures},
        )

    def _validate_polars(self, df: Any) -> ExpectationResult:
        quantiles: List[float] = self.kwargs.get("quantiles", [])
        value_ranges: List[List[Optional[float]]] = self.kwargs.get("value_ranges", [])

        if len(quantiles) != len(value_ranges):
            raise ValueError("'quantiles' and 'value_ranges' must have the same length.")

        series = df[self.column].drop_nulls()
        total = len(series)
        if total == 0:
            return self._build_result(
                success=False,
                observed_value=None,
                element_count=0,
                details={"quantile_checks": [], "failed_quantiles": list(quantiles)},
            )

        computed = [float(series.quantile(q)) for q in quantiles]

        failures = []
        rows = []
        for q, (lo, hi), actual in zip(quantiles, value_ranges, computed):
            ok = True
            if lo is not None and actual < lo:
                ok = False
            if hi is not None and actual > hi:
                ok = False
            if not ok:
                failures.append(q)
            rows.append(
                {
                    "quantile": q,
                    "value": round(float(actual), 6),
                    "min_value": lo,
                    "max_value": hi,
                    "success": ok,
                }
            )

        return self._build_result(
            success=len(failures) == 0,
            observed_value={str(r["quantile"]): r["value"] for r in rows},
            element_count=total,
            details={"quantile_checks": rows, "failed_quantiles": failures},
        )


# ---------------------------------------------------------------------------
# 2. expect_column_null_percentage_to_be_less_than
# ---------------------------------------------------------------------------


@register_expectation
@dataclass
class ExpectColumnNullPercentageToBeLessThan(Expectation):
    """Expect null percentage in a column to be below a threshold (0-100)."""

    expectation_type: str = field(init=False, default="expect_column_null_percentage_to_be_less_than")

    def _validate_pandas(self, df: pd.DataFrame) -> ExpectationResult:
        threshold: float = float(self.kwargs.get("threshold", self.kwargs.get("max_pct", 0)))
        total = len(df)
        null_count = int(df[self.column].isnull().sum())
        pct = (null_count / total * 100) if total > 0 else 0.0
        success = pct < threshold

        return self._build_result(
            success=success,
            observed_value=round(pct, 4),
            element_count=total,
            details={"null_count": null_count, "null_percent": pct, "threshold_percent": threshold},
        )

    def _validate_spark(self, df: Any) -> ExpectationResult:
        from pyspark.sql import functions as F

        threshold: float = float(self.kwargs.get("threshold", self.kwargs.get("max_pct", 0)))
        col = F.col(str(self.column))
        total = df.count()
        null_count = df.filter(col.isNull()).count()
        pct = (null_count / total * 100) if total > 0 else 0.0

        return self._build_result(
            success=(pct < threshold),
            observed_value=round(pct, 4),
            element_count=total,
            details={"null_count": null_count, "null_percent": pct, "threshold_percent": threshold},
        )

    def _validate_polars(self, df: Any) -> ExpectationResult:
        threshold: float = float(self.kwargs.get("threshold", self.kwargs.get("max_pct", 0)))
        total = len(df)
        null_count = int(df[self.column].is_null().sum())
        pct = (null_count / total * 100) if total > 0 else 0.0

        return self._build_result(
            success=(pct < threshold),
            observed_value=round(pct, 4),
            element_count=total,
            details={"null_count": null_count, "null_percent": pct, "threshold_percent": threshold},
        )

    def _validate_sql(self, sql_source: Any) -> ExpectationResult:
        from sqlalchemy import text

        engine, query_or_table = sql_source
        threshold: float = float(self.kwargs.get("threshold", self.kwargs.get("max_pct", 0)))
        col = quote_sql_identifier(self.column)
        from_clause = format_sql_from_clause(query_or_table)
        query = (
            f"SELECT COUNT(*) AS total, "  # nosec B608
            f"COALESCE(SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END), 0) AS null_count "
            f"FROM {from_clause}"
        )
        with engine.connect() as conn:
            row = conn.execute(text(query)).fetchone()
            total = int(row.total) if row and row.total else 0
            null_count = int(row.null_count) if row and row.null_count else 0

        pct = (null_count / total * 100) if total > 0 else 0.0
        return self._build_result(
            success=(pct < threshold),
            observed_value=round(pct, 4),
            element_count=total,
            details={"null_count": null_count, "null_percent": pct, "threshold_percent": threshold},
        )


# ---------------------------------------------------------------------------
# 3. expect_column_correlation_to_be_between
# ---------------------------------------------------------------------------


@register_expectation
@dataclass
class ExpectColumnCorrelationToBeBetween(Expectation):
    """
    Expect the Pearson correlation between this column and another
    to be within [min_value, max_value].

    kwargs:
        other_column (str): The column to correlate against.
        min_value (float): Minimum acceptable correlation (-1 to 1).
        max_value (float): Maximum acceptable correlation (-1 to 1).
    """

    expectation_type: str = field(init=False, default="expect_column_correlation_to_be_between")

    def _validate_pandas(self, df: pd.DataFrame) -> ExpectationResult:
        other_col = self.kwargs.get("other_column")
        min_val = self.kwargs.get("min_value", -1.0)
        max_val = self.kwargs.get("max_value", 1.0)

        pair = df[[self.column, other_col]].dropna()
        total = len(pair)
        if total < 2:
            return self._build_result(
                success=False,
                observed_value=None,
                element_count=total,
                details={"corr_with": other_col, "min_value": min_val, "max_value": max_val},
            )

        raw_corr = pair[self.column].corr(pair[other_col])
        if pd.isna(raw_corr):
            return self._build_result(
                success=False,
                observed_value=None,
                element_count=total,
                details={"corr_with": other_col, "min_value": min_val, "max_value": max_val},
            )

        actual_corr = float(raw_corr)
        success = min_val <= actual_corr <= max_val

        return self._build_result(
            success=success,
            observed_value=round(actual_corr, 6),
            element_count=total,
            details={"corr_with": other_col, "min_value": min_val, "max_value": max_val},
        )

    def _validate_spark(self, df: Any) -> ExpectationResult:
        from pyspark.sql import functions as F

        other_col = self.kwargs.get("other_column")
        min_val = self.kwargs.get("min_value", -1.0)
        max_val = self.kwargs.get("max_value", 1.0)

        total = df.filter(F.col(str(self.column)).isNotNull() & F.col(str(other_col)).isNotNull()).count()
        if total < 2:
            return self._build_result(
                success=False,
                observed_value=None,
                element_count=total,
                details={"corr_with": other_col, "min_value": min_val, "max_value": max_val},
            )

        raw_corr = df.stat.corr(str(self.column), str(other_col))
        if raw_corr is None or pd.isna(raw_corr):
            return self._build_result(
                success=False,
                observed_value=None,
                element_count=total,
                details={"corr_with": other_col, "min_value": min_val, "max_value": max_val},
            )

        actual_corr = float(raw_corr)
        success = min_val <= actual_corr <= max_val

        return self._build_result(
            success=success,
            observed_value=round(actual_corr, 6),
            element_count=total,
            details={"corr_with": other_col, "min_value": min_val, "max_value": max_val},
        )

    def _validate_polars(self, df: Any) -> ExpectationResult:
        import polars as pl

        other_col = self.kwargs.get("other_column")
        min_val = self.kwargs.get("min_value", -1.0)
        max_val = self.kwargs.get("max_value", 1.0)

        pair = df.filter(pl.col(self.column).is_not_null() & pl.col(other_col).is_not_null())
        total = len(pair)
        if total < 2:
            return self._build_result(
                success=False,
                observed_value=None,
                element_count=total,
                details={"corr_with": other_col, "min_value": min_val, "max_value": max_val},
            )

        raw_corr = pair.select(pl.corr(self.column, other_col)).to_series()[0]
        if raw_corr is None or pd.isna(raw_corr):
            return self._build_result(
                success=False,
                observed_value=None,
                element_count=total,
                details={"corr_with": other_col, "min_value": min_val, "max_value": max_val},
            )

        actual_corr = float(raw_corr)
        success = min_val <= actual_corr <= max_val

        return self._build_result(
            success=success,
            observed_value=round(actual_corr, 6),
            element_count=total,
            details={"corr_with": other_col, "min_value": min_val, "max_value": max_val},
        )


# ---------------------------------------------------------------------------
# 4. expect_column_values_to_have_no_whitespace
# ---------------------------------------------------------------------------


@register_expectation
@dataclass
class ExpectColumnValuesToHaveNoWhitespace(Expectation):
    """Expect string values to have no leading or trailing whitespace."""

    expectation_type: str = field(init=False, default="expect_column_values_to_have_no_whitespace")

    def _validate_pandas(self, df: pd.DataFrame) -> ExpectationResult:
        series = df[self.column].dropna().astype(str)
        total = len(series)
        unexpected_mask = series != series.str.strip()
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

        unexpected_df = filtered.filter(col != F.trim(col))
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

    def _validate_polars(self, df: Any) -> ExpectationResult:
        import polars as pl

        series = df[self.column].drop_nulls().cast(pl.String)
        total = len(series)
        unexpected_mask = series != series.str.strip_chars()
        unexpected_count = int(unexpected_mask.sum())
        pct = (unexpected_count / total * 100) if total > 0 else 0.0

        return self._build_result(
            success=(unexpected_count == 0),
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            unexpected_values=series.filter(unexpected_mask).to_list()[:20],
        )

    def _validate_sql(self, sql_source: Any) -> ExpectationResult:
        from sqlalchemy import text

        engine, query_or_table = sql_source
        col = quote_sql_identifier(self.column)
        from_clause = format_sql_from_clause(query_or_table)
        query = (
            f"SELECT COUNT({col}) AS total, "  # nosec B608
            f"COALESCE(SUM(CASE WHEN {col} IS NOT NULL AND CAST({col} AS VARCHAR) != TRIM(CAST({col} AS VARCHAR)) "
            f"THEN 1 ELSE 0 END), 0) AS unexpected "
            f"FROM {from_clause}"
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
        )


# ---------------------------------------------------------------------------
# 5. expect_column_values_to_be_positive
# ---------------------------------------------------------------------------


@register_expectation
@dataclass
class ExpectColumnValuesToBePositive(Expectation):
    """Expect all non-null numeric values in the column to be strictly positive (> 0)."""

    expectation_type: str = field(init=False, default="expect_column_values_to_be_positive")

    def _validate_pandas(self, df: pd.DataFrame) -> ExpectationResult:
        allow_zero = self.kwargs.get("allow_zero", False)
        series = df[self.column].dropna()
        total = len(series)

        unexpected_mask = series < 0 if allow_zero else series <= 0
        unexpected_count = int(unexpected_mask.sum())
        pct = (unexpected_count / total * 100) if total > 0 else 0.0

        return self._build_result(
            success=(unexpected_count == 0),
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            unexpected_values=series[unexpected_mask].tolist()[:20],
            details={"allow_zero": allow_zero},
        )

    def _validate_spark(self, df: Any) -> ExpectationResult:
        from pyspark.sql import functions as F

        allow_zero = self.kwargs.get("allow_zero", False)
        col = F.col(str(self.column))
        filtered = df.filter(col.isNotNull())
        total = filtered.count()

        unexpected_df = filtered.filter(col < 0 if allow_zero else col <= 0)
        unexpected_count = unexpected_df.count()
        pct = (unexpected_count / total * 100) if total > 0 else 0.0
        unexpected_vals = [row[0] for row in unexpected_df.select(self.column).limit(20).collect()]

        return self._build_result(
            success=(unexpected_count == 0),
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            unexpected_values=unexpected_vals,
            details={"allow_zero": allow_zero},
        )

    def _validate_polars(self, df: Any) -> ExpectationResult:
        allow_zero = self.kwargs.get("allow_zero", False)
        series = df[self.column].drop_nulls()
        total = len(series)

        unexpected_mask = series < 0 if allow_zero else series <= 0
        unexpected_count = int(unexpected_mask.sum())
        pct = (unexpected_count / total * 100) if total > 0 else 0.0

        return self._build_result(
            success=(unexpected_count == 0),
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            unexpected_values=series.filter(unexpected_mask).to_list()[:20],
            details={"allow_zero": allow_zero},
        )

    def _validate_sql(self, sql_source: Any) -> ExpectationResult:
        from sqlalchemy import text

        engine, query_or_table = sql_source
        allow_zero = self.kwargs.get("allow_zero", False)
        col = quote_sql_identifier(self.column)
        from_clause = format_sql_from_clause(query_or_table)
        op = "<" if allow_zero else "<="
        query = (
            f"SELECT COUNT({col}) AS total, "  # nosec B608
            f"COALESCE(SUM(CASE WHEN {col} IS NOT NULL AND {col} {op} 0 THEN 1 ELSE 0 END), 0) AS unexpected "
            f"FROM {from_clause}"
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
            details={"allow_zero": allow_zero},
        )


# ---------------------------------------------------------------------------
# 6. expect_column_values_to_be_negative
# ---------------------------------------------------------------------------


@register_expectation
@dataclass
class ExpectColumnValuesToBeNegative(Expectation):
    """Expect all non-null numeric values in the column to be strictly negative (< 0)."""

    expectation_type: str = field(init=False, default="expect_column_values_to_be_negative")

    def _validate_pandas(self, df: pd.DataFrame) -> ExpectationResult:
        allow_zero = self.kwargs.get("allow_zero", False)
        series = df[self.column].dropna()
        total = len(series)

        unexpected_mask = series > 0 if allow_zero else series >= 0
        unexpected_count = int(unexpected_mask.sum())
        pct = (unexpected_count / total * 100) if total > 0 else 0.0

        return self._build_result(
            success=(unexpected_count == 0),
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            unexpected_values=series[unexpected_mask].tolist()[:20],
            details={"allow_zero": allow_zero},
        )

    def _validate_spark(self, df: Any) -> ExpectationResult:
        from pyspark.sql import functions as F

        allow_zero = self.kwargs.get("allow_zero", False)
        col = F.col(str(self.column))
        filtered = df.filter(col.isNotNull())
        total = filtered.count()

        unexpected_df = filtered.filter(col > 0 if allow_zero else col >= 0)
        unexpected_count = unexpected_df.count()
        pct = (unexpected_count / total * 100) if total > 0 else 0.0
        unexpected_vals = [row[0] for row in unexpected_df.select(self.column).limit(20).collect()]

        return self._build_result(
            success=(unexpected_count == 0),
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            unexpected_values=unexpected_vals,
            details={"allow_zero": allow_zero},
        )

    def _validate_polars(self, df: Any) -> ExpectationResult:
        allow_zero = self.kwargs.get("allow_zero", False)
        series = df[self.column].drop_nulls()
        total = len(series)

        unexpected_mask = series > 0 if allow_zero else series >= 0
        unexpected_count = int(unexpected_mask.sum())
        pct = (unexpected_count / total * 100) if total > 0 else 0.0

        return self._build_result(
            success=(unexpected_count == 0),
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            unexpected_values=series.filter(unexpected_mask).to_list()[:20],
            details={"allow_zero": allow_zero},
        )

    def _validate_sql(self, sql_source: Any) -> ExpectationResult:
        from sqlalchemy import text

        engine, query_or_table = sql_source
        allow_zero = self.kwargs.get("allow_zero", False)
        col = quote_sql_identifier(self.column)
        from_clause = format_sql_from_clause(query_or_table)
        op = ">" if allow_zero else ">="
        query = (
            f"SELECT COUNT({col}) AS total, "  # nosec B608
            f"COALESCE(SUM(CASE WHEN {col} IS NOT NULL AND {col} {op} 0 THEN 1 ELSE 0 END), 0) AS unexpected "
            f"FROM {from_clause}"
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
            details={"allow_zero": allow_zero},
        )


# ---------------------------------------------------------------------------
# 7. expect_column_values_to_be_in_range_of_std_devs  (Z-score outlier detector)
# ---------------------------------------------------------------------------


@register_expectation
@dataclass
class ExpectColumnValuesToBeInRangeOfStdDevs(Expectation):
    """
    Expect column values to fall within N standard deviations of the column mean.
    This is a powerful outlier / anomaly detection expectation.

    kwargs:
        n_std_devs (float): Max number of standard deviations from mean (default 3.0).
    """

    expectation_type: str = field(init=False, default="expect_column_values_to_be_in_range_of_std_devs")

    def _validate_pandas(self, df: pd.DataFrame) -> ExpectationResult:
        n_std = float(self.kwargs.get("n_std_devs", 3.0))
        series = df[self.column].dropna()
        total = len(series)

        if total < 2:
            mean_val = round(float(series.mean()), 4) if total == 1 else None
            return self._build_result(
                success=True,
                observed_value={"mean": mean_val, "std": None},
                element_count=total,
                unexpected_count=0,
                unexpected_percent=0.0,
                unexpected_values=[],
                details={"n_std_devs": n_std, "lower_bound": None, "upper_bound": None},
            )

        mean = float(series.mean())
        std = float(series.std())
        lower = mean - n_std * std
        upper = mean + n_std * std

        unexpected_mask = (series < lower) | (series > upper)
        unexpected_count = int(unexpected_mask.sum())
        pct = (unexpected_count / total * 100) if total > 0 else 0.0

        return self._build_result(
            success=(unexpected_count == 0),
            observed_value={"mean": round(float(mean), 4), "std": round(float(std), 4)},
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            unexpected_values=series[unexpected_mask].tolist()[:20],
            details={
                "n_std_devs": n_std,
                "lower_bound": round(float(lower), 4),
                "upper_bound": round(float(upper), 4),
            },
        )

    def _validate_spark(self, df: Any) -> ExpectationResult:
        from pyspark.sql import functions as F

        n_std = float(self.kwargs.get("n_std_devs", 3.0))
        col_name = str(self.column)
        col = F.col(col_name)
        filtered = df.filter(col.isNotNull())
        total = filtered.count()

        if total < 2:
            mean_raw = filtered.select(F.mean(col)).first()[0] if total == 1 else None
            mean_val = round(float(mean_raw), 4) if mean_raw is not None else None
            return self._build_result(
                success=True,
                observed_value={"mean": mean_val, "std": None},
                element_count=total,
                unexpected_count=0,
                unexpected_percent=0.0,
                unexpected_values=[],
                details={"n_std_devs": n_std, "lower_bound": None, "upper_bound": None},
            )

        stats = filtered.select(F.mean(col).alias("mean"), F.stddev(col).alias("std")).collect()[0]

        mean = float(stats["mean"])
        std = float(stats["std"])
        lower = mean - n_std * std
        upper = mean + n_std * std

        unexpected_df = filtered.filter((col < lower) | (col > upper))
        unexpected_count = unexpected_df.count()
        pct = (unexpected_count / total * 100) if total > 0 else 0.0
        unexpected_vals = [row[0] for row in unexpected_df.select(self.column).limit(20).collect()]

        return self._build_result(
            success=(unexpected_count == 0),
            observed_value={"mean": round(mean, 4), "std": round(std, 4)},
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            unexpected_values=unexpected_vals,
            details={"n_std_devs": n_std, "lower_bound": round(lower, 4), "upper_bound": round(upper, 4)},
        )

    def _validate_polars(self, df: Any) -> ExpectationResult:
        n_std = float(self.kwargs.get("n_std_devs", 3.0))
        series = df[self.column].drop_nulls()
        total = len(series)

        if total < 2:
            mean_val = round(float(series.mean()), 4) if total == 1 else None
            return self._build_result(
                success=True,
                observed_value={"mean": mean_val, "std": None},
                element_count=total,
                unexpected_count=0,
                unexpected_percent=0.0,
                unexpected_values=[],
                details={"n_std_devs": n_std, "lower_bound": None, "upper_bound": None},
            )

        mean = float(series.mean())
        std = float(series.std())
        lower = mean - n_std * std
        upper = mean + n_std * std

        unexpected_mask = (series < lower) | (series > upper)
        unexpected_count = int(unexpected_mask.sum())
        pct = (unexpected_count / total * 100) if total > 0 else 0.0

        return self._build_result(
            success=(unexpected_count == 0),
            observed_value={"mean": round(float(mean), 4), "std": round(float(std), 4)},
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            unexpected_values=series.filter(unexpected_mask).to_list()[:20],
            details={
                "n_std_devs": n_std,
                "lower_bound": round(float(lower), 4),
                "upper_bound": round(float(upper), 4),
            },
        )
