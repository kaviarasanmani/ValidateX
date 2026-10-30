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
from validatex.core.utils import format_sql_from_clause, quote_sql_identifier

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
            observed_value=list(df.columns),
            details={"column_exists": exists},
        )

    def _validate_polars(self, df: Any) -> ExpectationResult:
        exists = self.column in df.columns
        return self._build_result(
            success=exists,
            observed_value=list(df.columns),
            details={"column_exists": exists},
        )

    def _validate_sql(self, sql_source: Any) -> ExpectationResult:
        from sqlalchemy import text

        engine, query_or_table = sql_source
        from_clause = format_sql_from_clause(query_or_table)

        # Fast query to just get column headers
        query = f"SELECT * FROM {from_clause} LIMIT 1"  # nosec B608
        with engine.connect() as conn:
            result = conn.execute(text(query))
            cols = list(result.keys())
            exists = str(self.column) in cols

        return self._build_result(
            success=exists,
            observed_value=cols,
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

    def _validate_polars(self, df: Any) -> ExpectationResult:
        col = str(self.column)
        total = len(df)
        null_count = int(df[col].is_null().sum())
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
        col = quote_sql_identifier(self.column)
        from_clause = format_sql_from_clause(query_or_table)
        query = (
            f"SELECT COUNT(*) as total, "  # nosec B608
            f"SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) as nulls "
            f"FROM {from_clause}"
        )
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

    expectation_type: str = field(init=False, default="expect_column_values_to_be_unique")

    def _validate_pandas(self, df: pd.DataFrame) -> ExpectationResult:
        total = len(df)
        dup_mask = df[self.column].duplicated(keep=False)
        dup_count = int(dup_mask.sum())
        distinct_count = int(df[self.column].nunique(dropna=False))
        pct = (dup_count / total * 100) if total > 0 else 0.0
        dup_values = df.loc[dup_mask, self.column].unique().tolist()[:20]
        return self._build_result(
            success=(dup_count == 0),
            observed_value=f"{distinct_count} unique values out of {total}",
            element_count=total,
            unexpected_count=dup_count,
            unexpected_percent=pct,
            unexpected_values=dup_values,
            details={"duplicate_count": dup_count, "distinct_count": distinct_count},
        )

    def _validate_sql(self, sql_source: Any) -> ExpectationResult:
        from sqlalchemy import text

        engine, query_or_table = sql_source
        col = quote_sql_identifier(self.column)
        from_clause = format_sql_from_clause(query_or_table)
        query = (
            f"SELECT COALESCE(SUM(cnt), 0) AS total, "  # nosec B608
            f"COUNT(*) AS distinct_count, "
            f"COALESCE(SUM(CASE WHEN cnt > 1 THEN cnt ELSE 0 END), 0) AS dup_count "
            f"FROM (SELECT {col} AS val, COUNT(*) AS cnt FROM {from_clause} GROUP BY {col}) AS grp"
        )
        dup_query = (
            f"SELECT {col} AS val, COUNT(*) AS cnt "  # nosec B608
            f"FROM {from_clause} "
            f"GROUP BY {col} HAVING COUNT(*) > 1"
        )
        with engine.connect() as conn:
            row = conn.execute(text(query)).fetchone()
            total = int(row.total) if row and row.total else 0
            distinct = int(row.distinct_count) if row and row.distinct_count else 0
            dup_count = int(row.dup_count) if row and row.dup_count else 0
            dup_rows = conn.execute(text(dup_query)).fetchall()

        dup_values = [r.val for r in dup_rows[:20]] if dup_rows else []
        pct = (dup_count / total * 100) if total > 0 else 0.0

        return self._build_result(
            success=(dup_count == 0),
            observed_value=f"{distinct} unique values out of {total}",
            element_count=total,
            unexpected_count=dup_count,
            unexpected_percent=pct,
            unexpected_values=dup_values,
            details={"duplicate_count": dup_count, "distinct_count": distinct},
        )

    def _validate_spark(self, df: Any) -> ExpectationResult:
        from pyspark.sql import functions as F

        col_name = str(self.column)
        total = df.count()
        distinct_count = df.select(col_name).distinct().count()
        dup_groups = df.groupBy(col_name).count().filter(F.col("count") > 1)
        dup_rows = dup_groups.collect()
        dup_count = sum(int(r["count"]) for r in dup_rows)
        dup_values = [r[col_name] for r in dup_rows[:20]]
        pct = (dup_count / total * 100) if total > 0 else 0.0
        return self._build_result(
            success=(dup_count == 0),
            observed_value=f"{distinct_count} unique values out of {total}",
            element_count=total,
            unexpected_count=dup_count,
            unexpected_percent=pct,
            unexpected_values=dup_values,
            details={"duplicate_count": dup_count, "distinct_count": distinct_count},
        )

    def _validate_polars(self, df: Any) -> ExpectationResult:
        import polars as pl

        col = str(self.column)
        total = len(df)
        dup_count = int(df[col].is_duplicated().sum())
        distinct_count = int(df[col].n_unique())
        pct = (dup_count / total * 100) if total > 0 else 0.0
        dup_values = df.filter(pl.col(col).is_duplicated())[col].unique().to_list()[:20]
        return self._build_result(
            success=(dup_count == 0),
            observed_value=f"{distinct_count} unique values out of {total}",
            element_count=total,
            unexpected_count=dup_count,
            unexpected_percent=pct,
            unexpected_values=dup_values,
            details={"duplicate_count": dup_count, "distinct_count": distinct_count},
        )


# ---------------------------------------------------------------------------
# 4. expect_column_values_to_be_between
# ---------------------------------------------------------------------------


@register_expectation
@dataclass
class ExpectColumnValuesToBeBetween(Expectation):
    """Expect column values to fall within [min_value, max_value]."""

    expectation_type: str = field(init=False, default="expect_column_values_to_be_between")

    def _validate_pandas(self, df: pd.DataFrame) -> ExpectationResult:
        min_val = self.kwargs.get("min_value")
        max_val = self.kwargs.get("max_value")
        strict_min = self.kwargs.get("strict_min", False)
        strict_max = self.kwargs.get("strict_max", False)

        series = df[self.column].dropna()
        total = len(series)

        if strict_min:
            mask_low = series <= min_val if min_val is not None else pd.Series(False, index=series.index)
        else:
            mask_low = series < min_val if min_val is not None else pd.Series(False, index=series.index)

        if strict_max:
            mask_high = series >= max_val if max_val is not None else pd.Series(False, index=series.index)
        else:
            mask_high = series > max_val if max_val is not None else pd.Series(False, index=series.index)

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
        stats = filtered.select(F.min(str(self.column)), F.max(str(self.column))).first()

        return self._build_result(
            success=(unexpected_count == 0),
            observed_value={"min": stats[0], "max": stats[1]},
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            details={"min_value": min_val, "max_value": max_val},
        )

    def _validate_polars(self, df: Any) -> ExpectationResult:
        import polars as pl

        col = str(self.column)
        min_val = self.kwargs.get("min_value")
        max_val = self.kwargs.get("max_value")
        strict_min = self.kwargs.get("strict_min", False)
        strict_max = self.kwargs.get("strict_max", False)

        series = df[col].drop_nulls()
        total = len(series)

        if min_val is not None:
            mask_low = series <= min_val if strict_min else series < min_val
        else:
            mask_low = pl.Series(values=[False] * total, dtype=pl.Boolean)

        if max_val is not None:
            mask_high = series >= max_val if strict_max else series > max_val
        else:
            mask_high = pl.Series(values=[False] * total, dtype=pl.Boolean)

        unexpected_mask = mask_low | mask_high
        unexpected_count = int(unexpected_mask.sum())
        pct = (unexpected_count / total * 100) if total > 0 else 0.0
        unexpected_vals = series.filter(unexpected_mask).to_list()[:20]

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

    def _validate_sql(self, sql_source: Any) -> ExpectationResult:
        from sqlalchemy import text

        engine, query_or_table = sql_source
        col = quote_sql_identifier(self.column)
        from_clause = format_sql_from_clause(query_or_table)
        min_val = self.kwargs.get("min_value")
        max_val = self.kwargs.get("max_value")
        strict_min = self.kwargs.get("strict_min", False)
        strict_max = self.kwargs.get("strict_max", False)

        conds = []
        params = {}
        if min_val is not None:
            op = "<=" if strict_min else "<"
            conds.append(f"{col} {op} :min_val")
            params["min_val"] = min_val
        if max_val is not None:
            op = ">=" if strict_max else ">"
            conds.append(f"{col} {op} :max_val")
            params["max_val"] = max_val

        where_unexpected = " OR ".join(conds) if conds else "1=0"
        query = (
            f"SELECT COUNT({col}) AS total, "  # nosec B608
            f"MIN({col}) AS col_min, MAX({col}) AS col_max, "
            f"SUM(CASE WHEN {col} IS NOT NULL AND ({where_unexpected}) THEN 1 ELSE 0 END) AS unexpected "
            f"FROM {from_clause}"
        )
        with engine.connect() as conn:
            row = conn.execute(text(query), params).fetchone()
            total = int(row.total) if row and row.total else 0
            unexpected_count = int(row.unexpected) if row and row.unexpected else 0
            col_min = row.col_min if row and total > 0 else None
            col_max = row.col_max if row and total > 0 else None

        pct = (unexpected_count / total * 100) if total > 0 else 0.0
        return self._build_result(
            success=(unexpected_count == 0),
            observed_value={"min": col_min, "max": col_max},
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            details={
                "min_value": min_val,
                "max_value": max_val,
                "strict_min": strict_min,
                "strict_max": strict_max,
            },
        )


# ---------------------------------------------------------------------------
# 5. expect_column_values_to_be_in_set
# ---------------------------------------------------------------------------


@register_expectation
@dataclass
class ExpectColumnValuesToBeInSet(Expectation):
    """Expect every value in a column to be a member of a given set."""

    expectation_type: str = field(init=False, default="expect_column_values_to_be_in_set")

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

    def _validate_polars(self, df: Any) -> ExpectationResult:
        value_set = set(self.kwargs.get("value_set", []))
        series = df[self.column].drop_nulls()
        total = len(series)
        unexpected_mask = ~series.is_in(value_set)
        unexpected_count = int(unexpected_mask.sum())
        pct = (unexpected_count / total * 100) if total > 0 else 0.0
        unexpected_vals = series.filter(unexpected_mask).unique().to_list()[:20]

        return self._build_result(
            success=(unexpected_count == 0),
            observed_value={"unique_values": series.n_unique()},
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            unexpected_values=unexpected_vals,
            details={"value_set": list(value_set)},
        )

    def _validate_sql(self, sql_source: Any) -> ExpectationResult:
        from sqlalchemy import text

        engine, query_or_table = sql_source
        value_list = list(self.kwargs.get("value_set", []))
        col = quote_sql_identifier(self.column)
        from_clause = format_sql_from_clause(query_or_table)

        params = {f"v_{i}": v for i, v in enumerate(value_list)}
        in_clause = ", ".join(f":v_{i}" for i in range(len(value_list)))
        cond = f"{col} NOT IN ({in_clause})" if value_list else "1=1"
        query = (
            f"SELECT COUNT({col}) AS total, COUNT(DISTINCT {col}) AS uniq, "  # nosec B608
            f"COALESCE(SUM(CASE WHEN {col} IS NOT NULL AND {cond} THEN 1 ELSE 0 END), 0) AS unexpected "
            f"FROM {from_clause}"
        )
        with engine.connect() as conn:
            row = conn.execute(text(query), params).fetchone()
            total = int(row.total) if row and row.total else 0
            uniq = int(row.uniq) if row and row.uniq else 0
            unexpected_count = int(row.unexpected) if row and row.unexpected else 0

        pct = (unexpected_count / total * 100) if total > 0 else 0.0
        return self._build_result(
            success=(unexpected_count == 0),
            observed_value={"unique_values": uniq},
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            details={"value_set": value_list},
        )


# ---------------------------------------------------------------------------
# 6. expect_column_values_to_not_be_in_set
# ---------------------------------------------------------------------------


@register_expectation
@dataclass
class ExpectColumnValuesToNotBeInSet(Expectation):
    """Expect no value in a column to be a member of the given set."""

    expectation_type: str = field(init=False, default="expect_column_values_to_not_be_in_set")

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

    def _validate_polars(self, df: Any) -> ExpectationResult:
        forbidden = set(self.kwargs.get("value_set", []))
        series = df[self.column].drop_nulls()
        total = len(series)
        unexpected_mask = series.is_in(forbidden)
        unexpected_count = int(unexpected_mask.sum())
        pct = (unexpected_count / total * 100) if total > 0 else 0.0
        unexpected_vals = series.filter(unexpected_mask).unique().to_list()[:20]

        return self._build_result(
            success=(unexpected_count == 0),
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            unexpected_values=unexpected_vals,
            details={"forbidden_set": list(forbidden)},
        )

    def _validate_sql(self, sql_source: Any) -> ExpectationResult:
        from sqlalchemy import text

        engine, query_or_table = sql_source
        forbidden = list(self.kwargs.get("value_set", []))
        col = quote_sql_identifier(self.column)
        from_clause = format_sql_from_clause(query_or_table)

        params = {f"v_{i}": v for i, v in enumerate(forbidden)}
        in_clause = ", ".join(f":v_{i}" for i in range(len(forbidden)))
        cond = f"{col} IN ({in_clause})" if forbidden else "1=0"
        query = (
            f"SELECT COUNT({col}) AS total, "  # nosec B608
            f"COALESCE(SUM(CASE WHEN {col} IS NOT NULL AND {cond} THEN 1 ELSE 0 END), 0) AS unexpected "
            f"FROM {from_clause}"
        )
        with engine.connect() as conn:
            row = conn.execute(text(query), params).fetchone()
            total = int(row.total) if row and row.total else 0
            unexpected_count = int(row.unexpected) if row and row.unexpected else 0

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

    expectation_type: str = field(init=False, default="expect_column_values_to_match_regex")

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

    def _validate_polars(self, df: Any) -> ExpectationResult:
        import polars as pl

        regex = self.kwargs.get("regex", ".*")
        series = df[self.column].drop_nulls().cast(pl.String)
        total = len(series)
        try:
            match_mask = series.str.contains(regex)
        except Exception:
            pattern = re.compile(regex)
            match_mask = series.map_elements(lambda x: bool(pattern.search(x)), return_dtype=pl.Boolean)
        unexpected_count = int((~match_mask).sum())
        pct = (unexpected_count / total * 100) if total > 0 else 0.0
        unexpected_vals = series.filter(~match_mask).to_list()[:20]

        return self._build_result(
            success=(unexpected_count == 0),
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            unexpected_values=unexpected_vals,
            details={"regex": regex},
        )


# ---------------------------------------------------------------------------
# 8. expect_column_values_to_be_of_type
# ---------------------------------------------------------------------------


@register_expectation
@dataclass
class ExpectColumnValuesToBeOfType(Expectation):
    """Expect a column's dtype to match the expected type string."""

    expectation_type: str = field(init=False, default="expect_column_values_to_be_of_type")

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

    def _validate_polars(self, df: Any) -> ExpectationResult:
        expected_type = self.kwargs.get("expected_type", "")
        actual_type = str(df[self.column].dtype)
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

    expectation_type: str = field(init=False, default="expect_column_values_to_be_dateutil_parseable")

    def _validate_pandas(self, df: pd.DataFrame) -> ExpectationResult:
        import dateutil.parser

        series = df[self.column].dropna()
        total = len(series)

        def check_date(x: Any) -> bool:
            try:
                dateutil.parser.parse(str(x))
                return True
            except Exception:
                return False

        parsed_mask = series.apply(check_date) if total > 0 else pd.Series(dtype=bool)
        unexpected_mask = ~parsed_mask
        null_after = int(unexpected_mask.sum())
        pct = (null_after / total * 100) if total > 0 else 0.0
        bad_vals = series[unexpected_mask].tolist()[:20]

        return self._build_result(
            success=(null_after == 0),
            element_count=total,
            unexpected_count=null_after,
            unexpected_percent=pct,
            unexpected_values=bad_vals,
            details={"unparseable_count": null_after},
        )

    def _validate_spark(self, df: Any) -> ExpectationResult:
        from pyspark.sql import functions as F

        col_name = str(self.column)
        filtered = df.filter(F.col(col_name).isNotNull())
        total = filtered.count()

        @F.udf(returnType="boolean")
        def check_date(val):
            try:
                import dateutil.parser

                dateutil.parser.parse(str(val))
                return True
            except Exception:
                return False

        unexpected_df = filtered.filter(~check_date(F.col(col_name)))
        unexpected_count = unexpected_df.count()
        pct = (unexpected_count / total * 100) if total > 0 else 0.0
        bad_vals = [r[0] for r in unexpected_df.select(col_name).limit(20).collect()]

        return self._build_result(
            success=(unexpected_count == 0),
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            unexpected_values=bad_vals,
            details={"unparseable_count": unexpected_count},
        )

    def _validate_polars(self, df: Any) -> ExpectationResult:
        import polars as pl

        series = df[self.column].drop_nulls()
        total = len(series)

        def check_date(x):
            try:
                import dateutil.parser

                dateutil.parser.parse(str(x))
                return True
            except Exception:
                return False

        parsed_mask = series.map_elements(check_date, return_dtype=pl.Boolean)
        unexpected_mask = parsed_mask.not_()
        unexpected_count = int(unexpected_mask.sum())
        pct = (unexpected_count / total * 100) if total > 0 else 0.0
        bad_vals = series.filter(unexpected_mask).to_list()[:20]

        return self._build_result(
            success=(unexpected_count == 0),
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            unexpected_values=bad_vals,
            details={"unparseable_count": unexpected_count},
        )


# ---------------------------------------------------------------------------
# 10. expect_column_value_lengths_to_be_between
# ---------------------------------------------------------------------------


@register_expectation
@dataclass
class ExpectColumnValueLengthsToBeBetween(Expectation):
    """Expect string lengths in a column to be within [min_value, max_value]."""

    expectation_type: str = field(init=False, default="expect_column_value_lengths_to_be_between")

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
        unexpected_count = filtered.filter((length_col < min_len) | (length_col > max_len)).count()
        pct = (unexpected_count / total * 100) if total > 0 else 0.0

        return self._build_result(
            success=(unexpected_count == 0),
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            details={"min_value": min_len, "max_value": max_len},
        )

    def _validate_polars(self, df: Any) -> ExpectationResult:
        import polars as pl

        min_len = self.kwargs.get("min_value", 0)
        max_len = self.kwargs.get("max_value", float("inf"))
        series = df[self.column].drop_nulls().cast(pl.String)
        total = len(series)
        lengths = series.str.len_chars()
        unexpected_mask = (lengths < min_len) | (lengths > max_len)
        unexpected_count = int(unexpected_mask.sum())
        pct = (unexpected_count / total * 100) if total > 0 else 0.0
        unexpected_vals = series.filter(unexpected_mask).to_list()[:20]

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

    def _validate_sql(self, sql_source: Any) -> ExpectationResult:
        from sqlalchemy import text

        engine, query_or_table = sql_source
        min_len = self.kwargs.get("min_value", 0)
        max_len = self.kwargs.get("max_value")
        col = quote_sql_identifier(self.column)
        from_clause = format_sql_from_clause(query_or_table)

        len_expr = f"LENGTH(CAST({col} AS VARCHAR))"
        conds = [f"{len_expr} < :min_len"]
        params = {"min_len": min_len}
        if max_len is not None and max_len != float("inf"):
            conds.append(f"{len_expr} > :max_len")
            params["max_len"] = max_len

        where_unexpected = " OR ".join(conds)
        query = (
            f"SELECT COUNT({col}) AS total, MIN({len_expr}) AS min_l, MAX({len_expr}) AS max_l, "  # nosec B608
            f"COALESCE(SUM(CASE WHEN {col} IS NOT NULL AND ({where_unexpected}) THEN 1 ELSE 0 END), 0) AS unexpected "
            f"FROM {from_clause}"
        )
        with engine.connect() as conn:
            row = conn.execute(text(query), params).fetchone()
            total = int(row.total) if row and row.total else 0
            unexpected_count = int(row.unexpected) if row and row.unexpected else 0
            min_l = int(row.min_l) if row and total > 0 and row.min_l is not None else None
            max_l = int(row.max_l) if row and total > 0 and row.max_l is not None else None

        pct = (unexpected_count / total * 100) if total > 0 else 0.0
        return self._build_result(
            success=(unexpected_count == 0),
            observed_value={"min_length": min_l, "max_length": max_l},
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            details={"min_value": min_len, "max_value": max_len if max_len is not None else float("inf")},
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
        series = df[self.column].dropna()
        raw_max = series.max() if len(series) > 0 else None
        col_max = None if raw_max is None or pd.isna(raw_max) else float(raw_max)

        success = True
        if col_max is None:
            success = False
        else:
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
        raw_max = df.agg(F.max(str(self.column))).first()[0]
        col_max = float(raw_max) if raw_max is not None else None

        success = True
        if col_max is None:
            success = False
        else:
            if min_val is not None and col_max < min_val:
                success = False
            if max_val is not None and col_max > max_val:
                success = False

        return self._build_result(
            success=success,
            observed_value=col_max,
            details={"min_value": min_val, "max_value": max_val, "column_max": col_max},
        )

    def _validate_polars(self, df: Any) -> ExpectationResult:
        min_val = self.kwargs.get("min_value")
        max_val = self.kwargs.get("max_value")
        series = df[self.column].drop_nulls()
        val = series.max() if len(series) > 0 else None
        col_max = float(val) if val is not None else None

        success = True
        if col_max is None:
            success = False
        else:
            if min_val is not None and col_max < min_val:
                success = False
            if max_val is not None and col_max > max_val:
                success = False

        return self._build_result(
            success=success,
            observed_value=col_max,
            details={"min_value": min_val, "max_value": max_val, "column_max": col_max},
        )

    def _validate_sql(self, sql_source: Any) -> ExpectationResult:
        from sqlalchemy import text

        engine, query_or_table = sql_source
        min_val = self.kwargs.get("min_value")
        max_val = self.kwargs.get("max_value")
        col = quote_sql_identifier(self.column)
        from_clause = format_sql_from_clause(query_or_table)

        query = f"SELECT MAX({col}) AS col_max FROM {from_clause}"  # nosec B608
        with engine.connect() as conn:
            raw_max = conn.execute(text(query)).scalar()
        col_max = float(raw_max) if raw_max is not None else None

        success = True
        if col_max is None:
            success = False
        else:
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
        series = df[self.column].dropna()
        raw_min = series.min() if len(series) > 0 else None
        col_min = None if raw_min is None or pd.isna(raw_min) else float(raw_min)

        success = True
        if col_min is None:
            success = False
        else:
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
        raw_min = df.agg(F.min(str(self.column))).first()[0]
        col_min = float(raw_min) if raw_min is not None else None

        success = True
        if col_min is None:
            success = False
        else:
            if min_val is not None and col_min < min_val:
                success = False
            if max_val is not None and col_min > max_val:
                success = False

        return self._build_result(
            success=success,
            observed_value=col_min,
            details={"min_value": min_val, "max_value": max_val, "column_min": col_min},
        )

    def _validate_polars(self, df: Any) -> ExpectationResult:
        min_val = self.kwargs.get("min_value")
        max_val = self.kwargs.get("max_value")
        series = df[self.column].drop_nulls()
        val = series.min() if len(series) > 0 else None
        col_min = float(val) if val is not None else None

        success = True
        if col_min is None:
            success = False
        else:
            if min_val is not None and col_min < min_val:
                success = False
            if max_val is not None and col_min > max_val:
                success = False

        return self._build_result(
            success=success,
            observed_value=col_min,
            details={"min_value": min_val, "max_value": max_val, "column_min": col_min},
        )

    def _validate_sql(self, sql_source: Any) -> ExpectationResult:
        from sqlalchemy import text

        engine, query_or_table = sql_source
        min_val = self.kwargs.get("min_value")
        max_val = self.kwargs.get("max_value")
        col = quote_sql_identifier(self.column)
        from_clause = format_sql_from_clause(query_or_table)

        query = f"SELECT MIN({col}) AS col_min FROM {from_clause}"  # nosec B608
        with engine.connect() as conn:
            raw_min = conn.execute(text(query)).scalar()
        col_min = float(raw_min) if raw_min is not None else None

        success = True
        if col_min is None:
            success = False
        else:
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

    expectation_type: str = field(init=False, default="expect_column_mean_to_be_between")

    def _validate_pandas(self, df: pd.DataFrame) -> ExpectationResult:
        min_val = self.kwargs.get("min_value")
        max_val = self.kwargs.get("max_value")
        series = df[self.column].dropna()
        raw_mean = series.mean() if len(series) > 0 else None
        col_mean = None if raw_mean is None or pd.isna(raw_mean) else float(raw_mean)

        success = True
        if col_mean is None:
            success = False
        else:
            if min_val is not None and col_mean < min_val:
                success = False
            if max_val is not None and col_mean > max_val:
                success = False

        return self._build_result(
            success=success,
            observed_value=round(col_mean, 4) if col_mean is not None else None,
            details={
                "min_value": min_val,
                "max_value": max_val,
                "column_mean": round(col_mean, 4) if col_mean is not None else None,
            },
        )

    def _validate_spark(self, df: Any) -> ExpectationResult:
        from pyspark.sql import functions as F

        min_val = self.kwargs.get("min_value")
        max_val = self.kwargs.get("max_value")
        raw_mean = df.agg(F.mean(str(self.column))).first()[0]
        col_mean = float(raw_mean) if raw_mean is not None else None

        success = True
        if col_mean is None:
            success = False
        else:
            if min_val is not None and col_mean < min_val:
                success = False
            if max_val is not None and col_mean > max_val:
                success = False

        return self._build_result(
            success=success,
            observed_value=round(col_mean, 4) if col_mean is not None else None,
            details={
                "min_value": min_val,
                "max_value": max_val,
                "column_mean": round(col_mean, 4) if col_mean is not None else None,
            },
        )

    def _validate_polars(self, df: Any) -> ExpectationResult:
        min_val = self.kwargs.get("min_value")
        max_val = self.kwargs.get("max_value")
        series = df[self.column].drop_nulls()
        val = series.mean() if len(series) > 0 else None
        col_mean = float(val) if val is not None else None

        success = True
        if col_mean is None:
            success = False
        else:
            if min_val is not None and col_mean < min_val:
                success = False
            if max_val is not None and col_mean > max_val:
                success = False

        return self._build_result(
            success=success,
            observed_value=round(col_mean, 4) if col_mean is not None else None,
            details={
                "min_value": min_val,
                "max_value": max_val,
                "column_mean": round(col_mean, 4) if col_mean is not None else None,
            },
        )

    def _validate_sql(self, sql_source: Any) -> ExpectationResult:
        from sqlalchemy import text

        engine, query_or_table = sql_source
        min_val = self.kwargs.get("min_value")
        max_val = self.kwargs.get("max_value")
        col = quote_sql_identifier(self.column)
        from_clause = format_sql_from_clause(query_or_table)

        query = f"SELECT AVG({col}) AS col_mean FROM {from_clause}"  # nosec B608
        with engine.connect() as conn:
            raw_mean = conn.execute(text(query)).scalar()
        col_mean = float(raw_mean) if raw_mean is not None else None

        success = True
        if col_mean is None:
            success = False
        else:
            if min_val is not None and col_mean < min_val:
                success = False
            if max_val is not None and col_mean > max_val:
                success = False

        return self._build_result(
            success=success,
            observed_value=round(col_mean, 4) if col_mean is not None else None,
            details={
                "min_value": min_val,
                "max_value": max_val,
                "column_mean": round(col_mean, 4) if col_mean is not None else None,
            },
        )


# ---------------------------------------------------------------------------
# 14. expect_column_stdev_to_be_between
# ---------------------------------------------------------------------------


@register_expectation
@dataclass
class ExpectColumnStdevToBeBetween(Expectation):
    """Expect the standard deviation of a column to fall within bounds."""

    expectation_type: str = field(init=False, default="expect_column_stdev_to_be_between")

    def _validate_pandas(self, df: pd.DataFrame) -> ExpectationResult:
        min_val = self.kwargs.get("min_value")
        max_val = self.kwargs.get("max_value")
        series = df[self.column].dropna()
        raw_std = series.std() if len(series) >= 2 else None
        col_std = None if raw_std is None or pd.isna(raw_std) else float(raw_std)

        success = True
        if col_std is None:
            success = False
        else:
            if min_val is not None and col_std < min_val:
                success = False
            if max_val is not None and col_std > max_val:
                success = False

        return self._build_result(
            success=success,
            observed_value=round(col_std, 4) if col_std is not None else None,
            details={
                "min_value": min_val,
                "max_value": max_val,
                "column_stdev": round(col_std, 4) if col_std is not None else None,
            },
        )

    def _validate_spark(self, df: Any) -> ExpectationResult:
        from pyspark.sql import functions as F

        min_val = self.kwargs.get("min_value")
        max_val = self.kwargs.get("max_value")
        raw_std = df.agg(F.stddev(str(self.column))).first()[0]
        col_std = float(raw_std) if raw_std is not None else None

        success = True
        if col_std is None:
            success = False
        else:
            if min_val is not None and col_std < min_val:
                success = False
            if max_val is not None and col_std > max_val:
                success = False

        return self._build_result(
            success=success,
            observed_value=round(col_std, 4) if col_std is not None else None,
            details={
                "min_value": min_val,
                "max_value": max_val,
                "column_stdev": round(col_std, 4) if col_std is not None else None,
            },
        )

    def _validate_polars(self, df: Any) -> ExpectationResult:
        min_val = self.kwargs.get("min_value")
        max_val = self.kwargs.get("max_value")
        series = df[self.column].drop_nulls()
        val = series.std() if len(series) >= 2 else None
        col_std = float(val) if val is not None else None

        success = True
        if col_std is None:
            success = False
        else:
            if min_val is not None and col_std < min_val:
                success = False
            if max_val is not None and col_std > max_val:
                success = False

        return self._build_result(
            success=success,
            observed_value=round(col_std, 4) if col_std is not None else None,
            details={
                "min_value": min_val,
                "max_value": max_val,
                "column_stdev": round(col_std, 4) if col_std is not None else None,
            },
        )


# ---------------------------------------------------------------------------
# 15. expect_column_distinct_values_to_be_in_set
# ---------------------------------------------------------------------------


@register_expectation
@dataclass
class ExpectColumnDistinctValuesToBeInSet(Expectation):
    """Expect all distinct values in a column to be in the given set."""

    expectation_type: str = field(init=False, default="expect_column_distinct_values_to_be_in_set")

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
            unexpected_percent=((len(unexpected) / total_distinct * 100) if total_distinct > 0 else 0.0),
            unexpected_values=list(unexpected)[:20],
            details={"value_set": list(value_set)},
        )

    def _validate_spark(self, df: Any) -> ExpectationResult:
        value_set = set(self.kwargs.get("value_set", []))
        row_list = df.select(self.column).distinct().collect()
        actual_values = {row[0] for row in row_list if row[0] is not None}
        unexpected = actual_values - value_set
        total_distinct = len(actual_values)

        return self._build_result(
            success=(len(unexpected) == 0),
            observed_value={"distinct_values": list(actual_values)[:20]},
            element_count=total_distinct,
            unexpected_count=len(unexpected),
            unexpected_percent=((len(unexpected) / total_distinct * 100) if total_distinct > 0 else 0.0),
            unexpected_values=list(unexpected)[:20],
            details={"value_set": list(value_set)},
        )

    def _validate_polars(self, df: Any) -> ExpectationResult:
        value_set = set(self.kwargs.get("value_set", []))
        actual_values = set(df[self.column].drop_nulls().unique().to_list())
        unexpected = actual_values - value_set
        total_distinct = len(actual_values)

        return self._build_result(
            success=(len(unexpected) == 0),
            observed_value={"distinct_values": list(actual_values)[:20]},
            element_count=total_distinct,
            unexpected_count=len(unexpected),
            unexpected_percent=((len(unexpected) / total_distinct * 100) if total_distinct > 0 else 0.0),
            unexpected_values=list(unexpected)[:20],
            details={"value_set": list(value_set)},
        )

    def _validate_sql(self, sql_source: Any) -> ExpectationResult:
        from sqlalchemy import text

        engine, query_or_table = sql_source
        value_set = set(self.kwargs.get("value_set", []))
        col = quote_sql_identifier(self.column)
        from_clause = format_sql_from_clause(query_or_table)

        query = f"SELECT DISTINCT {col} AS val FROM {from_clause} WHERE {col} IS NOT NULL"  # nosec B608
        with engine.connect() as conn:
            rows = conn.execute(text(query)).fetchall()
        actual_values = {r.val for r in rows}
        unexpected = actual_values - value_set
        total_distinct = len(actual_values)

        return self._build_result(
            success=(len(unexpected) == 0),
            observed_value={"distinct_values": list(actual_values)[:20]},
            element_count=total_distinct,
            unexpected_count=len(unexpected),
            unexpected_percent=((len(unexpected) / total_distinct * 100) if total_distinct > 0 else 0.0),
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

    def _validate_spark(self, df: Any) -> ExpectationResult:
        from pyspark.sql import functions as F

        min_val = self.kwargs.get("min_value", 0.0)
        max_val = self.kwargs.get("max_value", 1.0)
        col_name = str(self.column)
        filtered = df.filter(F.col(col_name).isNotNull())
        total = filtered.count()
        unique_count = filtered.select(col_name).distinct().count()
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

    def _validate_polars(self, df: Any) -> ExpectationResult:
        min_val = self.kwargs.get("min_value", 0.0)
        max_val = self.kwargs.get("max_value", 1.0)
        series = df[self.column].drop_nulls()
        total = len(series)
        unique_count = series.n_unique()
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

    def _validate_sql(self, sql_source: Any) -> ExpectationResult:
        from sqlalchemy import text

        engine, query_or_table = sql_source
        min_val = self.kwargs.get("min_value", 0.0)
        max_val = self.kwargs.get("max_value", 1.0)
        col = quote_sql_identifier(self.column)
        from_clause = format_sql_from_clause(query_or_table)

        query = f"SELECT COUNT({col}) AS total, COUNT(DISTINCT {col}) AS uniq FROM {from_clause}"  # nosec B608
        with engine.connect() as conn:
            row = conn.execute(text(query)).fetchone()
            total = int(row.total) if row and row.total else 0
            unique_count = int(row.uniq) if row and row.uniq else 0

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
