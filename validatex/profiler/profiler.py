"""
Data Profiler — analyse a dataset and auto-suggest expectations.

The profiler computes summary statistics for every column and
proposes a reasonable set of expectations that can serve as a
starting point for a quality suite.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import pandas as pd

from validatex.core.suite import ExpectationSuite


@dataclass
class ColumnProfile:
    """Statistical profile of a single column."""

    name: str
    dtype: str = ""
    total_count: int = 0
    null_count: int = 0
    null_percent: float = 0.0
    unique_count: int = 0
    unique_percent: float = 0.0
    min_value: Any = None
    max_value: Any = None
    mean_value: Optional[float] = None
    std_value: Optional[float] = None
    median_value: Optional[float] = None
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    top_values: List[Dict[str, Any]] = field(default_factory=list)
    sample_values: List[Any] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "dtype": self.dtype,
            "total_count": self.total_count,
            "null_count": self.null_count,
            "null_percent": round(self.null_percent, 2),
            "unique_count": self.unique_count,
            "unique_percent": round(self.unique_percent, 2),
            "min_value": self._safe(self.min_value),
            "max_value": self._safe(self.max_value),
            "mean_value": (
                round(self.mean_value, 4) if self.mean_value is not None else None
            ),
            "std_value": (
                round(self.std_value, 4) if self.std_value is not None else None
            ),
            "median_value": (
                round(self.median_value, 4) if self.median_value is not None else None
            ),
            "min_length": self.min_length,
            "max_length": self.max_length,
            "top_values": self.top_values[:10],
            "sample_values": [self._safe(v) for v in self.sample_values[:5]],
        }

    @staticmethod
    def _safe(val: Any) -> Any:
        if val is None:
            return None
        if isinstance(val, (int, float, bool, str)):
            return val
        return str(val)


@dataclass
class DataProfile:
    """Full profile of a DataFrame."""

    row_count: int = 0
    column_count: int = 0
    columns: List[ColumnProfile] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "row_count": self.row_count,
            "column_count": self.column_count,
            "columns": [c.to_dict() for c in self.columns],
        }

    def summary(self) -> str:
        """Return a human-readable summary."""
        lines = [
            f"{'='*60}",
            "  ValidateX Data Profile",
            f"{'='*60}",
            f"  Rows    : {self.row_count:,}",
            f"  Columns : {self.column_count}",
            f"{'='*60}",
        ]
        for cp in self.columns:
            lines.append(f"\n  📊 Column: {cp.name}")
            lines.append(f"     Type       : {cp.dtype}")
            lines.append(f"     Nulls      : {cp.null_count} ({cp.null_percent:.1f}%)")
            lines.append(
                f"     Unique     : {cp.unique_count} ({cp.unique_percent:.1f}%)"
            )
            if cp.min_value is not None:
                lines.append(f"     Min        : {cp.min_value}")
                lines.append(f"     Max        : {cp.max_value}")
            if cp.mean_value is not None:
                lines.append(f"     Mean       : {cp.mean_value:.4f}")
                lines.append(f"     Std Dev    : {cp.std_value:.4f}")
                lines.append(f"     Median     : {cp.median_value:.4f}")
            if cp.min_length is not None:
                lines.append(f"     Str Len    : {cp.min_length} – {cp.max_length}")
            if cp.top_values:
                top_str = ", ".join(
                    f"{v['value']}({v['count']})" for v in cp.top_values[:5]
                )
                lines.append(f"     Top Values : {top_str}")
        lines.append(f"\n{'='*60}")
        return "\n".join(lines)


class DataProfiler:
    """
    Analyse a Pandas DataFrame and produce a :class:`DataProfile`.

    Usage
    -----
    >>> profiler = DataProfiler()
    >>> profile = profiler.profile(df)
    >>> print(profile.summary())
    >>> suite = profiler.suggest_expectations(df, suite_name="auto_suite")
    """

    def profile(self, df: pd.DataFrame) -> DataProfile:
        """
        Profile every column in *df*.

        Returns
        -------
        DataProfile
        """
        profile = DataProfile(
            row_count=len(df),
            column_count=len(df.columns),
        )

        for col_name in df.columns:
            cp = self._profile_column(df, col_name)
            profile.columns.append(cp)

        return profile

    def suggest_expectations(
        self,
        df: pd.DataFrame,
        suite_name: str = "auto_generated_suite",
    ) -> ExpectationSuite:
        """
        Auto-generate an :class:`ExpectationSuite` based on the data profile.

        Heuristics
        ----------
        * If a column has zero nulls → ``expect_column_to_not_be_null``
        * If a column is fully unique → ``expect_column_values_to_be_unique``
        * For numeric columns → ``expect_column_values_to_be_between``
          with observed min/max.
        * For string columns with few distinct values →
          ``expect_column_values_to_be_in_set``
        * For string columns → ``expect_column_value_lengths_to_be_between``
        """
        # Import expectations so they are registered
        import validatex.expectations  # noqa: F401

        profile = self.profile(df)
        suite = ExpectationSuite(name=suite_name)

        # Table-level
        suite.add(
            "expect_table_row_count_to_be_between",
            min_value=max(0, profile.row_count - profile.row_count // 10),
            max_value=profile.row_count + profile.row_count // 10,
        )
        suite.add(
            "expect_table_column_count_to_equal",
            value=profile.column_count,
        )

        for cp in profile.columns:
            # Column existence
            suite.add("expect_column_to_exist", column=cp.name)

            # Null checks
            if cp.null_count == 0:
                suite.add("expect_column_to_not_be_null", column=cp.name)

            # Uniqueness
            if cp.unique_count == cp.total_count and cp.total_count > 0:
                suite.add("expect_column_values_to_be_unique", column=cp.name)

            # Numeric range
            if cp.mean_value is not None and cp.min_value is not None:
                margin = (
                    abs(cp.max_value - cp.min_value) * 0.1
                    if cp.max_value != cp.min_value
                    else 1
                )
                suite.add(
                    "expect_column_values_to_be_between",
                    column=cp.name,
                    min_value=cp.min_value - margin,
                    max_value=cp.max_value + margin,
                )

            # Categorical (string with few distinct values)
            dtype_lower = cp.dtype.lower()
            is_string = (
                dtype_lower.startswith("object")
                or dtype_lower in ("str", "string")
                or "string" in dtype_lower
            )
            if is_string and 0 < cp.unique_count <= 20 and cp.total_count > 0:
                values = [v["value"] for v in cp.top_values if v["value"] is not None]
                if values:
                    suite.add(
                        "expect_column_values_to_be_in_set",
                        column=cp.name,
                        value_set=values,
                    )

            # String length
            if cp.min_length is not None and cp.max_length is not None:
                suite.add(
                    "expect_column_value_lengths_to_be_between",
                    column=cp.name,
                    min_value=max(0, cp.min_length - 1),
                    max_value=cp.max_length + 10,
                )

        return suite

    # -- internal ----------------------------------------------------------

    def _profile_column(self, df: pd.DataFrame, col: str) -> ColumnProfile:
        """Profile a single column."""
        series = df[col]
        total = len(series)
        null_count = int(series.isnull().sum())
        non_null = series.dropna()
        unique_count = int(non_null.nunique())

        cp = ColumnProfile(
            name=col,
            dtype=str(series.dtype),
            total_count=total,
            null_count=null_count,
            null_percent=(null_count / total * 100) if total > 0 else 0.0,
            unique_count=unique_count,
            unique_percent=(unique_count / total * 100) if total > 0 else 0.0,
        )

        # Numeric stats
        if pd.api.types.is_numeric_dtype(series):
            if len(non_null) > 0:
                cp.min_value = non_null.min()
                cp.max_value = non_null.max()
                cp.mean_value = float(non_null.mean())
                cp.std_value = float(non_null.std()) if len(non_null) > 1 else 0.0
                cp.median_value = float(non_null.median())

        # String stats — handle 'object', 'str', 'string', 'StringDtype' etc.
        dtype_str = str(series.dtype).lower()
        is_string_col = (
            dtype_str == "object"
            or dtype_str in ("str", "string")
            or "string" in dtype_str
        )
        if is_string_col:
            str_series = non_null.astype(str)
            if len(str_series) > 0:
                lengths = str_series.str.len()
                cp.min_length = int(lengths.min())
                cp.max_length = int(lengths.max())
                cp.min_value = str(non_null.min()) if len(non_null) > 0 else None
                cp.max_value = str(non_null.max()) if len(non_null) > 0 else None

        # Top values
        if len(non_null) > 0:
            value_counts = non_null.value_counts().head(10)
            cp.top_values = [
                {"value": str(v), "count": int(c)} for v, c in value_counts.items()
            ]

        # Sample values
        if len(non_null) > 0:
            cp.sample_values = non_null.head(5).tolist()

        return cp
