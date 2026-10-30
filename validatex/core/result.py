"""
Validation result data models.

Every expectation run produces an :class:`ExpectationResult`.
A full validation run aggregates them into a :class:`ValidationResult`.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

# ---------------------------------------------------------------------------
# Severity constants
# ---------------------------------------------------------------------------

SEVERITY_CRITICAL = "critical"
SEVERITY_WARNING = "warning"
SEVERITY_INFO = "info"

SEVERITY_WEIGHTS = {
    SEVERITY_CRITICAL: 3,
    SEVERITY_WARNING: 2,
    SEVERITY_INFO: 1,
}

# Default severity mapping based on expectation type keywords (covers all 50 built-in expectations)
_SEVERITY_MAP: Dict[str, str] = {
    # Critical — data integrity & schema contracts
    "expect_column_to_exist": SEVERITY_CRITICAL,
    "expect_column_to_not_be_null": SEVERITY_CRITICAL,
    "expect_column_values_to_be_unique": SEVERITY_CRITICAL,
    "expect_table_row_count_to_equal": SEVERITY_CRITICAL,
    "expect_table_row_count_to_be_between": SEVERITY_CRITICAL,
    "expect_table_columns_to_match_ordered_list": SEVERITY_CRITICAL,
    "expect_table_columns_to_match_set": SEVERITY_CRITICAL,
    "expect_table_column_count_to_equal": SEVERITY_CRITICAL,
    "expect_compound_columns_to_be_unique": SEVERITY_CRITICAL,
    "expect_column_values_to_be_not_null_when": SEVERITY_CRITICAL,
    # Warning — domain rules, format & row-level quality
    "expect_column_values_to_be_between": SEVERITY_WARNING,
    "expect_column_values_to_be_in_set": SEVERITY_WARNING,
    "expect_column_values_to_not_be_in_set": SEVERITY_WARNING,
    "expect_column_values_to_match_regex": SEVERITY_WARNING,
    "expect_column_values_to_not_match_regex": SEVERITY_WARNING,
    "expect_column_values_to_be_of_type": SEVERITY_WARNING,
    "expect_column_values_to_be_dateutil_parseable": SEVERITY_WARNING,
    "expect_column_pair_values_a_to_be_greater_than_b": SEVERITY_WARNING,
    "expect_column_pair_values_to_be_equal": SEVERITY_WARNING,
    "expect_multicolumn_sum_to_equal": SEVERITY_WARNING,
    "expect_column_values_to_be_valid_email": SEVERITY_WARNING,
    "expect_column_values_to_be_json_parseable": SEVERITY_WARNING,
    "expect_column_null_percentage_to_be_less_than": SEVERITY_WARNING,
    "expect_column_values_to_be_positive": SEVERITY_WARNING,
    "expect_column_values_to_be_negative": SEVERITY_WARNING,
    "expect_column_values_to_have_no_whitespace": SEVERITY_WARNING,
    "expect_column_values_to_be_valid_url": SEVERITY_WARNING,
    "expect_column_values_to_be_valid_ip_address": SEVERITY_WARNING,
    "expect_column_values_to_be_valid_uuid": SEVERITY_WARNING,
    "expect_column_values_to_be_valid_iso_date": SEVERITY_WARNING,
    "expect_column_values_to_be_valid_phone_number": SEVERITY_WARNING,
    "expect_column_values_to_be_null_when": SEVERITY_WARNING,
    "expect_column_values_to_satisfy": SEVERITY_WARNING,
    # Info — statistical / distributional / informational
    "expect_column_value_lengths_to_be_between": SEVERITY_INFO,
    "expect_column_value_lengths_to_equal": SEVERITY_INFO,
    "expect_column_max_to_be_between": SEVERITY_INFO,
    "expect_column_min_to_be_between": SEVERITY_INFO,
    "expect_column_mean_to_be_between": SEVERITY_INFO,
    "expect_column_stdev_to_be_between": SEVERITY_INFO,
    "expect_column_sum_to_be_between": SEVERITY_INFO,
    "expect_column_median_to_be_between": SEVERITY_INFO,
    "expect_column_quantile_values_to_be_between": SEVERITY_INFO,
    "expect_column_values_to_be_in_range_of_std_devs": SEVERITY_INFO,
    "expect_column_correlation_to_be_between": SEVERITY_INFO,
    "expect_column_distinct_values_to_be_in_set": SEVERITY_INFO,
    "expect_column_proportion_of_unique_values_to_be_between": SEVERITY_INFO,
    "expect_column_values_to_be_all_uppercase": SEVERITY_INFO,
    "expect_column_values_to_be_all_lowercase": SEVERITY_INFO,
    "expect_column_values_to_be_increasing": SEVERITY_INFO,
    "expect_column_values_to_be_decreasing": SEVERITY_INFO,
}


def get_severity(expectation_type: str, meta: Optional[Dict] = None) -> str:
    """Return severity for an expectation type (user meta overrides default)."""
    if meta and "severity" in meta:
        return str(meta["severity"])
    return _SEVERITY_MAP.get(expectation_type, SEVERITY_WARNING)


# ---------------------------------------------------------------------------
# Native-type coercion helper
# ---------------------------------------------------------------------------


def to_native(value: Any) -> Any:
    """
    Convert numpy / pandas scalar types to native Python types.

    Professional tools NEVER leak internal types like ``np.int64(20)``.
    """
    import math

    if value is None:
        return None
    if isinstance(value, (bool,)):
        return bool(value)
    if isinstance(value, float):
        return None if math.isnan(value) else value
    if isinstance(value, (int, str)):
        return value
    # numpy scalar types
    try:
        import numpy as np

        if isinstance(value, np.integer):
            return int(value)
        if isinstance(value, np.floating):
            f_val = float(value)
            return None if math.isnan(f_val) else f_val
        if isinstance(value, np.bool_):
            return bool(value)
        if isinstance(value, np.ndarray):
            return [to_native(v) for v in value.tolist()]
    except ImportError:
        pass
    # dict — recursively clean
    if isinstance(value, dict):
        return {k: to_native(v) for k, v in value.items()}
    # list / tuple
    if isinstance(value, (list, tuple)):
        return [to_native(v) for v in value]
    return value


# ---------------------------------------------------------------------------
# ExpectationResult
# ---------------------------------------------------------------------------


@dataclass
class ExpectationResult:
    """Result of a single expectation evaluation."""

    expectation_type: str
    success: bool
    column: Optional[str] = None
    observed_value: Any = None
    element_count: int = 0
    unexpected_count: int = 0
    unexpected_percent: float = 0.0
    unexpected_values: List[Any] = field(default_factory=list)
    details: Dict[str, Any] = field(default_factory=dict)
    exception_info: Optional[str] = None
    meta: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """Sanitize all numeric values to native Python types."""
        self.observed_value = to_native(self.observed_value)
        self.element_count = int(self.element_count) if self.element_count else 0
        self.unexpected_count = int(self.unexpected_count) if self.unexpected_count else 0
        self.unexpected_percent = float(self.unexpected_percent) if self.unexpected_percent else 0.0
        self.unexpected_values = [to_native(v) for v in self.unexpected_values]
        self.details = to_native(self.details) or {}

    @property
    def status(self) -> str:
        if self.exception_info:
            return "ERROR"
        return "PASSED" if self.success else "FAILED"

    @property
    def status_icon(self) -> str:
        icons = {"PASSED": "✅", "FAILED": "❌", "ERROR": "⚠️"}
        return icons.get(self.status, "❓")

    @property
    def severity(self) -> str:
        """Return severity level for this expectation."""
        return get_severity(self.expectation_type, self.meta)

    @property
    def severity_icon(self) -> str:
        icons = {
            SEVERITY_CRITICAL: "🔴",
            SEVERITY_WARNING: "🟡",
            SEVERITY_INFO: "🔵",
        }
        return icons.get(self.severity, "🟡")

    @property
    def human_observed(self) -> str:
        """
        Return a human-readable string for the observed value.

        Converts raw dicts / technical strings into executive-friendly text.
        """
        val = self.observed_value
        if val is None:
            return "—"

        # Dict-style observed values → readable sentences
        if isinstance(val, dict):
            parts = []
            if "min" in val and "max" in val:
                parts.append(f"Min: {val['min']}  |  Max: {val['max']}")
            if "min_length" in val and "max_length" in val:
                parts.append(f"Length: {val['min_length']} – {val['max_length']}")
            if "unique_values" in val:
                parts.append(f"Distinct values: {val['unique_values']}")
            if "distinct_values" in val:
                vals = val["distinct_values"]
                if isinstance(vals, list):
                    parts.append(f"Distinct: {', '.join(str(v) for v in vals[:8])}")
                else:
                    parts.append(f"Distinct values: {vals}")
            if parts:
                return " · ".join(parts)
            # Fallback: key=value pairs
            return " · ".join(f"{k}: {v}" for k, v in val.items())

        # String containing "unique ... out of" → reformat
        s = str(val)
        if "unique" in s and "out of" in s:
            import re

            m = re.search(r"(\d+)\s+unique.*?out of\s+(\d+)", s)
            if m:
                uniq = int(m.group(1))
                total = int(m.group(2))
                pct = round(uniq / total * 100, 1) if total > 0 else 0
                return f"Unique: {uniq}/{total} ({pct}%)"

        # List → join
        if isinstance(val, list):
            if len(val) == 0:
                return "—"
            return ", ".join(str(v) for v in val[:10])

        return str(val)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "expectation_type": self.expectation_type,
            "success": self.success,
            "status": self.status,
            "severity": self.severity,
            "column": self.column,
            "observed_value": to_native(self.observed_value),
            "element_count": self.element_count,
            "unexpected_count": self.unexpected_count,
            "unexpected_percent": round(self.unexpected_percent, 4),
            "unexpected_values": [to_native(v) for v in self.unexpected_values[:20]],
            "details": to_native(self.details),
            "exception_info": self.exception_info,
            "meta": self.meta,
        }


# ---------------------------------------------------------------------------
# ColumnHealthSummary
# ---------------------------------------------------------------------------


@dataclass
class ColumnHealthSummary:
    """Aggregated health metrics for a single column."""

    column: str
    checks: int = 0
    passed: int = 0
    failed: int = 0
    errors: int = 0
    null_count: Optional[int] = None
    null_percent: Optional[float] = None
    unique_count: Optional[int] = None
    unique_percent: Optional[float] = None
    total_rows: Optional[int] = None

    @property
    def health_score(self) -> float:
        if self.checks == 0:
            return 0.0
        return round((self.passed / self.checks) * 100, 1)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "column": self.column,
            "checks": self.checks,
            "passed": self.passed,
            "failed": self.failed,
            "errors": self.errors,
            "health_score": self.health_score,
            "null_count": self.null_count,
            "null_percent": (round(self.null_percent, 2) if self.null_percent is not None else None),
            "unique_count": self.unique_count,
            "unique_percent": (round(self.unique_percent, 2) if self.unique_percent is not None else None),
        }


# ---------------------------------------------------------------------------
# ValidationResult
# ---------------------------------------------------------------------------


@dataclass
class ValidationResult:
    """Aggregate result of running an entire expectation suite."""

    suite_name: str
    results: List[ExpectationResult] = field(default_factory=list)
    run_time: Optional[datetime] = None
    run_duration_seconds: float = 0.0
    data_source: Optional[str] = None
    engine: str = "pandas"
    statistics: Dict[str, Any] = field(default_factory=dict)
    total_columns: Optional[int] = None
    suite_meta: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if self.run_time is None:
            self.run_time = datetime.now()

    @property
    def success(self) -> bool:
        """True only if at least one expectation ran and *every* expectation passed."""
        if not self.results:
            return True
        return all(r.success for r in self.results)

    @property
    def total_expectations(self) -> int:
        return len(self.results)

    @property
    def successful_expectations(self) -> int:
        return sum(1 for r in self.results if r.success)

    @property
    def failed_expectations(self) -> int:
        return sum(1 for r in self.results if not r.success and not r.exception_info)

    @property
    def errored_expectations(self) -> int:
        return sum(1 for r in self.results if r.exception_info)

    @property
    def success_percent(self) -> float:
        if not self.results:
            return 0.0
        return round((self.successful_expectations / self.total_expectations) * 100, 2)

    @property
    def checked_columns(self) -> int:
        """Number of distinct dataset columns covered by at least one expectation."""
        cols = set()
        for r in self.results:
            if r.column:
                cols.add(r.column)
            if isinstance(r.details, dict):
                for key in ("column_a", "column_b", "corr_with"):
                    if r.details.get(key):
                        cols.add(str(r.details[key]))
                if isinstance(r.details.get("column_list"), list):
                    cols.update(str(c) for c in r.details["column_list"])
        return len(cols)

    @property
    def column_coverage_percent(self) -> Optional[float]:
        """Percentage of dataset columns covered by at least one expectation."""
        if self.total_columns is None or self.total_columns <= 0:
            return None
        return round(min(self.checked_columns / self.total_columns * 100.0, 100.0), 1)

    # -- Quality Score -----------------------------------------------------

    def compute_quality_score(self) -> float:
        """
        Compute a weighted rule pass score (0–100) across configured expectations.

        IMPORTANT: This score reflects pass rates of *configured checks only*,
        not absolute dataset quality. Always inspect ``column_coverage_percent``
        and ``total_expectations`` alongside this score.

        Severity weights:
          - Critical: ×3
          - Warning : ×2
          - Info    : ×1

        Score = 100 × (weighted_passed / weighted_total)
        """
        if not self.results:
            return 0.0
        weighted_passed = 0.0
        weighted_total = 0.0
        for r in self.results:
            w = SEVERITY_WEIGHTS.get(r.severity, 2)
            weighted_total += w
            if r.success:
                weighted_passed += w
        if weighted_total == 0:
            return 0.0
        return round((weighted_passed / weighted_total) * 100, 1)

    # -- Column Health Summary ---------------------------------------------

    def column_health(self) -> List[ColumnHealthSummary]:
        """
        Aggregate expectation results by column.

        Extracts null % and unique % from specific expectation types
        when present.
        """
        col_map: Dict[str, ColumnHealthSummary] = {}

        for r in self.results:
            col = r.column or "__table__"
            if col not in col_map:
                col_map[col] = ColumnHealthSummary(column=col)
            summary = col_map[col]
            summary.checks += 1
            if r.success:
                summary.passed += 1
            elif r.exception_info:
                summary.errors += 1
            else:
                summary.failed += 1

            # Extract null info
            if r.expectation_type == "expect_column_to_not_be_null":
                details = r.details or {}
                summary.null_count = details.get("null_count", r.unexpected_count)
                total = details.get("total_count", r.element_count)
                summary.total_rows = total
                if total and total > 0:
                    nc = summary.null_count or 0
                    summary.null_percent = (nc / total) * 100

            # Extract uniqueness info
            if r.expectation_type == "expect_column_values_to_be_unique":
                details = r.details or {}
                total = r.element_count
                dup = details.get("duplicate_count", r.unexpected_count)
                summary.total_rows = total
                if total and total > 0:
                    summary.unique_count = total - dup
                    summary.unique_percent = ((total - dup) / total) * 100

        # Return column summaries (table-level last)
        cols = sorted(
            col_map.values(),
            key=lambda c: (c.column == "__table__", c.column),
        )
        return cols

    # -- Statistics --------------------------------------------------------

    def compute_statistics(self) -> Dict[str, Any]:
        """Compute summary statistics and store them."""
        self.statistics = {
            "total": self.total_expectations,
            "passed": self.successful_expectations,
            "failed": self.failed_expectations,
            "errors": self.errored_expectations,
            "success_percent": self.success_percent,
            "overall_success": self.success,
            "quality_score": self.compute_quality_score(),
            "quality_score_scope": "configured_checks_only",
            "checked_columns": self.checked_columns,
            "total_columns": self.total_columns,
            "column_coverage_percent": self.column_coverage_percent,
            "run_time": self.run_time.isoformat() if self.run_time else None,
            "run_duration_seconds": round(self.run_duration_seconds, 3),
        }
        return self.statistics

    def to_dict(self) -> Dict[str, Any]:
        self.compute_statistics()
        return {
            "suite_name": self.suite_name,
            "suite_meta": self.suite_meta,
            "success": self.success,
            "statistics": self.statistics,
            "quality_score": self.compute_quality_score(),
            "quality_score_scope": "configured_checks_only",
            "column_health": [c.to_dict() for c in self.column_health()],
            "data_source": self.data_source,
            "engine": self.engine,
            "results": [r.to_dict() for r in self.results],
        }

    def to_json(self, indent: int = 2) -> str:
        """Serialize the full result to a JSON string."""
        return json.dumps(self.to_dict(), indent=indent, default=str)

    def to_json_file(self, filepath: str) -> None:
        """Write the validation result to a JSON file."""
        import os

        dirname = os.path.dirname(filepath)
        if dirname:
            os.makedirs(dirname, exist_ok=True)
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(self.to_json())

    def to_html(self, filepath: str) -> None:
        """Generate a rich HTML report and write to *filepath*."""
        import os

        dirname = os.path.dirname(filepath)
        if dirname:
            os.makedirs(dirname, exist_ok=True)
        from validatex.reporting.html_report import HTMLReportGenerator

        generator = HTMLReportGenerator()
        generator.generate(self, filepath)

    def save_to_store(self, store_path: Optional[str] = None) -> int:
        """Log this validation run to a local RunStore database."""
        from validatex.storage.run_store import RunStore

        store = RunStore(db_path=store_path)
        return store.log_run(self)

    def send_slack(self, webhook_url: Optional[str] = None, notify_on: str = "failure") -> bool:
        """
        Send a validation alert to Slack via incoming webhook.

        Parameters
        ----------
        webhook_url : str, optional
            The Slack webhook URL. If not provided, will look for the
            VALIDATEX_SLACK_WEBHOOK_URL environment variable.
        notify_on : str, default "failure"
            When to send notifications. Options: ``"failure"`` or ``"always"``.

        Returns
        -------
        bool
            True if notification was sent successfully or skipped by config, False otherwise.
        """
        import os

        url = webhook_url or os.environ.get("VALIDATEX_SLACK_WEBHOOK_URL")
        if not url:
            import sys

            sys.stderr.write(
                "ValidateX Slack Alert: No webhook URL provided and VALIDATEX_SLACK_WEBHOOK_URL not set.\n"
            )
            return False

        # Check notify_on condition
        if notify_on == "failure" and self.success:
            return True  # Skipped successfully since it passed

        from validatex.reporting.alerts import SlackNotifier

        return SlackNotifier.send(self, url)

    def send_teams(self, webhook_url: Optional[str] = None, notify_on: str = "failure") -> bool:
        """
        Send a validation alert to Microsoft Teams via incoming webhook.

        Parameters
        ----------
        webhook_url : str, optional
            The Microsoft Teams webhook URL. If not provided, will look for the
            VALIDATEX_TEAMS_WEBHOOK_URL environment variable.
        notify_on : str, default "failure"
            When to send notifications. Options: ``"failure"`` or ``"always"``.

        Returns
        -------
        bool
            True if notification was sent successfully or skipped by config, False otherwise.
        """
        import os

        url = webhook_url or os.environ.get("VALIDATEX_TEAMS_WEBHOOK_URL")
        if not url:
            import sys

            sys.stderr.write(
                "ValidateX Teams Alert: No webhook URL provided and VALIDATEX_TEAMS_WEBHOOK_URL not set.\n"
            )
            return False

        # Check notify_on condition
        if notify_on == "failure" and self.success:
            return True  # Skipped successfully since it passed

        from validatex.reporting.alerts import TeamsNotifier

        return TeamsNotifier.send(self, url)

    def summary(self) -> str:
        """Return a human-readable summary string."""
        self.compute_statistics()
        score = self.compute_quality_score()
        status = "✅ ALL PASSED" if self.success else "❌ SOME FAILED"
        cov_pct = self.column_coverage_percent
        cov_str = (
            f"{self.checked_columns}/{self.total_columns} columns ({cov_pct}%)"
            if cov_pct is not None
            else f"{self.checked_columns} columns checked"
        )
        lines = [
            f"{'='*60}",
            f"  ValidateX Validation Report — {self.suite_name}",
            f"{'='*60}",
            f"  Status           : {status}",
            f"  Quality Score    : {score} / 100 (configured checks only)",
            f"  Column Coverage  : {cov_str}",
            f"  Total Expectations: {self.total_expectations}",
            f"  Passed           : {self.successful_expectations}",
            f"  Failed           : {self.failed_expectations}",
            f"  Errors           : {self.errored_expectations}",
            f"  Success Rate     : {self.success_percent}%",
            f"  Run Duration     : {self.run_duration_seconds:.3f}s",
            f"  Engine           : {self.engine}",
            f"{'='*60}",
        ]
        if self.failed_expectations > 0 or self.errored_expectations > 0:
            lines.append("  Failed / Errored Expectations:")
            lines.append(f"  {'-'*56}")
            for r in self.results:
                if not r.success:
                    col_str = f" (column: {r.column})" if r.column else ""
                    sev = r.severity_icon
                    lines.append(f"  {r.status_icon} {sev} {r.expectation_type}{col_str}")
                    if r.exception_info:
                        lines.append(f"      Error: {r.exception_info}")
                    elif r.unexpected_count:
                        lines.append(f"      Unexpected: {r.unexpected_count} " f"({r.unexpected_percent:.2f}%)")
            lines.append(f"{'='*60}")
        return "\n".join(lines)

    def __repr__(self) -> str:
        return (
            f"ValidationResult(suite={self.suite_name!r}, "
            f"success={self.success}, "
            f"passed={self.successful_expectations}/{self.total_expectations})"
        )
