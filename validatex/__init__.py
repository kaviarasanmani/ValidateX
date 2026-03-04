"""
ValidateX - A powerful data quality validation framework.

ValidateX provides a comprehensive suite of tools for validating,
profiling, and monitoring data quality across Pandas and PySpark DataFrames.

Usage:
    >>> import validatex as vx
    >>> suite = vx.ExpectationSuite("my_suite")
    >>> suite.add("expect_column_to_not_be_null", column="user_id")
    >>> suite.add("expect_column_values_to_be_between", column="age", min_value=0, max_value=150)
    >>> result = vx.validate(df, suite)
    >>> result.to_html("report.html")
"""

__version__ = "1.1.0"
__author__ = "ValidateX Team"

# Register all built-in expectations on import
import validatex.expectations  # noqa: F401

from validatex.core.expectation import Expectation
from validatex.core.suite import ExpectationSuite
from validatex.core.result import (
    ValidationResult,
    ExpectationResult,
    ColumnHealthSummary,
    SEVERITY_CRITICAL,
    SEVERITY_WARNING,
    SEVERITY_INFO,
)
from validatex.core.validator import Validator, validate
from validatex.profiler.profiler import DataProfiler
from validatex.drift.detector import DriftDetector
from validatex.drift.report import DriftReport

__all__ = [
    "Expectation",
    "ExpectationSuite",
    "ValidationResult",
    "ExpectationResult",
    "ColumnHealthSummary",
    "Validator",
    "validate",
    "DataProfiler",
    "SEVERITY_CRITICAL",
    "SEVERITY_WARNING",
    "SEVERITY_INFO",
    "DriftDetector",
    "DriftReport",
]
