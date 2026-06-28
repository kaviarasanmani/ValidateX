"""
ValidateX Domain Exceptions Hierarchy.

Wraps raw third-party errors (SQLAlchemy / PySpark JVM traces / I/O failures)
into clean, user-friendly domain exceptions.
"""

from __future__ import annotations


class ValidateXError(Exception):
    """Base exception for all ValidateX errors."""
    pass


class ValidateXEngineError(ValidateXError):
    """Raised when an execution engine (Pandas, Polars, PySpark, SQL) encounters a runtime failure."""

    def __init__(self, message: str, original_exception: Exception | None = None):
        super().__init__(message)
        self.original_exception = original_exception


class SuiteConfigurationError(ValidateXError):
    """Raised when an Expectation Suite or JSON/YAML configuration is invalid."""
    pass


class DataLoadError(ValidateXError):
    """Raised when a data file or database source cannot be loaded."""
    pass
