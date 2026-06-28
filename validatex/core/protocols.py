"""
ValidateX Core Protocols — Structural Subtyping.

Defines static type interfaces for DataFrames and Engines without forcing runtime imports.
"""

from __future__ import annotations

from typing import Any, List, Protocol, runtime_checkable


@runtime_checkable
class DataFrameLike(Protocol):
    """Structural protocol matching Pandas, Polars, and PySpark DataFrames."""

    @property
    def columns(self) -> Any:
        ...

    def __getitem__(self, item: Any) -> Any:
        ...


@runtime_checkable
class ValidationEngineProtocol(Protocol):
    """Structural protocol matching ValidateX validator execution implementations."""

    def run_suite(self, df: Any, suite: Any) -> Any:
        ...
