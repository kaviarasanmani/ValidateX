"""
Expectation base classes and registry.

This module defines the base Expectation class and the global registry
that maps expectation type names to their implementation classes.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Type

from validatex.core.result import ExpectationResult

# ---------------------------------------------------------------------------
# Global Expectation Registry
# ---------------------------------------------------------------------------

_EXPECTATION_REGISTRY: Dict[str, Type["Expectation"]] = {}


def register_expectation(cls: Type["Expectation"]) -> Type["Expectation"]:
    """Decorator that registers an expectation class by its *type_name*."""
    # expectation_type is a dataclass field with init=False and a default value.
    # We must read the default from __dataclass_fields__ rather than getattr(),
    # because getattr returns the Field descriptor object (truthy) instead of
    # the actual default string on uninstantiated classes.
    fields = getattr(cls, "__dataclass_fields__", {})
    et_field = fields.get("expectation_type")
    if et_field is not None:
        name = et_field.default
    else:
        name = getattr(cls, "expectation_type", None) or cls.__name__
    _EXPECTATION_REGISTRY[name] = cls
    return cls


def get_expectation_class(name: str) -> Type["Expectation"]:
    """Look up an expectation class by its registered type name."""
    if name not in _EXPECTATION_REGISTRY:
        available = ", ".join(sorted(_EXPECTATION_REGISTRY.keys()))
        raise ValueError(f"Unknown expectation type '{name}'. " f"Available types: {available}")
    return _EXPECTATION_REGISTRY[name]


def list_expectations() -> List[str]:
    """Return a sorted list of all registered expectation type names."""
    return sorted(_EXPECTATION_REGISTRY.keys())


# ---------------------------------------------------------------------------
# Base Expectation
# ---------------------------------------------------------------------------


@dataclass
class Expectation(ABC):
    """
    Abstract base class for all expectations.

    Subclasses must:
      1. Set the class attribute ``expectation_type`` (a unique string id).
      2. Implement :meth:`_validate_pandas` and/or :meth:`_validate_spark`.
    """

    expectation_type: str = field(init=False, default="base_expectation")
    column: Optional[str] = None
    kwargs: Dict[str, Any] = field(default_factory=dict)
    meta: Dict[str, Any] = field(default_factory=dict)

    # -- public API --------------------------------------------------------

    def validate(self, data: Any, engine: str = "pandas") -> ExpectationResult:
        """
        Run this expectation against *data* using the specified engine.

        Parameters
        ----------
        data : Any
            The dataset (pd.DataFrame or pyspark.sql.DataFrame).
        engine : str
            ``"pandas"`` or ``"spark"``.

        Returns
        -------
        ExpectationResult
        """
        try:
            if engine == "pandas":
                return self._validate_pandas(data)
            elif engine == "spark":
                return self._validate_spark(data)
            elif engine == "sql":
                return self._validate_sql(data)
            else:
                raise ValueError(f"Unsupported engine: {engine}")
        except Exception as exc:
            return ExpectationResult(
                expectation_type=self.expectation_type,
                success=False,
                column=self.column,
                details={"error": str(exc)},
                exception_info=str(exc),
            )

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to a plain dictionary (for YAML / JSON configs)."""
        d: Dict[str, Any] = {
            "expectation_type": self.expectation_type,
        }
        if self.column is not None:
            d["column"] = self.column
        if self.kwargs:
            d["kwargs"] = self.kwargs
        if self.meta:
            d["meta"] = self.meta
        return d

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "Expectation":
        """Deserialize from a dictionary."""
        exp_type = d["expectation_type"]
        exp_cls = get_expectation_class(exp_type)
        column = d.get("column")
        kwargs = d.get("kwargs", {})
        meta = d.get("meta", {})
        instance = exp_cls(column=column, kwargs=kwargs, meta=meta)
        return instance

    # -- hooks for subclasses ----------------------------------------------

    @abstractmethod
    def _validate_pandas(self, df: Any) -> ExpectationResult:
        """Validate using Pandas engine. Must be implemented by subclasses."""
        ...

    def _validate_spark(self, df: Any) -> ExpectationResult:
        """Validate using PySpark engine.  Optional override."""
        raise NotImplementedError(f"{self.expectation_type} does not support PySpark yet.")

    def _validate_sql(self, engine: Any) -> ExpectationResult:
        """Validate using SQLAlchemy engine. Optional override."""
        raise NotImplementedError(f"{self.expectation_type} does not support SQL yet.")

    # -- helpers -----------------------------------------------------------

    def _build_result(
        self,
        success: bool,
        observed_value: Any = None,
        element_count: int = 0,
        unexpected_count: int = 0,
        unexpected_percent: float = 0.0,
        unexpected_values: Optional[List[Any]] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> ExpectationResult:
        """Convenience builder for :class:`ExpectationResult`."""
        return ExpectationResult(
            expectation_type=self.expectation_type,
            success=success,
            column=self.column,
            observed_value=observed_value,
            element_count=element_count,
            unexpected_count=unexpected_count,
            unexpected_percent=unexpected_percent,
            unexpected_values=unexpected_values or [],
            details=details or {},
            meta=self.meta,
        )

    def __repr__(self) -> str:
        parts = [f"type={self.expectation_type!r}"]
        if self.column:
            parts.append(f"column={self.column!r}")
        if self.kwargs:
            parts.append(f"kwargs={self.kwargs!r}")
        return f"Expectation({', '.join(parts)})"
