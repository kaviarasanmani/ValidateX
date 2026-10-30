"""
Expectation Suite — a named, ordered collection of expectations.

Suites can be built programmatically or loaded from YAML / JSON configs.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import yaml  # type: ignore

from validatex.core.expectation import Expectation, get_expectation_class


@dataclass
class ExpectationSuite:
    """
    A named collection of expectations.

    Examples
    --------
    >>> suite = ExpectationSuite("user_data_quality")
    >>> suite.add("expect_column_to_not_be_null", column="user_id")
    >>> suite.add("expect_column_values_to_be_between",
    ...           column="age", min_value=0, max_value=150)
    """

    name: str
    expectations: List[Expectation] = field(default_factory=list)
    meta: Dict[str, Any] = field(default_factory=dict)
    version: str = "1.0.0"
    owner: Optional[str] = None
    contract: Dict[str, Any] = field(default_factory=dict)

    def add(
        self,
        expectation_type: str,
        column: Optional[str] = None,
        meta: Optional[Dict[str, Any]] = None,
        owner: Optional[str] = None,
        **kwargs: Any,
    ) -> "ExpectationSuite":
        """
        Add an expectation to this suite.

        Parameters
        ----------
        expectation_type : str
            The registered name of the expectation
            (e.g. ``"expect_column_to_not_be_null"``).
        column : str, optional
            Target column name.
        meta : dict, optional
            Arbitrary metadata to attach.
        owner : str, optional
            Rule owner (person or team) for governance routing.
        **kwargs
            Additional arguments forwarded to the expectation
            (e.g. ``min_value``, ``regex``).

        Returns
        -------
        ExpectationSuite
            ``self`` for fluent chaining.
        """
        exp_cls = get_expectation_class(expectation_type)
        effective_meta = dict(meta or {})
        if owner is not None:
            effective_meta["owner"] = owner
        exp = exp_cls(column=column, kwargs=kwargs, meta=effective_meta)
        self.expectations.append(exp)
        return self

    def add_expectation(self, expectation: Expectation) -> "ExpectationSuite":
        """Add a pre-built Expectation instance."""
        self.expectations.append(expectation)
        return self

    def remove(self, index: int) -> "ExpectationSuite":
        """Remove an expectation by index."""
        if 0 <= index < len(self.expectations):
            self.expectations.pop(index)
        return self

    def clear(self) -> "ExpectationSuite":
        """Remove all expectations."""
        self.expectations.clear()
        return self

    # -- serialization -----------------------------------------------------

    def to_dict(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {
            "suite_name": self.name,
            "version": self.version,
            "meta": self.meta,
            "expectations": [e.to_dict() for e in self.expectations],
        }
        if self.owner:
            d["owner"] = self.owner
        if self.contract:
            d["contract"] = self.contract
        return d

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, default=str)

    def to_yaml(self) -> str:
        return str(yaml.dump(self.to_dict(), default_flow_style=False, sort_keys=False))

    def save(self, filepath: str) -> None:
        """Save to YAML or JSON based on file extension."""
        data = self.to_dict()
        with open(filepath, "w", encoding="utf-8") as f:
            if filepath.endswith((".yaml", ".yml")):
                yaml.dump(data, f, default_flow_style=False, sort_keys=False)
            else:
                json.dump(data, f, indent=2, default=str)

    @classmethod
    def load(cls, filepath: str) -> "ExpectationSuite":
        """Load from a YAML or JSON file."""
        with open(filepath, "r", encoding="utf-8") as f:
            if filepath.endswith((".yaml", ".yml")):
                data = yaml.safe_load(f)
            else:
                data = json.load(f)
        if not isinstance(data, dict):
            raise ValueError(f"Invalid suite configuration in {filepath!r}: expected a mapping.")
        return cls.from_dict(data)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ExpectationSuite":
        """Create a suite from a plain dictionary."""
        meta = data.get("meta", {}) or {}
        suite = cls(
            name=data.get("suite_name", "unnamed_suite"),
            meta=meta,
            version=str(data.get("version", meta.get("version", "1.0.0"))),
            owner=data.get("owner", meta.get("owner")),
            contract=data.get("contract", {}) or {},
        )
        for exp_data in data.get("expectations", []):
            exp = Expectation.from_dict(exp_data)
            suite.add_expectation(exp)
        return suite

    # -- dunder ------------------------------------------------------------

    def __len__(self) -> int:
        return len(self.expectations)

    def __iter__(self):
        return iter(self.expectations)

    def __repr__(self) -> str:
        return f"ExpectationSuite(name={self.name!r}, " f"expectations={len(self.expectations)})"
