"""
Data contract model.

A :class:`DataContract` is a reusable, version-controlled agreement between a data producer and its
consumers. It bundles the expected schema, business quality rules (each with a severity and an owner),
quality thresholds, and ownership metadata, and compiles down to a regular
:class:`~validatex.core.suite.ExpectationSuite` for execution on any engine.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml  # type: ignore

from validatex.core.exceptions import ValidateXError
from validatex.core.introspect import canonical_type
from validatex.core.suite import ExpectationSuite

VALID_SEVERITIES = ("critical", "warning", "info")
_NAME_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.\-]*$")
_VERSION_PATTERN = re.compile(r"^\d+(\.\d+){0,2}$")


class ContractError(ValidateXError):
    """Raised when a data contract is malformed, invalid, or violates registry rules."""


# ---------------------------------------------------------------------------
# Versions
# ---------------------------------------------------------------------------


def parse_version(value: Any) -> Tuple[int, int, int]:
    """Parse ``1``, ``1.2`` or ``1.2.3`` (string or number) into a ``(major, minor, patch)`` tuple."""
    text = str(value).strip().lstrip("vV")
    if not _VERSION_PATTERN.match(text):
        raise ContractError(f"Invalid contract version {value!r}: expected MAJOR[.MINOR[.PATCH]], e.g. '1.2.0'.")
    parts = [int(p) for p in text.split(".")]
    while len(parts) < 3:
        parts.append(0)
    return parts[0], parts[1], parts[2]


def normalize_version(value: Any) -> str:
    """Return the canonical ``MAJOR.MINOR.PATCH`` form of *value*."""
    return ".".join(str(p) for p in parse_version(value))


# ---------------------------------------------------------------------------
# Building blocks
# ---------------------------------------------------------------------------


@dataclass
class ContractOwner:
    """Who is accountable for a contract."""

    team: Optional[str] = None
    steward: Optional[str] = None
    contact: Optional[str] = None

    @classmethod
    def from_value(cls, value: Any) -> "ContractOwner":
        if value is None:
            return cls()
        if isinstance(value, str):
            return cls(team=value)
        if isinstance(value, dict):
            unknown = set(value) - {"team", "steward", "contact"}
            if unknown:
                raise ContractError(f"Unknown owner field(s): {sorted(unknown)}")
            return cls(team=value.get("team"), steward=value.get("steward"), contact=value.get("contact"))
        raise ContractError(f"Invalid owner definition: {value!r}")

    def to_dict(self) -> Dict[str, Any]:
        return {k: v for k, v in (("team", self.team), ("steward", self.steward), ("contact", self.contact)) if v}

    from_dict = from_value

    def is_empty(self) -> bool:
        return not (self.team or self.steward or self.contact)


@dataclass
class SchemaField:
    """A column promised by the contract."""

    name: str
    type: str = "any"
    nullable: bool = True
    description: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {"name": self.name, "type": self.type, "nullable": self.nullable}
        if self.description:
            d["description"] = self.description
        return d


_RULE_KEYS = {"expectation", "expectation_type", "column", "severity", "owner", "id", "description", "kwargs"}


@dataclass
class QualityRule:
    """A business rule: an expectation plus severity and ownership."""

    expectation: str
    column: Optional[str] = None
    severity: str = "warning"
    owner: Optional[str] = None
    id: Optional[str] = None
    description: Optional[str] = None
    kwargs: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "QualityRule":
        if not isinstance(data, dict):
            raise ContractError(f"Invalid quality rule (expected a mapping): {data!r}")
        expectation = data.get("expectation") or data.get("expectation_type")
        if not expectation:
            raise ContractError(f"Quality rule is missing 'expectation': {data!r}")
        kwargs = dict(data.get("kwargs") or {})
        # Any non-reserved key is treated as an expectation argument (flat YAML style).
        for key, value in data.items():
            if key not in _RULE_KEYS:
                kwargs[key] = value
        return cls(
            expectation=str(expectation),
            column=data.get("column"),
            severity=str(data.get("severity", "warning")).lower(),
            owner=data.get("owner"),
            id=data.get("id"),
            description=data.get("description"),
            kwargs=kwargs,
        )

    def to_dict(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {"expectation": self.expectation}
        if self.id:
            d["id"] = self.id
        if self.column is not None:
            d["column"] = self.column
        d["severity"] = self.severity
        if self.owner:
            d["owner"] = self.owner
        if self.description:
            d["description"] = self.description
        if self.kwargs:
            d["kwargs"] = self.kwargs
        return d


@dataclass
class Thresholds:
    """Pass/fail criteria for a contract run."""

    minimum_quality_score: Optional[float] = None
    max_critical_failures: int = 0
    max_warning_failures: Optional[int] = None

    @classmethod
    def from_dict(cls, data: Optional[Dict[str, Any]]) -> "Thresholds":
        data = data or {}
        unknown = set(data) - {"minimum_quality_score", "max_critical_failures", "max_warning_failures"}
        if unknown:
            raise ContractError(f"Unknown threshold(s): {sorted(unknown)}")
        mqs = data.get("minimum_quality_score")
        mcf = data.get("max_critical_failures", 0)
        mwf = data.get("max_warning_failures")
        return cls(
            minimum_quality_score=float(mqs) if mqs is not None else None,
            max_critical_failures=int(mcf),
            max_warning_failures=int(mwf) if mwf is not None else None,
        )

    def to_dict(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {}
        if self.minimum_quality_score is not None:
            d["minimum_quality_score"] = self.minimum_quality_score
        d["max_critical_failures"] = self.max_critical_failures
        if self.max_warning_failures is not None:
            d["max_warning_failures"] = self.max_warning_failures
        return d

    def is_empty(self) -> bool:
        return (
            self.minimum_quality_score is None and self.max_critical_failures == 0 and self.max_warning_failures is None
        )


# ---------------------------------------------------------------------------
# DataContract
# ---------------------------------------------------------------------------


@dataclass
class DataContract:
    """
    A versioned data contract.

    Examples
    --------
    >>> contract = DataContract.from_yaml_file("contracts/customer_master.yaml")
    >>> result = vx.validate_contract(df, contract)
    >>> result.success
    True
    """

    name: str
    version: str = "1.0.0"
    owner: ContractOwner = field(default_factory=ContractOwner)
    description: Optional[str] = None
    schema: List[SchemaField] = field(default_factory=list)
    quality_rules: List[QualityRule] = field(default_factory=list)
    thresholds: Thresholds = field(default_factory=Thresholds)
    strict_schema: bool = False
    status: str = "active"
    meta: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.version = normalize_version(self.version)

    # -- creation ----------------------------------------------------------

    @classmethod
    def create(cls, name: str, version: Any = "1.0.0", **kwargs: Any) -> "DataContract":
        """Create an empty contract to be filled with :meth:`add_field` / :meth:`add_rule`."""
        owner = kwargs.pop("owner", None)
        team = kwargs.pop("team", None)
        steward = kwargs.pop("steward", None)
        contact = kwargs.pop("contact", None)
        owner_obj = ContractOwner.from_value(owner)
        if team is not None:
            owner_obj.team = team
        if steward is not None:
            owner_obj.steward = steward
        if contact is not None:
            owner_obj.contact = contact
        thresholds = kwargs.pop("thresholds", None)
        mqs = kwargs.pop("minimum_quality_score", None)
        mcf = kwargs.pop("max_critical_failures", None)
        mwf = kwargs.pop("max_warning_failures", None)
        thresh_obj = Thresholds.from_dict(thresholds) if isinstance(thresholds, dict) else (thresholds or Thresholds())
        if mqs is not None:
            thresh_obj.minimum_quality_score = float(mqs)
        if mcf is not None:
            thresh_obj.max_critical_failures = int(mcf)
        if mwf is not None:
            thresh_obj.max_warning_failures = int(mwf)
        return cls(name=name, version=normalize_version(version), owner=owner_obj, thresholds=thresh_obj, **kwargs)

    def add_field(
        self, name: str, type: str = "any", nullable: bool = True, description: Optional[str] = None
    ) -> "DataContract":
        self.schema.append(SchemaField(name=name, type=str(type).lower(), nullable=nullable, description=description))
        return self

    def add_rule(
        self,
        expectation: str,
        column: Optional[str] = None,
        severity: str = "warning",
        owner: Optional[str] = None,
        id: Optional[str] = None,
        description: Optional[str] = None,
        **kwargs: Any,
    ) -> "DataContract":
        self.quality_rules.append(
            QualityRule(
                expectation=expectation,
                column=column,
                severity=severity.lower(),
                owner=owner,
                id=id,
                description=description,
                kwargs=kwargs,
            )
        )
        return self

    # -- (de)serialisation -------------------------------------------------

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DataContract":
        if not isinstance(data, dict):
            raise ContractError("A data contract must be a mapping.")
        body = data.get("contract", data)
        if not isinstance(body, dict):
            raise ContractError("The 'contract' key must contain a mapping.")
        if not body.get("name"):
            raise ContractError("Contract is missing required field 'name'.")
        known = {
            "name",
            "version",
            "owner",
            "description",
            "schema",
            "quality_rules",
            "thresholds",
            "strict_schema",
            "status",
            "meta",
        }
        unknown = set(body) - known
        if unknown:
            raise ContractError(f"Unknown contract field(s): {sorted(unknown)}")

        schema_fields = []
        for item in body.get("schema") or []:
            if not isinstance(item, dict) or "name" not in item:
                raise ContractError(f"Invalid schema field (needs a 'name'): {item!r}")
            schema_fields.append(
                SchemaField(
                    name=str(item["name"]),
                    type=str(item.get("type", "any")).lower(),
                    nullable=bool(item.get("nullable", True)),
                    description=item.get("description"),
                )
            )
        return cls(
            name=str(body["name"]),
            version=normalize_version(body.get("version", "1.0.0")),
            owner=ContractOwner.from_value(body.get("owner")),
            description=body.get("description"),
            schema=schema_fields,
            quality_rules=[QualityRule.from_dict(r) for r in body.get("quality_rules") or []],
            thresholds=Thresholds.from_dict(body.get("thresholds")),
            strict_schema=bool(body.get("strict_schema", False)),
            status=str(body.get("status", "active")),
            meta=dict(body.get("meta") or {}),
        )

    def to_dict(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {"name": self.name, "version": self.version}
        if self.status != "active":
            d["status"] = self.status
        if self.description:
            d["description"] = self.description
        owner = self.owner.to_dict()
        if owner:
            d["owner"] = owner
        if self.strict_schema:
            d["strict_schema"] = True
        d["schema"] = [f.to_dict() for f in self.schema]
        d["quality_rules"] = [r.to_dict() for r in self.quality_rules]
        d["thresholds"] = self.thresholds.to_dict()
        if self.meta:
            d["meta"] = self.meta
        return d

    def to_yaml(self) -> str:
        return str(yaml.safe_dump({"contract": self.to_dict()}, default_flow_style=False, sort_keys=False))

    def to_json(self, indent: int = 2) -> str:
        return json.dumps({"contract": self.to_dict()}, indent=indent, default=str)

    @classmethod
    def from_yaml(cls, text: str) -> "DataContract":
        try:
            data = yaml.safe_load(text)
        except yaml.YAMLError as exc:
            raise ContractError(f"Contract is not valid YAML: {exc}") from exc
        return cls.from_dict(data)

    @classmethod
    def from_yaml_file(cls, path: Any) -> "DataContract":
        return cls.load(path)

    @classmethod
    def load(cls, path: Any) -> "DataContract":
        """Load a contract from a YAML or JSON file."""
        p = Path(path)
        if not p.is_file():
            raise ContractError(f"Contract file not found: {p}")
        text = p.read_text(encoding="utf-8")
        if p.suffix.lower() == ".json":
            try:
                return cls.from_dict(json.loads(text))
            except json.JSONDecodeError as exc:
                raise ContractError(f"Contract is not valid JSON: {exc}") from exc
        return cls.from_yaml(text)

    def save(self, path: Any) -> Path:
        """Write the contract to a YAML (default) or JSON file."""
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(self.to_json() if p.suffix.lower() == ".json" else self.to_yaml(), encoding="utf-8")
        return p

    # -- identity ----------------------------------------------------------

    @property
    def content_hash(self) -> str:
        """Short stable hash of the contract content (used to detect edits to a published version)."""
        canonical = json.dumps(self.to_dict(), sort_keys=True, default=str)
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:12]

    def rule_ids(self) -> List[str]:
        """Stable, unique identifier for each quality rule (explicit ``id`` or ``expectation:column``)."""
        seen: Dict[str, int] = {}
        ids: List[str] = []
        for rule in self.quality_rules:
            base = rule.id or f"{rule.expectation}:{rule.column or '*'}"
            seen[base] = seen.get(base, 0) + 1
            ids.append(base if seen[base] == 1 else f"{base}#{seen[base]}")
        return ids

    def get_field(self, name: str) -> Optional[SchemaField]:
        for f in self.schema:
            if f.name == name:
                return f
        return None

    # -- validation of the definition itself -------------------------------

    def validate_definition(self) -> List[str]:
        """Return a list of problems with this contract definition (empty when valid)."""
        from validatex.core.expectation import get_expectation_class

        problems: List[str] = []
        if not _NAME_PATTERN.match(self.name or ""):
            problems.append(f"Invalid contract name {self.name!r}: use letters, digits, '_', '-', '.'.")
        names = [f.name for f in self.schema]
        for dup in sorted({n for n in names if names.count(n) > 1}):
            problems.append(f"Duplicate schema field '{dup}'.")
        for f in self.schema:
            if not f.name.strip():
                problems.append("Schema field with an empty name.")
            if canonical_type(f.type) is None:
                problems.append(f"Field '{f.name}' has unknown type '{f.type}'.")
        for rid, rule in zip(self.rule_ids(), self.quality_rules):
            try:
                get_expectation_class(rule.expectation)
            except ValueError:
                problems.append(f"Rule '{rid}' uses unknown expectation '{rule.expectation}'.")
            if rule.severity not in VALID_SEVERITIES:
                problems.append(f"Rule '{rid}' has invalid severity '{rule.severity}' (use {VALID_SEVERITIES}).")
            if self.schema and rule.column is not None and rule.column not in names:
                problems.append(f"Rule '{rid}' references column '{rule.column}' that is not in the schema.")
        score = self.thresholds.minimum_quality_score
        if score is not None and not 0 <= score <= 100:
            problems.append(f"minimum_quality_score must be between 0 and 100 (got {score}).")
        if self.thresholds.max_critical_failures < 0:
            problems.append("max_critical_failures must be >= 0.")
        return problems

    def ensure_valid(self) -> "DataContract":
        """Raise :class:`ContractError` if the definition has problems."""
        problems = self.validate_definition()
        if problems:
            raise ContractError(f"Invalid contract '{self.name}' v{self.version}:\n  - " + "\n  - ".join(problems))
        return self

    # -- compilation -------------------------------------------------------

    def to_suite(self) -> ExpectationSuite:
        """
        Compile the contract into an :class:`ExpectationSuite`.

        Non-nullable schema fields become critical ``expect_column_to_not_be_null`` checks; every quality rule
        becomes an expectation carrying its severity, owner and rule id in ``meta``. Column existence and type
        checks are performed separately by the contract validator, which can introspect the dataset schema.
        """
        team = self.owner.team
        suite = ExpectationSuite(
            self.name,
            version=self.version,
            owner=team,
            meta={"contract": self.name, "contract_version": self.version, "contract_hash": self.content_hash},
        )
        suite.contract = {"name": self.name, "version": self.version}
        for f in self.schema:
            if not f.nullable:
                suite.add(
                    "expect_column_to_not_be_null",
                    column=f.name,
                    meta={
                        "severity": "critical",
                        "owner": team,
                        "rule_id": f"schema.{f.name}.not_null",
                        "source": "schema",
                    },
                )
        for rid, rule in zip(self.rule_ids(), self.quality_rules):
            meta: Dict[str, Any] = {
                "severity": rule.severity,
                "owner": rule.owner or team,
                "rule_id": rid,
                "source": "rule",
            }
            if rule.description:
                meta["description"] = rule.description
            suite.add(rule.expectation, column=rule.column, meta=meta, **rule.kwargs)
        return suite

    def __repr__(self) -> str:
        return (
            f"DataContract(name={self.name!r}, version={self.version!r}, "
            f"fields={len(self.schema)}, rules={len(self.quality_rules)})"
        )
