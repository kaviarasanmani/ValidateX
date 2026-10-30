"""
Contract comparison and breaking-change detection.

Changes are classified from the **consumer's** point of view: a change is *breaking* when something a
consumer was entitled to rely on is removed or weakened (a column disappears, a type changes incompatibly,
a column becomes nullable, a rule is dropped or downgraded, a threshold is relaxed). Tightening a guarantee or
adding a column is *non-breaking*; documentation and ownership edits are *informational*.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from validatex.contracts.model import DataContract, QualityRule, parse_version
from validatex.core.introspect import canonical_type

BREAKING = "breaking"
NON_BREAKING = "non_breaking"
INFO = "info"

_SEVERITY_RANK = {"critical": 3, "warning": 2, "info": 1}
_BUMP_RANK = {"none": 0, "patch": 1, "minor": 2, "major": 3}
_WIDENINGS = {("integer", "float"), ("date", "datetime")}


@dataclass
class Change:
    """A single difference between two contract versions."""

    category: str  # schema | rule | threshold | metadata
    kind: str  # e.g. field_removed, type_changed, rule_added
    impact: str  # breaking | non_breaking | info
    subject: str
    message: str
    old: Any = None
    new: Any = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "category": self.category,
            "kind": self.kind,
            "impact": self.impact,
            "subject": self.subject,
            "message": self.message,
            "old": self.old,
            "new": self.new,
        }


@dataclass
class ContractDiff:
    """The full comparison between an old and a new contract version."""

    contract_name: str
    old_version: str
    new_version: str
    changes: List[Change] = field(default_factory=list)

    @property
    def breaking_changes(self) -> List[Change]:
        return [c for c in self.changes if c.impact == BREAKING]

    @property
    def non_breaking_changes(self) -> List[Change]:
        return [c for c in self.changes if c.impact == NON_BREAKING]

    @property
    def has_breaking_changes(self) -> bool:
        return bool(self.breaking_changes)

    @property
    def is_identical(self) -> bool:
        return not self.changes

    @property
    def required_bump(self) -> str:
        """Minimum semantic-version bump the changes demand: ``major``, ``minor``, ``patch`` or ``none``."""
        if self.breaking_changes:
            return "major"
        if self.non_breaking_changes:
            return "minor"
        if self.changes:
            return "patch"
        return "none"

    @property
    def actual_bump(self) -> str:
        """The version bump that was actually applied between old and new."""
        return version_bump(self.old_version, self.new_version)

    @property
    def version_ok(self) -> bool:
        """True when the applied version bump is at least as large as the changes require."""
        return _BUMP_RANK[self.actual_bump] >= _BUMP_RANK[self.required_bump]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "contract": self.contract_name,
            "old_version": self.old_version,
            "new_version": self.new_version,
            "breaking": self.has_breaking_changes,
            "required_bump": self.required_bump,
            "actual_bump": self.actual_bump,
            "version_ok": self.version_ok,
            "changes": [c.to_dict() for c in self.changes],
        }

    def summary(self) -> str:
        lines = [
            f"Contract '{self.contract_name}': {self.old_version} -> {self.new_version}",
            f"  Breaking: {len(self.breaking_changes)}  Non-breaking: {len(self.non_breaking_changes)}  "
            f"Info: {len(self.changes) - len(self.breaking_changes) - len(self.non_breaking_changes)}",
            f"  Required version bump: {self.required_bump} (applied: {self.actual_bump})"
            + ("" if self.version_ok else "  <-- VERSION BUMP TOO SMALL"),
        ]
        for label, impact in (("BREAKING", BREAKING), ("non-breaking", NON_BREAKING), ("info", INFO)):
            for c in self.changes:
                if c.impact == impact:
                    lines.append(f"  [{label}] {c.message}")
        return "\n".join(lines)

    def to_markdown(self) -> str:
        lines = [
            f"### Contract `{self.contract_name}`: {self.old_version} → {self.new_version}",
            "",
            f"Required bump: **{self.required_bump}** (applied: **{self.actual_bump}**)",
            "",
            "| Impact | Category | Change |",
            "|---|---|---|",
        ]
        for c in self.changes:
            lines.append(f"| {c.impact} | {c.category} | {c.message} |")
        if not self.changes:
            lines.append("| – | – | No differences |")
        return "\n".join(lines)


def version_bump(old: Any, new: Any) -> str:
    """Classify the step from *old* to *new*: ``major``, ``minor``, ``patch`` or ``none`` (equal/regression)."""
    o, n = parse_version(old), parse_version(new)
    if n[0] > o[0]:
        return "major"
    if n[0] == o[0] and n[1] > o[1]:
        return "minor"
    if n[:2] == o[:2] and n[2] > o[2]:
        return "patch"
    return "none"


# ---------------------------------------------------------------------------
# Classification helpers
# ---------------------------------------------------------------------------


def _as_number(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _bound_change(key: str, old: Any, new: Any) -> str:
    """Classify a change to a lower (``min_*``) or upper (``max_*``) bound."""
    is_lower = key.startswith("min")
    if old is None and new is not None:
        return NON_BREAKING  # a bound was introduced: stricter
    if old is not None and new is None:
        return BREAKING  # a bound was removed: looser
    o, n = _as_number(old), _as_number(new)
    if o is None or n is None or o == n:
        return BREAKING
    tighter = n > o if is_lower else n < o
    return NON_BREAKING if tighter else BREAKING


def _set_change(expectation: str, old: Any, new: Any) -> str:
    try:
        o, n = set(old or []), set(new or [])
    except TypeError:
        return BREAKING
    if o == n:
        return INFO
    forbids = "not_be_in_set" in expectation
    if forbids:  # more forbidden values means a stricter rule
        return NON_BREAKING if o <= n else BREAKING
    return NON_BREAKING if n <= o else BREAKING  # fewer allowed values means stricter


def _classify_kwarg_change(expectation: str, key: str, old: Any, new: Any) -> str:
    if key in ("min_value", "max_value", "min", "max"):
        return _bound_change(key, old, new)
    if key == "value_set":
        return _set_change(expectation, old, new)
    if key in ("threshold", "max_pct"):  # "must be less than X" style limits
        o, n = _as_number(old), _as_number(new)
        if o is None or n is None or o == n:
            return BREAKING
        return NON_BREAKING if n < o else BREAKING
    return BREAKING  # cannot prove the new argument is at least as strict


# ---------------------------------------------------------------------------
# compare_contracts
# ---------------------------------------------------------------------------


def compare_contracts(old: DataContract, new: DataContract) -> ContractDiff:
    """Compare two versions of a contract and classify every difference."""
    diff = ContractDiff(contract_name=new.name, old_version=old.version, new_version=new.version)
    add = diff.changes.append

    if old.name != new.name:
        add(
            Change(
                "metadata",
                "name_changed",
                BREAKING,
                "name",
                f"Contract renamed '{old.name}' -> '{new.name}' (consumers reference the old name)",
                old.name,
                new.name,
            )
        )

    _compare_schema(old, new, add)
    _compare_rules(old, new, add)
    _compare_thresholds(old, new, add)
    _compare_metadata(old, new, add)
    return diff


def _compare_schema(old: DataContract, new: DataContract, add: Any) -> None:
    old_fields = {f.name: f for f in old.schema}
    new_fields = {f.name: f for f in new.schema}

    for name, f in old_fields.items():
        if name not in new_fields:
            add(Change("schema", "field_removed", BREAKING, name, f"Column '{name}' was removed", f.to_dict(), None))
    for name, f in new_fields.items():
        if name not in old_fields:
            add(
                Change(
                    "schema",
                    "field_added",
                    NON_BREAKING,
                    name,
                    f"Column '{name}' ({f.type}{'' if f.nullable else ', required'}) was added",
                    None,
                    f.to_dict(),
                )
            )

    for name in old_fields.keys() & new_fields.keys():
        o, n = old_fields[name], new_fields[name]
        ot, nt = canonical_type(o.type) or o.type, canonical_type(n.type) or n.type
        if ot != nt:
            impact = NON_BREAKING if (ot, nt) in _WIDENINGS or ot == "any" else BREAKING
            if nt == "any":
                impact = BREAKING  # consumers lose the type guarantee
            note = "widened" if impact == NON_BREAKING else "changed incompatibly"
            add(Change("schema", "type_changed", impact, name, f"Column '{name}' type {note}: {ot} -> {nt}", ot, nt))
        if o.nullable != n.nullable:
            if n.nullable:
                add(
                    Change(
                        "schema",
                        "became_nullable",
                        BREAKING,
                        name,
                        f"Column '{name}' may now contain nulls (was NOT NULL)",
                        False,
                        True,
                    )
                )
            else:
                add(
                    Change(
                        "schema",
                        "became_required",
                        NON_BREAKING,
                        name,
                        f"Column '{name}' is now NOT NULL (was nullable)",
                        True,
                        False,
                    )
                )
        if o.description != n.description:
            add(Change("schema", "description_changed", INFO, name, f"Column '{name}' description changed"))

    if old.strict_schema != new.strict_schema:
        add(
            Change(
                "schema",
                "strict_schema_changed",
                NON_BREAKING if new.strict_schema else INFO,
                "strict_schema",
                f"strict_schema {old.strict_schema} -> {new.strict_schema}",
                old.strict_schema,
                new.strict_schema,
            )
        )


def _rule_map(contract: DataContract) -> Dict[str, QualityRule]:
    return dict(zip(contract.rule_ids(), contract.quality_rules))


def _compare_rules(old: DataContract, new: DataContract, add: Any) -> None:
    old_rules, new_rules = _rule_map(old), _rule_map(new)

    for rid, r in old_rules.items():
        if rid not in new_rules:
            add(
                Change(
                    "rule", "rule_removed", BREAKING, rid, f"Rule '{rid}' was removed (guarantee dropped)", r.to_dict()
                )
            )
    for rid, r in new_rules.items():
        if rid not in old_rules:
            add(
                Change(
                    "rule",
                    "rule_added",
                    NON_BREAKING,
                    rid,
                    f"Rule '{rid}' was added ({r.severity})",
                    None,
                    r.to_dict(),
                )
            )

    for rid in old_rules.keys() & new_rules.keys():
        o, n = old_rules[rid], new_rules[rid]
        if o.severity != n.severity:
            down = _SEVERITY_RANK.get(n.severity, 0) < _SEVERITY_RANK.get(o.severity, 0)
            add(
                Change(
                    "rule",
                    "severity_downgraded" if down else "severity_upgraded",
                    BREAKING if down else NON_BREAKING,
                    rid,
                    f"Rule '{rid}' severity {o.severity} -> {n.severity}",
                    o.severity,
                    n.severity,
                )
            )
        for key in sorted(set(o.kwargs) | set(n.kwargs)):
            ov, nv = o.kwargs.get(key), n.kwargs.get(key)
            if ov == nv:
                continue
            impact = _classify_kwarg_change(n.expectation, key, ov, nv)
            word = {BREAKING: "weakened or unverifiable", NON_BREAKING: "tightened", INFO: "reordered"}[impact]
            add(
                Change(
                    "rule",
                    "rule_argument_changed",
                    impact,
                    f"{rid}.{key}",
                    f"Rule '{rid}' argument '{key}' {ov!r} -> {nv!r} ({word})",
                    ov,
                    nv,
                )
            )
        if o.owner != n.owner:
            add(
                Change(
                    "rule",
                    "rule_owner_changed",
                    INFO,
                    rid,
                    f"Rule '{rid}' owner {o.owner!r} -> {n.owner!r}",
                    o.owner,
                    n.owner,
                )
            )
        if o.description != n.description:
            add(Change("rule", "rule_description_changed", INFO, rid, f"Rule '{rid}' description changed"))


def _compare_thresholds(old: DataContract, new: DataContract, add: Any) -> None:
    o, n = old.thresholds, new.thresholds

    if o.minimum_quality_score != n.minimum_quality_score:
        if n.minimum_quality_score is None:
            impact = BREAKING
        elif o.minimum_quality_score is None:
            impact = NON_BREAKING
        else:
            impact = NON_BREAKING if n.minimum_quality_score > o.minimum_quality_score else BREAKING
        add(
            Change(
                "threshold",
                "minimum_quality_score_changed",
                impact,
                "minimum_quality_score",
                f"minimum_quality_score {o.minimum_quality_score} -> {n.minimum_quality_score}",
                o.minimum_quality_score,
                n.minimum_quality_score,
            )
        )
    if o.max_critical_failures != n.max_critical_failures:
        add(
            Change(
                "threshold",
                "max_critical_failures_changed",
                BREAKING if n.max_critical_failures > o.max_critical_failures else NON_BREAKING,
                "max_critical_failures",
                f"max_critical_failures {o.max_critical_failures} -> {n.max_critical_failures}",
                o.max_critical_failures,
                n.max_critical_failures,
            )
        )
    if o.max_warning_failures != n.max_warning_failures:
        if n.max_warning_failures is None:
            impact = BREAKING
        elif o.max_warning_failures is None:
            impact = NON_BREAKING
        else:
            impact = BREAKING if n.max_warning_failures > o.max_warning_failures else NON_BREAKING
        add(
            Change(
                "threshold",
                "max_warning_failures_changed",
                impact,
                "max_warning_failures",
                f"max_warning_failures {o.max_warning_failures} -> {n.max_warning_failures}",
                o.max_warning_failures,
                n.max_warning_failures,
            )
        )


def _compare_metadata(old: DataContract, new: DataContract, add: Any) -> None:
    if old.owner.to_dict() != new.owner.to_dict():
        add(
            Change(
                "metadata",
                "owner_changed",
                INFO,
                "owner",
                f"Owner {old.owner.to_dict()} -> {new.owner.to_dict()}",
                old.owner.to_dict(),
                new.owner.to_dict(),
            )
        )
    if old.description != new.description:
        add(Change("metadata", "description_changed", INFO, "description", "Contract description changed"))
    if old.status != new.status:
        add(
            Change(
                "metadata",
                "status_changed",
                INFO,
                "status",
                f"Status {old.status} -> {new.status}",
                old.status,
                new.status,
            )
        )
