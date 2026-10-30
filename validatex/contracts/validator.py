"""
Contract validation engine.

Runs a :class:`~validatex.contracts.model.DataContract` against a dataset on any supported engine:

1. **Schema validation** - every contracted column must exist, with a compatible type (and, in
   ``strict_schema`` mode, no undeclared extra columns).
2. **Business rule validation** - nullability and every quality rule, each tagged with severity and owner.
3. **Threshold evaluation** - the contract passes only if its quality-score and failure-count thresholds hold.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from validatex.contracts.model import ContractError, DataContract
from validatex.core.introspect import get_schema, is_type_compatible, normalize_type, canonical_type
from validatex.core.result import ExpectationResult, ValidationResult
from validatex.core.validator import Validator


@dataclass
class ThresholdCheck:
    """Outcome of one contract threshold."""

    name: str
    passed: bool
    actual: Any
    expected: Any
    message: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "passed": self.passed,
            "actual": self.actual,
            "expected": self.expected,
            "message": self.message,
        }


@dataclass
class ContractResult:
    """Result of validating a dataset against a data contract."""

    contract: DataContract
    validation: ValidationResult
    threshold_checks: List[ThresholdCheck] = field(default_factory=list)

    @property
    def success(self) -> bool:
        """True when every contract threshold is satisfied."""
        return all(c.passed for c in self.threshold_checks)

    @property
    def quality_score(self) -> float:
        return self.validation.compute_quality_score()

    @property
    def schema_results(self) -> List[ExpectationResult]:
        return [r for r in self.validation.results if (r.meta or {}).get("source") == "schema"]

    @property
    def schema_violations(self) -> List[ExpectationResult]:
        return [r for r in self.schema_results if not r.success]

    @property
    def rule_violations(self) -> List[ExpectationResult]:
        return [r for r in self.validation.results if (r.meta or {}).get("source") != "schema" and not r.success]

    @property
    def violations(self) -> List[ExpectationResult]:
        return [r for r in self.validation.results if not r.success]

    def failures_by_owner(self) -> Dict[str, List[ExpectationResult]]:
        """Group failing checks by the team that owns the rule (``unassigned`` if none)."""
        out: Dict[str, List[ExpectationResult]] = {}
        for r in self.violations:
            owner = (r.meta or {}).get("owner") or "unassigned"
            out.setdefault(str(owner), []).append(r)
        return out

    def failures_by_severity(self) -> Dict[str, int]:
        counts: Dict[str, int] = {"critical": 0, "warning": 0, "info": 0}
        for r in self.violations:
            counts[r.severity] = counts.get(r.severity, 0) + 1
        return counts

    def to_dict(self) -> Dict[str, Any]:
        return {
            "contract": {
                "name": self.contract.name,
                "version": self.contract.version,
                "hash": self.contract.content_hash,
                "owner": self.contract.owner.to_dict(),
            },
            "success": self.success,
            "quality_score": self.quality_score,
            "thresholds": [c.to_dict() for c in self.threshold_checks],
            "failures_by_severity": self.failures_by_severity(),
            "failures_by_owner": {k: len(v) for k, v in self.failures_by_owner().items()},
            "validation": self.validation.to_dict(),
        }

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, default=str)

    def to_json_file(self, filepath: str) -> None:
        p = Path(filepath)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(self.to_json(), encoding="utf-8")

    def to_html(self, filepath: str) -> None:
        self.validation.to_html(filepath)

    def summary(self) -> str:
        status = "PASSED" if self.success else "FAILED"
        sev = self.failures_by_severity()
        lines = [
            "=" * 60,
            f"  Data Contract: {self.contract.name} v{self.contract.version}  -> {status}",
            f"  Owner: {self.contract.owner.team or 'n/a'} (steward: {self.contract.owner.steward or 'n/a'})",
            f"  Quality Score: {self.quality_score} / 100",
            f"  Violations: critical={sev.get('critical', 0)} warning={sev.get('warning', 0)} info={sev.get('info', 0)}",
            "-" * 60,
        ]
        for c in self.threshold_checks:
            lines.append(f"  [{'OK' if c.passed else 'FAIL'}] {c.message}")
        for r in self.violations:
            col = f" ({r.column})" if r.column else ""
            lines.append(f"  - [{r.severity}] {r.expectation_type}{col} owner={(r.meta or {}).get('owner') or '-'}")
        lines.append("=" * 60)
        return "\n".join(lines)

    def __repr__(self) -> str:
        return (
            f"ContractResult(contract={self.contract.name!r}, version={self.contract.version!r}, "
            f"success={self.success}, quality_score={self.quality_score})"
        )


# ---------------------------------------------------------------------------
# Schema validation
# ---------------------------------------------------------------------------


def _pandas_integer_like(data: Any, column: str) -> bool:
    """True for float columns that hold only whole numbers (integers widened to float by NaN)."""
    try:
        series = data[column].dropna()
        return bool(len(series) > 0 and (series % 1 == 0).all())
    except Exception:
        return False


def _schema_results(contract: DataContract, data: Any, engine: str) -> List[ExpectationResult]:
    actual = get_schema(data, engine)
    team = contract.owner.team
    results: List[ExpectationResult] = []

    def meta(rule_id: str) -> Dict[str, Any]:
        return {"severity": "critical", "owner": team, "rule_id": rule_id, "source": "schema"}

    for f in contract.schema:
        present = f.name in actual
        results.append(
            ExpectationResult(
                expectation_type="expect_column_to_exist",
                success=present,
                column=f.name,
                observed_value="present" if present else "missing",
                details={"contract_field": f.name},
                meta=meta(f"schema.{f.name}.exists"),
            )
        )
        expected = canonical_type(f.type) or "any"
        if not present or expected == "any":
            continue
        raw = actual[f.name]
        logical = normalize_type(raw)
        note = None
        if engine == "pandas" and expected == "integer" and logical == "float" and _pandas_integer_like(data, f.name):
            logical, note = "integer", "float-encoded integers (column contains nulls)"
        verified = logical != "unknown"
        details: Dict[str, Any] = {
            "expected_type": expected,
            "actual_type": raw,
            "actual_logical_type": logical,
            "verified": verified,
        }
        if note:
            details["note"] = note
        results.append(
            ExpectationResult(
                expectation_type="expect_column_values_to_be_of_type",
                success=is_type_compatible(expected, logical),
                column=f.name,
                observed_value=raw,
                details=details,
                meta=meta(f"schema.{f.name}.type"),
            )
        )

    if contract.strict_schema:
        declared = {f.name for f in contract.schema}
        extra = sorted(c for c in actual if c not in declared)
        results.append(
            ExpectationResult(
                expectation_type="expect_table_columns_to_match_set",
                success=not extra,
                observed_value=sorted(actual),
                details={"unexpected_columns": extra, "column_set": sorted(declared)},
                meta=meta("schema.strict"),
            )
        )
    return results


# ---------------------------------------------------------------------------
# Thresholds
# ---------------------------------------------------------------------------


def _evaluate_thresholds(contract: DataContract, validation: ValidationResult) -> List[ThresholdCheck]:
    t = contract.thresholds
    score = validation.compute_quality_score()
    failed = [r for r in validation.results if not r.success]
    critical = sum(1 for r in failed if r.severity == "critical")
    warnings = sum(1 for r in failed if r.severity == "warning")
    checks: List[ThresholdCheck] = []

    if t.minimum_quality_score is not None:
        ok = score >= t.minimum_quality_score
        checks.append(
            ThresholdCheck(
                "minimum_quality_score",
                ok,
                score,
                t.minimum_quality_score,
                f"quality score {score} {'>=' if ok else '<'} required {t.minimum_quality_score}",
            )
        )
    ok = critical <= t.max_critical_failures
    checks.append(
        ThresholdCheck(
            "max_critical_failures",
            ok,
            critical,
            t.max_critical_failures,
            f"{critical} critical failure(s), allowed {t.max_critical_failures}",
        )
    )
    if t.max_warning_failures is not None:
        ok = warnings <= t.max_warning_failures
        checks.append(
            ThresholdCheck(
                "max_warning_failures",
                ok,
                warnings,
                t.max_warning_failures,
                f"{warnings} warning failure(s), allowed {t.max_warning_failures}",
            )
        )
    return checks


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def _coerce_contract(contract: Any) -> DataContract:
    if isinstance(contract, DataContract):
        return contract
    if isinstance(contract, dict):
        return DataContract.from_dict(contract)
    if isinstance(contract, (str, Path)):
        return DataContract.load(contract)
    raise ContractError(f"Cannot interpret {type(contract).__name__} as a data contract.")


def validate_contract(
    data: Any,
    contract: Any,
    engine: str = "pandas",
    data_source: Optional[str] = None,
    sql_engine: Optional[Any] = None,
    run_name: Optional[str] = None,
    dataset_name: Optional[str] = None,
    store: Any = None,
    alert_policy: Any = None,
) -> ContractResult:
    """
    Validate *data* against a data *contract* (a :class:`DataContract`, dict, or YAML/JSON path).

    ``run_name``, ``dataset_name``, ``store`` and ``alert_policy`` behave exactly as in :func:`validatex.validate`,
    so contract runs feed the same historical quality monitoring.
    """
    dc = _coerce_contract(contract).ensure_valid()
    validator = Validator(dc.to_suite(), engine=engine, sql_engine=sql_engine)
    result = validator.run(data, data_source=data_source, sql_engine=sql_engine)

    target: Any = data
    if validator.engine == "sql" and not (isinstance(data, tuple) and len(data) == 2):
        target = (sql_engine or validator.sql_engine, data)

    result.results = _schema_results(dc, target, validator.engine) + result.results
    result.suite_meta = {**result.suite_meta, "contract": dc.name, "contract_version": dc.version}
    result.compute_statistics()

    from validatex.observability.recorder import record_run

    record_run(
        result,
        target,
        validator.engine,
        run_name=run_name,
        dataset_name=dataset_name or data_source or dc.name,
        store=store,
        alert_policy=alert_policy,
    )
    return ContractResult(contract=dc, validation=result, threshold_checks=_evaluate_thresholds(dc, result))
