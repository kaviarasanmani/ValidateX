"""
Great Expectations (GE) Suite Migrator for ValidateX.

Converts Great Expectations JSON/YAML expectation suites (v0.13 – v1.x) into
ValidateX :class:`~validatex.core.suite.ExpectationSuite` or
:class:`~validatex.contracts.DataContract` objects.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

from validatex.contracts.model import ContractError, DataContract, QualityRule, SchemaField
from validatex.core.expectation import get_expectation_class
from validatex.core.introspect import normalize_type
from validatex.core.suite import ExpectationSuite

GE_TO_VALIDATEX_MAP: Dict[str, str] = {
    "expect_column_values_to_not_be_null": "expect_column_to_not_be_null",
    "expect_column_to_exist": "expect_column_to_exist",
    "expect_column_values_to_be_unique": "expect_column_values_to_be_unique",
    "expect_column_values_to_be_between": "expect_column_values_to_be_between",
    "expect_column_values_to_be_in_set": "expect_column_values_to_be_in_set",
    "expect_column_values_to_not_be_in_set": "expect_column_values_to_not_be_in_set",
    "expect_column_values_to_match_regex": "expect_column_values_to_match_regex",
    "expect_column_values_to_not_match_regex": "expect_column_values_to_not_match_regex",
    "expect_column_values_to_be_of_type": "expect_column_values_to_be_of_type",
    "expect_column_values_to_be_dateutil_parseable": "expect_column_values_to_be_dateutil_parseable",
    "expect_column_values_to_be_json_parseable": "expect_column_values_to_be_json_parseable",
    "expect_column_value_lengths_to_be_between": "expect_column_value_lengths_to_be_between",
    "expect_column_value_lengths_to_equal": "expect_column_value_lengths_to_equal",
    "expect_column_max_to_be_between": "expect_column_max_to_be_between",
    "expect_column_min_to_be_between": "expect_column_min_to_be_between",
    "expect_column_mean_to_be_between": "expect_column_mean_to_be_between",
    "expect_column_median_to_be_between": "expect_column_median_to_be_between",
    "expect_column_stdev_to_be_between": "expect_column_stdev_to_be_between",
    "expect_column_sum_to_be_between": "expect_column_sum_to_be_between",
    "expect_column_quantile_values_to_be_between": "expect_column_quantile_values_to_be_between",
    "expect_column_distinct_values_to_be_in_set": "expect_column_distinct_values_to_be_in_set",
    "expect_column_proportion_of_unique_values_to_be_between": "expect_column_proportion_of_unique_values_to_be_between",
    "expect_column_values_to_be_increasing": "expect_column_values_to_be_increasing",
    "expect_column_values_to_be_decreasing": "expect_column_values_to_be_decreasing",
    "expect_table_row_count_to_equal": "expect_table_row_count_to_equal",
    "expect_table_row_count_to_be_between": "expect_table_row_count_to_be_between",
    "expect_table_column_count_to_equal": "expect_table_column_count_to_equal",
    "expect_table_columns_to_match_ordered_list": "expect_table_columns_to_match_ordered_list",
    "expect_table_columns_to_match_set": "expect_table_columns_to_match_set",
    "expect_column_pair_values_a_to_be_greater_than_b": "expect_column_pair_values_a_to_be_greater_than_b",
    "expect_column_pair_values_to_be_equal": "expect_column_pair_values_to_be_equal",
    "expect_multicolumn_sum_to_equal": "expect_multicolumn_sum_to_equal",
    "expect_compound_columns_to_be_unique": "expect_compound_columns_to_be_unique",
}


@dataclass
class MigrationReport:
    """Summary of migrating a Great Expectations suite to ValidateX."""

    suite_name: str
    total_ge_expectations: int = 0
    migrated_count: int = 0
    skipped_expectations: List[str] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "suite_name": self.suite_name,
            "total_ge_expectations": self.total_ge_expectations,
            "migrated_count": self.migrated_count,
            "skipped_expectations": list(self.skipped_expectations),
            "notes": list(self.notes),
        }


def _load_ge_source(source: Any) -> Dict[str, Any]:
    if isinstance(source, dict):
        return source
    if isinstance(source, (str, os.PathLike)):
        p = Path(source)
        raw = p.read_text(encoding="utf-8") if "\n" not in str(source) and p.exists() else str(source)
        try:
            loaded = json.loads(raw)
        except Exception:
            loaded = yaml.safe_load(raw)
        if isinstance(loaded, dict):
            return loaded
    raise ContractError("Invalid Great Expectations suite input")


def migrate_ge_suite(source: Any) -> Tuple[ExpectationSuite, MigrationReport]:
    """
    Convert a Great Expectations JSON/YAML suite into a ValidateX :class:`ExpectationSuite`
    and a :class:`MigrationReport`.
    """
    import validatex.expectations  # noqa: F401

    doc = _load_ge_source(source)
    suite_name = str(doc.get("expectation_suite_name") or doc.get("suite_name") or "migrated_ge_suite")
    ge_exps = list(doc.get("expectations") or [])
    report = MigrationReport(suite_name=suite_name, total_ge_expectations=len(ge_exps))
    suite = ExpectationSuite(suite_name, meta=dict(doc.get("meta") or {}))

    for item in ge_exps:
        if not isinstance(item, dict):
            continue
        ge_type = str(item.get("expectation_type") or item.get("type") or "")
        vx_type = GE_TO_VALIDATEX_MAP.get(ge_type, ge_type)
        try:
            get_expectation_class(vx_type)
        except Exception:
            report.skipped_expectations.append(ge_type)
            continue

        kwargs = dict(item.get("kwargs") or {})
        col = kwargs.pop("column", item.get("column"))
        meta = dict(item.get("meta") or {})
        if "mostly" in kwargs:
            mostly_val = kwargs.pop("mostly")
            meta["ge_mostly"] = mostly_val
            report.notes.append(f"{ge_type} on {col or 'table'}: preserved GE 'mostly={mostly_val}' in meta")
        if "type_" in kwargs and "expected_type" not in kwargs:
            kwargs["expected_type"] = kwargs.pop("type_")

        suite.add(vx_type, column=col, meta=meta, **kwargs)
        report.migrated_count += 1

    return suite, report


def migrate_ge_to_contract(
    source: Any,
    version: str = "1.0.0",
    team: Optional[str] = None,
) -> Tuple[DataContract, MigrationReport]:
    """Convert a Great Expectations suite directly into a ValidateX :class:`DataContract`."""
    suite, report = migrate_ge_suite(source)
    fields_by_col: Dict[str, SchemaField] = {}
    rules: List[QualityRule] = []

    for exp in suite.expectations:
        col = exp.column
        if col and col not in fields_by_col:
            fields_by_col[col] = SchemaField(name=col, type="any", nullable=True)

        if exp.expectation_type == "expect_column_to_exist" and col:
            continue
        if exp.expectation_type == "expect_column_to_not_be_null" and col:
            fields_by_col[col].nullable = False
            continue
        if exp.expectation_type == "expect_column_values_to_be_of_type" and col:
            raw_t = getattr(exp, "expected_type", None) or exp.kwargs.get("expected_type") or "any"
            try:
                fields_by_col[col].type = normalize_type(str(raw_t))
            except Exception:
                fields_by_col[col].type = "any"
            continue

        sev = str((exp.meta or {}).get("severity") or getattr(exp, "severity", None) or "warning")
        extra_kwargs = dict(exp.kwargs or {})
        for attr in ("min_value", "max_value", "value_set", "regex", "column_a", "column_b", "column_list"):
            if hasattr(exp, attr) and getattr(exp, attr) is not None and attr not in extra_kwargs:
                extra_kwargs[attr] = getattr(exp, attr)
        rules.append(
            QualityRule(
                expectation=exp.expectation_type,
                column=col,
                severity=sev if sev in ("critical", "warning", "info") else "warning",
                kwargs=extra_kwargs,
            )
        )

    contract = DataContract.create(
        name=suite.name,
        version=version,
        team=team,
    )
    contract.schema = list(fields_by_col.values())
    contract.quality_rules = rules
    return contract, report
