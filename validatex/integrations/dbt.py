"""
dbt Integration for ValidateX.

Provides a two-way bridge between dbt ``schema.yml`` / ``manifest.json`` artifacts and
ValidateX :class:`~validatex.contracts.DataContract` definitions, plus a runner to execute
ValidateX checks against materialized dbt models via Push-Down SQL or in-memory DataFrames.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Union

import yaml

from validatex.contracts.model import (
    ContractError,
    ContractOwner,
    DataContract,
    QualityRule,
    SchemaField,
    Thresholds,
)
from validatex.contracts.validator import ContractResult, validate_contract
from validatex.core.introspect import normalize_type


def _load_yaml_or_dict(source: Any) -> Dict[str, Any]:
    if isinstance(source, dict):
        return source
    if isinstance(source, (str, os.PathLike)):
        path_str = os.fspath(source)
        if "\n" not in path_str and Path(path_str).exists():
            raw = Path(path_str).read_text(encoding="utf-8")
        else:
            raw = str(source)
        loaded = yaml.safe_load(raw)
        if not isinstance(loaded, dict):
            raise ContractError("dbt schema YAML must parse to a mapping")
        return loaded
    raise ContractError(f"Unsupported dbt schema source: {type(source).__name__}")


def _map_dbt_severity(raw_sev: Optional[str], default: str = "warning") -> str:
    if not raw_sev:
        return default
    s = str(raw_sev).strip().lower()
    if s in ("error", "critical", "fatal"):
        return "critical"
    if s in ("warn", "warning"):
        return "warning"
    if s in ("info", "informational"):
        return "info"
    return default


def _parse_column_test(
    test_entry: Any,
    col_name: str,
    default_owner: Optional[str] = None,
) -> TupleResult:
    """
    Interpret a single dbt column test entry.
    Returns ``(makes_non_null, Optional[QualityRule])``.
    """
    if isinstance(test_entry, str):
        tname = test_entry.strip()
        tkwargs: Dict[str, Any] = {}
    elif isinstance(test_entry, dict) and len(test_entry) == 1 and "expectation" not in test_entry:
        tname, raw_cfg = next(iter(test_entry.items()))
        tkwargs = dict(raw_cfg) if isinstance(raw_cfg, dict) else {}
    elif isinstance(test_entry, dict) and ("expectation" in test_entry or "expectation_type" in test_entry):
        exp_name = str(test_entry.get("expectation") or test_entry.get("expectation_type"))
        extra = {
            k: v
            for k, v in test_entry.items()
            if k not in ("expectation", "expectation_type", "column", "severity", "owner", "description")
        }
        if isinstance(extra.get("kwargs"), dict):
            nested = dict(extra.pop("kwargs"))
            extra = {**nested, **extra}
        return (
            False,
            QualityRule(
                expectation=exp_name,
                column=str(test_entry.get("column") or col_name),
                severity=_map_dbt_severity(test_entry.get("severity"), "warning"),
                owner=test_entry.get("owner") or default_owner,
                description=test_entry.get("description"),
                kwargs=extra,
            ),
        )
    else:
        return (False, None)

    config_block = tkwargs.get("config") if isinstance(tkwargs.get("config"), dict) else {}
    raw_sev = tkwargs.get("severity") or config_block.get("severity")
    owner = tkwargs.get("owner") or config_block.get("meta", {}).get("owner") or default_owner

    if tname == "not_null":
        return (True, None)
    if tname == "unique":
        return (
            False,
            QualityRule(
                expectation="expect_column_values_to_be_unique",
                column=col_name,
                severity=_map_dbt_severity(raw_sev, "critical"),
                owner=owner,
            ),
        )
    if tname == "accepted_values":
        values = tkwargs.get("values") or tkwargs.get("arguments", {}).get("values") or []
        return (
            False,
            QualityRule(
                expectation="expect_column_values_to_be_in_set",
                column=col_name,
                severity=_map_dbt_severity(raw_sev, "warning"),
                owner=owner,
                kwargs={"value_set": list(values)},
            ),
        )
    if tname.startswith("expect_"):
        clean_kwargs = {
            k: v for k, v in tkwargs.items() if k not in ("severity", "owner", "config", "description", "column")
        }
        return (
            False,
            QualityRule(
                expectation=tname,
                column=col_name,
                severity=_map_dbt_severity(raw_sev, "warning"),
                owner=owner,
                description=tkwargs.get("description"),
                kwargs=clean_kwargs,
            ),
        )
    return (False, None)


TupleResult = tuple[bool, Optional[QualityRule]]


def _model_dict_to_contract(
    model: Dict[str, Any],
    default_version: str = "1.0.0",
    default_owner: Optional[Dict[str, Optional[str]]] = None,
) -> DataContract:
    name = str(model.get("name") or "").strip()
    if not name:
        raise ContractError("dbt model entry is missing a 'name'")

    meta = dict(model.get("meta") or {})
    cfg_meta = dict((model.get("config") or {}).get("meta") or {})
    merged_meta = {**cfg_meta, **meta}

    raw_version = model.get("latest_version") or model.get("version") or merged_meta.get("version") or default_version
    owner_raw = merged_meta.get("owner") or default_owner
    if isinstance(owner_raw, str):
        owner = ContractOwner(team=owner_raw)
    elif isinstance(owner_raw, dict):
        owner = ContractOwner.from_dict(owner_raw)
    else:
        owner = ContractOwner()

    thresholds_raw = merged_meta.get("thresholds") or {}
    if not thresholds_raw and "minimum_quality_score" in merged_meta:
        thresholds_raw = {"minimum_quality_score": merged_meta["minimum_quality_score"]}
    thresholds = Thresholds.from_dict(thresholds_raw)

    schema_fields: List[SchemaField] = []
    quality_rules: List[QualityRule] = []

    for col in model.get("columns") or []:
        if not isinstance(col, dict) or not col.get("name"):
            continue
        col_name = str(col["name"])
        raw_dtype = col.get("data_type") or col.get("type") or "any"
        try:
            logical_type = normalize_type(str(raw_dtype))
        except Exception:
            logical_type = "any"

        col_meta = dict(col.get("meta") or {})
        col_owner = col_meta.get("owner")
        nullable = bool(col.get("nullable", True))

        col_tests = list(col.get("data_tests") or []) + list(col.get("tests") or [])
        vx_meta_rules = col_meta.get("validatex") or col_meta.get("quality_rules") or []
        if isinstance(vx_meta_rules, dict):
            vx_meta_rules = [vx_meta_rules]
        col_tests.extend(vx_meta_rules)

        for test_entry in col_tests:
            makes_non_null, rule = _parse_column_test(test_entry, col_name, default_owner=col_owner)
            if makes_non_null:
                nullable = False
            if rule is not None:
                quality_rules.append(rule)

        schema_fields.append(
            SchemaField(
                name=col_name,
                type=logical_type,
                nullable=nullable,
                description=col.get("description"),
            )
        )

    # Model-level tests
    model_tests = list(model.get("data_tests") or []) + list(model.get("tests") or [])
    extra_model_rules = merged_meta.get("quality_rules") or merged_meta.get("validatex") or []
    if isinstance(extra_model_rules, dict):
        extra_model_rules = [extra_model_rules]
    model_tests.extend(extra_model_rules)

    for mtest in model_tests:
        if isinstance(mtest, dict):
            if "dbt_utils.unique_combination_of_columns" in mtest or "unique_combination_of_columns" in mtest:
                key = (
                    "dbt_utils.unique_combination_of_columns"
                    if "dbt_utils.unique_combination_of_columns" in mtest
                    else "unique_combination_of_columns"
                )
                cfg = mtest[key] if isinstance(mtest[key], dict) else {}
                cols = cfg.get("combination_of_columns") or []
                if cols:
                    quality_rules.append(
                        QualityRule(
                            expectation="expect_compound_columns_to_be_unique",
                            severity=_map_dbt_severity(cfg.get("severity"), "critical"),
                            kwargs={"column_list": list(cols)},
                        )
                    )
            elif "expectation" in mtest or "expectation_type" in mtest:
                quality_rules.append(QualityRule.from_dict(mtest))

    return DataContract(
        name=name,
        version=str(raw_version),
        description=model.get("description"),
        owner=owner,
        schema=schema_fields,
        quality_rules=quality_rules,
        thresholds=thresholds,
        meta={"dbt_source": True},
    )


def contracts_from_dbt_schema(
    source: Any,
    default_version: str = "1.0.0",
    default_owner: Optional[Dict[str, Optional[str]]] = None,
) -> List[DataContract]:
    """
    Parse a dbt ``schema.yml`` file (or YAML string / dict) into a list of :class:`DataContract` objects.
    Supports both ``models:`` and ``sources:`` (``tables:``) sections.
    """
    doc = _load_yaml_or_dict(source)
    contracts: List[DataContract] = []

    for model in doc.get("models") or []:
        if isinstance(model, dict):
            contracts.append(
                _model_dict_to_contract(model, default_version=default_version, default_owner=default_owner)
            )

    for src in doc.get("sources") or []:
        if not isinstance(src, dict):
            continue
        src_meta = dict(src.get("meta") or {})
        src_owner = src_meta.get("owner") or default_owner
        if isinstance(src_owner, str):
            src_owner = {"team": src_owner}
        for table in src.get("tables") or []:
            if isinstance(table, dict):
                contracts.append(
                    _model_dict_to_contract(table, default_version=default_version, default_owner=src_owner)
                )

    return contracts


def contract_from_dbt_model(
    source: Any,
    model_name: str,
    default_version: str = "1.0.0",
    default_owner: Optional[Dict[str, Optional[str]]] = None,
) -> DataContract:
    """Load a single model's :class:`DataContract` by name from a dbt ``schema.yml``."""
    for c in contracts_from_dbt_schema(source, default_version=default_version, default_owner=default_owner):
        if c.name == model_name:
            return c
    raise ContractError(f"Model {model_name!r} not found in dbt schema")


def contract_to_dbt_model(contract: DataContract) -> Dict[str, Any]:
    """Convert a ValidateX :class:`DataContract` into a dbt ``schema.yml`` model entry."""
    rules_by_col: Dict[str, List[QualityRule]] = {}
    table_rules: List[QualityRule] = []
    for r in contract.quality_rules:
        if r.column:
            rules_by_col.setdefault(r.column, []).append(r)
        else:
            table_rules.append(r)

    columns_out: List[Dict[str, Any]] = []
    seen_cols = set()
    for f in contract.schema:
        seen_cols.add(f.name)
        col_entry: Dict[str, Any] = {"name": f.name}
        if f.type and f.type != "any":
            col_entry["data_type"] = f.type
        if f.description:
            col_entry["description"] = f.description

        data_tests: List[Any] = []
        extra_vx_rules: List[Dict[str, Any]] = []
        if not f.nullable:
            data_tests.append("not_null")

        for r in rules_by_col.get(f.name, []):
            if r.expectation == "expect_column_to_not_be_null":
                if "not_null" not in data_tests:
                    data_tests.append("not_null")
            elif r.expectation == "expect_column_values_to_be_unique":
                data_tests.append("unique")
            elif r.expectation == "expect_column_values_to_be_in_set" and "value_set" in r.kwargs:
                data_tests.append({"accepted_values": {"values": list(r.kwargs["value_set"])}})
            else:
                extra_vx_rules.append(r.to_dict())

        if data_tests:
            col_entry["data_tests"] = data_tests
        if extra_vx_rules:
            col_entry["meta"] = {"validatex": extra_vx_rules}
        columns_out.append(col_entry)

    # Include any columns referenced by quality_rules that weren't in contract.schema
    for col_name, col_rules in rules_by_col.items():
        if col_name in seen_cols:
            continue
        col_entry = {"name": col_name, "meta": {"validatex": [r.to_dict() for r in col_rules]}}
        columns_out.append(col_entry)

    model_meta: Dict[str, Any] = {"version": contract.version}
    if not contract.owner.is_empty():
        model_meta["owner"] = contract.owner.to_dict()
    if not contract.thresholds.is_empty():
        model_meta["thresholds"] = contract.thresholds.to_dict()
    if table_rules:
        model_meta["quality_rules"] = [r.to_dict() for r in table_rules]

    out: Dict[str, Any] = {"name": contract.name}
    if contract.description:
        out["description"] = contract.description
    out["meta"] = model_meta
    if columns_out:
        out["columns"] = columns_out
    return out


def contracts_to_dbt_schema(contracts: Union[DataContract, Sequence[DataContract]]) -> Dict[str, Any]:
    """Build a dbt ``version: 2`` ``schema.yml`` dictionary from one or more contracts."""
    items = [contracts] if isinstance(contracts, DataContract) else list(contracts)
    return {
        "version": 2,
        "models": [contract_to_dbt_model(c) for c in items],
    }


def save_dbt_schema(
    contracts: Union[DataContract, Sequence[DataContract]],
    filepath: Union[str, os.PathLike],
) -> Path:
    """Write one or more :class:`DataContract` objects to a dbt ``schema.yml`` file."""
    path = Path(filepath)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = contracts_to_dbt_schema(contracts)
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return path


def parse_dbt_manifest(manifest_source: Any) -> List[DataContract]:
    """
    Extract :class:`DataContract` definitions from a dbt ``target/manifest.json`` file or dict.
    """
    if isinstance(manifest_source, dict):
        manifest = manifest_source
    else:
        manifest = json.loads(Path(manifest_source).read_text(encoding="utf-8"))

    nodes = manifest.get("nodes") or {}
    models_by_uid: Dict[str, Dict[str, Any]] = {}
    for uid, node in nodes.items():
        if isinstance(node, dict) and node.get("resource_type") == "model":
            models_by_uid[uid] = {
                "name": node.get("name"),
                "description": node.get("description"),
                "meta": node.get("meta") or {},
                "config": node.get("config") or {},
                "columns": [
                    {
                        "name": col_info.get("name") or col_key,
                        "data_type": col_info.get("data_type") or "any",
                        "description": col_info.get("description"),
                        "meta": col_info.get("meta") or {},
                        "data_tests": [],
                    }
                    for col_key, col_info in (node.get("columns") or {}).items()
                ],
                "data_tests": [],
            }

    # Attach test nodes to their parent model/column
    for uid, node in nodes.items():
        if not isinstance(node, dict) or node.get("resource_type") != "test":
            continue
        depends = (node.get("depends_on") or {}).get("nodes") or []
        test_meta = node.get("test_metadata") or {}
        tname = test_meta.get("name")
        tkwargs = dict(test_meta.get("kwargs") or {})
        col_name = node.get("column_name") or tkwargs.get("column_name")
        if not tname:
            continue
        for parent_uid in depends:
            if parent_uid not in models_by_uid:
                continue
            m = models_by_uid[parent_uid]
            if col_name:
                matched_col = next((c for c in m["columns"] if c["name"] == col_name), None)
                if matched_col is None:
                    matched_col = {"name": col_name, "data_type": "any", "data_tests": []}
                    m["columns"].append(matched_col)
                if tname in ("not_null", "unique"):
                    matched_col["data_tests"].append(tname)
                elif tname == "accepted_values":
                    matched_col["data_tests"].append({"accepted_values": {"values": tkwargs.get("values") or []}})
            elif tname == "unique_combination_of_columns":
                m["data_tests"].append(
                    {
                        "unique_combination_of_columns": {
                            "combination_of_columns": tkwargs.get("combination_of_columns") or []
                        }
                    }
                )

    return [_model_dict_to_contract(m) for m in models_by_uid.values()]


def run_dbt_validations(
    source: Any,
    data_or_engine: Any,
    models: Optional[Sequence[str]] = None,
    engine: str = "sql",
    store: Any = None,
    alert_policy: Any = None,
) -> Dict[str, ContractResult]:
    """
    Validate materialized dbt models defined in ``schema.yml`` or ``manifest.json``.

    Parameters
    ----------
    source
        Path/dict for ``schema.yml`` or ``manifest.json``, or a list of :class:`DataContract`.
    data_or_engine
        Either a SQLAlchemy Engine / connection URL (when ``engine="sql"``) or a dictionary
        mapping ``model_name -> DataFrame`` (for Pandas/Polars/PySpark testing).
    models
        Optional subset of model names to validate.
    """
    if isinstance(source, list) and all(isinstance(x, DataContract) for x in source):
        contracts = list(source)
    elif isinstance(source, (str, os.PathLike)) and str(source).endswith(".json"):
        contracts = parse_dbt_manifest(source)
    else:
        contracts = contracts_from_dbt_schema(source)

    if models:
        allowed = set(models)
        contracts = [c for c in contracts if c.name in allowed]

    sql_engine_obj = data_or_engine
    if engine == "sql" and isinstance(data_or_engine, str):
        from sqlalchemy import create_engine

        sql_engine_obj = create_engine(data_or_engine)

    results: Dict[str, ContractResult] = {}
    for contract in contracts:
        if isinstance(data_or_engine, dict):
            if contract.name not in data_or_engine:
                continue
            target = data_or_engine[contract.name]
            actual_engine = "pandas" if engine == "sql" else engine
            res = validate_contract(
                target,
                contract,
                engine=actual_engine,
                dataset_name=contract.name,
                run_name=f"dbt:{contract.name}",
                store=store,
                alert_policy=alert_policy,
            )
        else:
            res = validate_contract(
                (sql_engine_obj, contract.name),
                contract,
                engine="sql",
                dataset_name=contract.name,
                run_name=f"dbt:{contract.name}",
                store=store,
                alert_policy=alert_policy,
            )
        results[contract.name] = res
    return results
