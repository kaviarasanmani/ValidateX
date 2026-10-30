"""
ValidateX - A powerful data quality validation, data contract, and observability framework.

ValidateX provides a comprehensive suite of tools for validating,
governing, profiling, and monitoring data quality across Pandas, Polars, PySpark, and Push-Down SQL.

Usage:
    >>> import validatex as vx
    >>> suite = vx.ExpectationSuite("my_suite")
    >>> suite.add("expect_column_to_not_be_null", column="user_id")
    >>> suite.add("expect_column_values_to_be_between", column="age", min_value=0, max_value=150)
    >>> result = vx.validate(df, suite)
    >>> result.to_html("report.html")
"""

__version__ = "1.2.0"
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
from validatex.contracts import (
    ContractDiff,
    ContractRegistry,
    ContractResult,
    DataContract,
    compare_contracts,
    validate_contract,
)
from validatex.storage.run_store import RunStore
from validatex.observability import (
    AlertPolicy,
    check_freshness,
    check_run_freshness,
    detect_volume_anomaly,
)
from validatex.integrations.dbt import (
    contract_from_dbt_model,
    contracts_from_dbt_schema,
    contracts_to_dbt_schema,
    parse_dbt_manifest,
    run_dbt_validations,
    save_dbt_schema,
)
from validatex.dashboard import (
    build_dashboard_payload,
    create_wsgi_app,
    export_dashboard_html,
    render_dashboard_html,
    serve_dashboard,
)
from validatex.migrations import (
    MigrationReport,
    migrate_ge_suite,
    migrate_ge_to_contract,
)

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
    "DataContract",
    "ContractRegistry",
    "ContractResult",
    "ContractDiff",
    "compare_contracts",
    "validate_contract",
    "RunStore",
    "AlertPolicy",
    "check_freshness",
    "check_run_freshness",
    "detect_volume_anomaly",
    "contract_from_dbt_model",
    "contracts_from_dbt_schema",
    "contracts_to_dbt_schema",
    "parse_dbt_manifest",
    "run_dbt_validations",
    "save_dbt_schema",
    "build_dashboard_payload",
    "create_wsgi_app",
    "export_dashboard_html",
    "render_dashboard_html",
    "serve_dashboard",
    "MigrationReport",
    "migrate_ge_suite",
    "migrate_ge_to_contract",
]
