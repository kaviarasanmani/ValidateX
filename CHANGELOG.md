# Changelog

All notable changes to ValidateX are documented here.
This project follows [Semantic Versioning](https://semver.org/).

---

## v1.3.0 - 2026-10-31

### Added
- **Data Contracts Engine (v1)** (`validatex.contracts`):
  - Declarative YAML/JSON contracts with schema fields (`name`, `type`, `nullable`), business quality rules with `severity`, contract/rule-level ownership (`team`, `steward`), and score thresholds.
  - Cross-engine logical type compatibility and strict schema mode.
  - Automated breaking-change detection (`compare_contracts`) classifying schema/rule changes as `breaking`, `non_breaking`, or `info` and validating semantic version bumps.
  - Immutable version registry (`ContractRegistry`) with content hashing and semver progression enforcement.
  - CLI subcommands: `validatex contract {lint,validate,diff,register}`.
- **Historical Data Quality Observability** (`validatex.observability` & `validatex.storage`):
  - SQLite `RunStore` tracking `runs`, `expectation_runs`, and `schema_snapshots` with automatic in-place schema migration.
  - `result.quality_score` property and top-level `vx.validate(..., run_name=..., store=...)` integration.
  - Historical score analytics (`quality_trend`) with OLS trend trajectory (`improving`, `degrading`, `stable`).
  - Failing expectation streak tracking (`failed_expectation_trends`).
  - Dataset schema evolution log (`schema_changes`).
  - Data freshness verification across engines (`check_freshness`, `check_run_freshness`).
  - Volume anomaly detection via median/MAD modified z-scores (`detect_volume_anomaly`).
  - Threshold-based alerting (`AlertPolicy`) with Slack Block Kit & Microsoft Teams MessageCard dispatch.
  - CLI subcommand: `validatex history`.
- **HTML Report Embedded Historical Trend Charts** (`validatex.reporting.html_report`):
  - Standalone single-file HTML reports automatically render interactive inline SVG quality score trend charts, mean baselines, streak tables, and schema evolution logs.
- **dbt Integration** (`validatex.integrations.dbt`):
  - Two-way bridge between dbt `schema.yml` / `manifest.json` and ValidateX `DataContract` objects (`contracts_from_dbt_schema`, `contracts_to_dbt_schema`, `parse_dbt_manifest`).
  - Warehouse-native Push-Down SQL and DataFrame model validation runner (`run_dbt_validations`).
  - CLI subcommands: `validatex dbt {import-schema,export-schema,test}`.
- **Multi-Dataset Web Dashboard** (`validatex.dashboard`):
  - Zero-dependency WSGI local control plane and static HTML exporter (`export_dashboard_html`) backed by `RunStore` and `ContractRegistry`.
  - CLI subcommand: `validatex dashboard`.
- **Great Expectations Migrator** (`validatex.migrations.ge_migrator`):
  - Programmatic and CLI migration from Great Expectations JSON/YAML suites into ValidateX expectation suites or Data Contracts.
  - CLI subcommand: `validatex migrate`.
- **Cross-Engine Semantic Parity Audit (Phase 1)**:
  - 100% test coverage across empty datasets, null-only columns, type boundaries, and driver memory limits (`<= 20` samples) across Pandas, Polars, PySpark, and Push-Down SQL.

## v1.2.0 - 2026-06-16

### Added
- **Airflow Integration fix**: `ValidateXOperator` now correctly computes
  quality score via `result.compute_quality_score()` and resolves failed
  columns from `result.results` list (previously used non-existent attributes)
- **20 unit tests** for `ValidateXOperator` covering CSV, Parquet, gate
  pass/fail, HTML report generation, unsupported formats, and XCom output
- **`USER_GUIDE.md`**: comprehensive step-by-step guide covering all engines,
  CLI, YAML config, Airflow, CI/CD, custom expectations, and troubleshooting
- **PySpark data quality example** (`examples/spark_data_quality.py`):
  end-to-end e-commerce orders dataset with 15+ checks and HTML/JSON reports
- **`MANIFEST.in`**: ensures README, LICENSE, CHANGELOG, and templates are
  bundled in source distributions

### Fixed
- `ValidateXOperator.execute()` bug: `result.score` → `result.compute_quality_score()`
- `ValidateXOperator.execute()` bug: `result.columns.items()` → `result.results` list
- `ValidateXOperator.execute()` bug: `result.passed_count` / `result.failed_count`
  → `len(passed)` / `len(failed)` from filtered `result.results`
- Airflow integration no longer raises `ImportError` at module load time;
  error is deferred to operator instantiation so non-Airflow users are unaffected

### Changed
- `python_requires` updated from `>=3.8` to `>=3.9` (aligns with pandas 2.x)
- `requirements.txt` cleaned up: removed `pyspark` as hard dependency
  (it is an optional extra — `pip install "validatex[spark]"`)
- Added `sqlalchemy` to `requirements.txt` core deps
- Expanded PyPI classifiers: Python 3.12, OS Independent, Scientific/Engineering

---

## v1.1.0 - 2026-03-04

### Added
- Push-Down SQL Native Validation engine (SQLAlchemy-based)
- Data Drift Detection with Population Stability Index (PSI)
- Apache Airflow `ValidateXOperator` integration
- Sequential / time-series expectations (`increasing`, `decreasing`)
- Conditional / cross-row expectations (`null_when`, `not_null_when`, `satisfy`)
- 20 advanced column expectations (email, URL, UUID, ISO date, phone, JSON, etc.)
- YAML / JSON declarative suite configuration
- CLI interface (`validatex validate`, `profile`, `run`, `init`, `list-expectations`)
- Data profiler with auto-suggest (`DataProfiler`)

---

## v1.0.1 - 2026-02-28

### Fixed
- Minor packaging fix for PyPI metadata

---

## v1.0.0 - 2026-02-21

### Added
- Initial public release
- 36 column-level expectations
- 5 table-level expectations
- 4 aggregate / cross-column expectations
- Pandas and PySpark dual-engine support
- Severity modeling (Critical / Warning / Info)
- Weighted data quality score (0–100)
- Column health summary with mini bar charts
- Modern dark-theme HTML reports
- JSON report export
