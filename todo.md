# ValidateX Development Todo List

This file tracks the status of the ValidateX development tasks and upcoming roadmap features.

## Completed Tasks
- [x] **v1.2.0 Release & Distribution**: Setup package dependencies, `pyproject.toml`, and twine distribution to PyPI.
- [x] **Airflow Integration Fixes**: Update `ValidateXOperator` logic and write unit tests.
- [x] **PySpark Windows Auto-Config**: Create `conftest.py` to auto-detect `JAVA_HOME` and download Spark `winutils.exe` dynamically.
- [x] **GitHub Action Template**: Build composite `action.yml` for CI/CD integration and write template workflow.
- [x] **Polars Engine Support (Core & Expansion)**: Add routing for Polars engine and implement `_validate_polars` for all column, table, sequential, format, advanced, conditional, aggregate, and statistical expectations.
- [x] **Polars Engine Verification**: Create a dummy test file and run quality checks on a Polars DataFrame to verify the engine functionality.
- [x] **Slack & Teams Notifications**: Implement webhook-based alerts in the reporting layer.
- [x] **Cross-Engine Compatibility & Trustworthiness Audit (Phase 1)**: Audit all 50 expectations across Pandas, Polars, PySpark, and Push-Down SQL; verify empty datasets, null-only columns, invalid types, boundary conditions, memory safety (`<= 20` unexpected samples), and JSON/HTML report parity (`docs/CROSS_ENGINE_COMPATIBILITY.md`).
- [x] **Data Contract Engine v1 (Phase 2)**: Implement declarative YAML/JSON & fluent Python `DataContract`, semantic versioning, `ContractRegistry`, cross-engine schema + business rule validation, rule ownership (`failures_by_owner()`), `compare_contracts()` breaking-change detection, and `validatex contract {lint,validate,diff,register}` CLI commands (`docs/DATA_CONTRACTS_GUIDE.md`).
- [x] **Historical Data Quality Observability & Alerting (Phase 3)**: Upgrade SQLite `RunStore` (`runs`, `expectation_runs`, `schema_snapshots`), add `vx.validate(..., run_name=..., store=...)` and `result.quality_score`, implement `quality_trend()`, `failed_expectation_trends()`, `schema_changes()`, `check_freshness()`, `detect_volume_anomaly()`, `AlertPolicy` threshold alerting, and `validatex history` CLI (`docs/OBSERVABILITY_GUIDE.md`).
- [x] **Baseline History Tracking & Trend Charts**: Save validation history locally in `RunStore` and render interactive inline SVG quality trend lines, failing expectation streaks, schema evolution logs, and alert banners inside standalone HTML reports (`validatex/reporting/html_report.py`).
- [x] **Great Expectations Migrator**: Implement programmatic and CLI converter (`validatex migrate --from-ge suite.json --output suite.yaml [--as-contract]`) to import Great Expectations JSON/YAML suites into ValidateX suites and Data Contracts (`validatex/migrations/ge_migrator.py`).
- [x] **Web Dashboard**: Create a zero-dependency WSGI web dashboard and static HTML exporter (`validatex dashboard`) backed by `RunStore` and `ContractRegistry` for multi-dataset quality monitoring, drilldowns, and contract governance (`validatex/dashboard/server.py`).
- [x] **dbt Integration**: Build a two-way dbt `schema.yml` & `manifest.json` bridge (`contracts_from_dbt_schema`, `save_dbt_schema`, `parse_dbt_manifest`) and SQL validation runner (`run_dbt_validations`, `validatex dbt {import-schema,export-schema,test}`) (`validatex/integrations/dbt.py`).
- [x] **v1.3.0 Release Preparation & Pre-Flight Audit**: Version bumped to 1.3.0 across codebase, PEP 517 build-system configured, sdist and wheel built and validated with `twine check`, 292 tests and linters 100% passing.

## In Progress
*(None - All planned v1.3.0 roadmap items are fully developed, verified, and audited)*

## Upcoming Work (Strategic Post-v1.3.0 Roadmap)
- [ ] **Cloud RunStore Storage Adapters**: Pluggable storage providers for AWS S3, Google Cloud Storage, Azure Blob Storage, and shared PostgreSQL for enterprise teams.
- [ ] **dbt Cloud & CI Webhook Automation**: Trigger ValidateX contract validations directly from dbt Cloud job webhooks or GitHub Action check-runs on pull request merge.
- [ ] **Real-Time Streaming & Kafka Contract Validator**: Micro-batch and continuous event stream data contract validation for Kafka and Redpanda topics.
- [ ] **ML Feature Store Observability**: Pre-built contract blueprints and PSI drift triggers for Feast and Hopsworks feature stores.
- [ ] **Catalog Connectors**: Automated bi-directional contract and metadata synchronization with OpenMetadata, DataHub, and AWS Glue Data Catalog.
