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

## In Progress

## Upcoming Work
- [ ] **HTML Report Embedded Trend Charts**: Render interactive historical quality score and failure trend sparklines from `RunStore` directly inside single-file HTML reports (backend `RunStore` history & `quality_trend()` are complete).
- [ ] **Great Expectations Migrator**: Implement CLI parser (`validatex migrate --from-ge suite.json --output suite.yaml`) to automatically convert Great Expectations JSON expectation suites into ValidateX suites and Data Contracts.
- [ ] **Web Dashboard**: Create a lightweight Streamlit/FastAPI + React web dashboard backed by `RunStore` and `ContractRegistry` for multi-dataset quality trends, schema history, freshness, and contract governance.
- [ ] **dbt Integration**: Build a `validatex dbt` CLI runner and dbt macro/post-hook integration to execute ValidateX suites and Data Contracts during `dbt test` / `dbt build` runs.
