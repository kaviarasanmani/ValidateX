# ValidateX Development Task List

- [x] **v1.2.0 Release & Distribution**: Setup package dependencies, pyproject.toml, and twine distribution to PyPI.
- [x] **Airflow Integration Fixes**: Update `ValidateXOperator` logic and write unit tests.
- [x] **PySpark Windows Auto-Config**: Create `conftest.py` to auto-detect `JAVA_HOME` and download Spark `winutils.exe` dynamically.
- [x] **GitHub Action Template**: Build composite `action.yml` for CI/CD integration and write template workflow.
- [x] **Polars Engine Support (Core)**: Add routing for Polars engine and implement `_validate_polars` for core expectations (`ExpectColumnToExist`, `ExpectColumnToNotBeNull`, `ExpectColumnValuesToBeUnique`, `ExpectColumnValuesToBeBetween`).

- [/] **Polars Engine Support (Expansion)**: Port the remaining 40+ column, table, and aggregate expectations to Polars.
- [/] **Slack & Teams Notifications**: Implement webhook-based failure alerts in the reporting layer.

- [ ] **Baseline History Tracking & Trend Charts**: Save validation history locally and render trend lines inside HTML reports.
- [ ] **Great Expectations Migrator**: Implement CLI parser to import Great Expectations JSON suites.
- [ ] **Web Dashboard**: Create a lightweight Streamlit/React web dashboard for multi-dataset monitoring.
- [ ] **dbt Integration**: Write a wrapper to invoke ValidateX checks during `dbt test` runs.
