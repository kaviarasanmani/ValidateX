<p align="center">
  ⭐ If ValidateX solves a real problem for you, a star helps others discover it too — takes 2 seconds.
  <h1 align="center">🚀 ValidateX</h1>
  <p align="center">
    <strong>The Open-Source Python Data Quality, Data Contracts &amp; Data Observability Framework</strong>
  </p>
  <p align="center">
    Validate <b>Pandas</b>, <b>Polars</b>, <b>PySpark</b>, and <b>Push-Down SQL</b> tables in 5 lines of Python — with versioned <b>YAML Data Contracts</b>, <b>dbt integration</b>, <b>historical SQLite observability</b>, <b>PSI drift detection</b>, and <b>zero-config HTML reports &amp; dashboards</b>.
  </p>
  <p align="center">
    <!-- Build & Tests -->
    <a href="https://github.com/kaviarasanmani/ValidateX/actions/workflows/tests.yml"><img src="https://img.shields.io/github/actions/workflow/status/kaviarasanmani/ValidateX/tests.yml?branch=main" alt="Build Status (Tests & CI)"></a>
    <img src="https://img.shields.io/badge/coverage-97%25-brightgreen" alt="Code Coverage">
    <img src="https://img.shields.io/badge/tests-292%20passed-brightgreen" alt="Test Passing Rate">
    <!-- Package, Downloads & Language -->
    <a href="https://pypi.org/project/validatex/"><img src="https://img.shields.io/pypi/v/validatex.svg" alt="PyPI Latest Version"></a>
    <img src="https://img.shields.io/pypi/dm/validatex.svg" alt="PyPI Monthly Downloads">
    <img src="https://img.shields.io/badge/python-3.9+-blue?logo=python&logoColor=white" alt="Supported Python Versions">
    <!-- License, Stars & Style -->
    <img src="https://img.shields.io/github/stars/kaviarasanmani/ValidateX?style=flat&color=yellow" alt="GitHub Stars">
    <img src="https://img.shields.io/badge/license-MIT-green" alt="MIT License">
    <img src="https://img.shields.io/badge/code%20style-black-000000" alt="Code Style: black">
  </p>
  <p align="center">
    <em>Keywords: Python Data Quality &middot; Data Validation &middot; Data Contracts &middot; Data Observability &middot; Great Expectations Alternative &middot; Pandas &middot; Polars &middot; PySpark &middot; SQLAlchemy &middot; dbt &middot; Airflow &middot; Data Drift (PSI)</em>
  </p>
</p>

---

## 💡 What Is ValidateX? (Plain-English Overview)

Bad data breaks dashboards, corrupts machine-learning models, and erodes trust. Existing tools often force a painful trade-off: either write fragile `assert` statements by hand, or spend days configuring complex boilerplate.

**ValidateX** is an open-source Python library that gives data engineers, analytics engineers, and ML teams **four capabilities in one lightweight package**:

1. **✅ Data Validation (50+ Built-in Checks):** Check nulls, uniqueness, ranges, regexes, schemas, statistical outliers, and cross-column rules across **Pandas**, **Polars**, **PySpark**, and **SQL databases** (PostgreSQL, Snowflake, BigQuery, DuckDB, SQLite) with identical semantics.
2. **📜 Data Contracts Engine (v1):** Define producer–consumer agreements in YAML or Python with column types, nullability, business rules, team/steward ownership, quality score thresholds, and automated **breaking-change detection** in CI/CD.
3. **🔭 Data Quality Observability (`RunStore`):** Automatically log every validation run to a zero-setup SQLite store (`.validatex/runs.db`) to track **historical quality score trends**, **failing expectation streaks**, **schema evolution**, **data freshness**, **volume anomalies**, and **Slack/Teams alerts**.
4. **📊 Self-Contained HTML Reports & Multi-Dataset Web Dashboard:** Generate dark-theme single-file HTML reports with embedded **SVG historical trend charts**, or launch a local multi-dataset control plane (`validatex dashboard`) with a single command.

---

## 📑 Table of Contents
- [💡 What Is ValidateX?](#-what-is-validatex-plain-english-overview)
- [🖼️ Report Preview](#️-report-preview)
- [🤔 Why ValidateX? (Comparison)](#-why-validatex)
- [⚡ Performance Benchmarks](#-performance-benchmarks)
- [💼 Real-World Industry Showcases](#-real-world-industry-showcases)
- [🎯 Who Is This For?](#-who-is-this-for)
- [✨ Features](#-features)
- [📦 Installation](#-installation)
- [🏁 Quick Start (Python & CLI)](#-quick-start)
- [📜 Data Contracts Engine (v1)](#-data-contracts-engine-v1)
- [🔭 Data Quality Observability & HTML Trend Charts](#-data-quality-observability)
- [🧱 dbt Integration (`schema.yml` & `manifest.json`)](#-dbt-integration)
- [🖥️ Multi-Dataset Web Dashboard](#-multi-dataset-web-dashboard)
- [🔁 Great Expectations 1-Command Migrator](#-great-expectations-1-command-migrator)
- [🗄️ Push-Down SQL Native Validation](#️-push-down-sql-native-validation)
- [🤖 Automate with CI/CD](#-automate-with-cicd)
- [🧩 Apache Airflow Integration](#-apache-airflow-integration)
- [🔔 Slack & Teams Webhook Alerts](#-slack--teams-webhook-alerts)
- [📈 Data Drift Detection (PSI)](#-data-drift-detection-psi)
- [🎯 Data Quality Score](#-data-quality-score)
- [📋 Available Expectations (50+)](#-available-expectations)
- [📚 Documentation Hub](#-documentation-hub)
- [🚀 Roadmap](#-roadmap)

---

## 🖼️ Report Preview

<p align="center">
  <img src="docs/screenshots/report_overview.png" alt="ValidateX HTML Data Quality Report — Overview" width="100%">
</p>

<table>
  <tr>
    <td width="50%">
      <img src="docs/screenshots/report_header.png" alt="Column Health Summary" width="100%">
      <p align="center"><em>Column Health Summary with mini bar charts</em></p>
    </td>
    <td width="50%">
      <img src="docs/screenshots/expectations_table.png" alt="Expectations Table" width="100%">
      <p align="center"><em>Severity-tagged Expectations with human-readable output</em></p>
    </td>
  </tr>
</table>

---

## 🤔 Why ValidateX?

| Capability | **ValidateX** | **Great Expectations** | **Pandera** | **Soda Core** |
|---|---|---|---|---|
| **Quickstart Setup** | ✅ `pip install` → validate in 5 lines | ⚠️ Multi-step Data Context setup | ✅ DataFrame schema decorators | ⚠️ Custom SodaCL syntax |
| **HTML Report & SVG Trends** | ✅ Standalone HTML + embedded SVG trend charts | ✅ Multi-file Data Docs site | ❌ | ⚠️ Cloud required for UI |
| **Weighted Quality Score** | ✅ 0–100 business score (`Critical ×3, Warning ×2, Info ×1`) | ❌ | ❌ | ❌ |
| **Data Contracts & Semver Diff** | ✅ Schema + Rules + Ownership + Breaking-Change Diff | ❌ | ⚠️ Schema only | ⚠️ Separate contract spec |
| **Local Observability & Dashboard** | ✅ Built-in SQLite `RunStore` + `validatex dashboard` | ⚠️ Cloud / external stores | ❌ | ⚠️ Soda Cloud |
| **Quad-Engine Parity** | ✅ Pandas, Polars, PySpark & Push-Down SQL | ✅ Pandas, Spark, SQL | ⚠️ Primarily DataFrames | ⚠️ Primarily SQL / Spark |
| **dbt Integration** | ✅ Two-way `schema.yml` & `manifest.json` bridge | ⚠️ External package | ❌ | ⚠️ External integration |
| **Data Drift (PSI)** | ✅ Built-in PSI & Schema shift detector | ❌ | ❌ | ⚠️ |
| **1-Command GE Migration** | ✅ `validatex migrate --from-ge suite.json` | — | ❌ | ❌ |

ValidateX is a **focused, zero-boilerplate alternative** for teams that want production-grade data validation, data contracts, and historical observability completely open-source and self-hosted.

---

## ⚡ Performance Benchmarks

Execution performance measured on deterministic synthetic datasets across Pandas, Polars, and PySpark engines.

### Reproducibility & Benchmark Methodology
- **Script:** [`benchmarks/benchmark_suite.py`](benchmarks/benchmark_suite.py) (`python -m benchmarks.benchmark_suite`)
- **Dataset Generation:** Deterministic NumPy `default_rng(seed=42)` synthetic generator (`user_id`, `age`, `email`, `status`, `score`)
- **Expectations Evaluated (5 checks/run):** `expect_column_to_not_be_null`, `expect_column_values_to_be_unique`, `expect_column_values_to_be_between`, `expect_column_values_to_be_in_set`, `expect_table_row_count_to_equal`
- **Timing Protocol:** 1 warmup run + 3 measured runs per engine/size; reports **median ± standard deviation** and exports full hardware/OS/version metadata to `benchmarks/latest_benchmark_results.json`
- **Reference Environment:** Python 3.11+, 64-bit x86_64 (8+ logical cores, 16 GB RAM), Pandas 2.x, Polars 1.x, PySpark 3.5 (`local[2]`, DataFrame pre-materialized in cache)

| Dataset Size | Engine | Execution Time (Median) | Memory Notes | Setup / API Lines |
|---|---|---|---|---|
| **100,000 rows** | Pandas | **0.04s** | ~2.1 MB Python heap | 5 lines |
| **100,000 rows** | Polars | **0.03s** | Native Arrow/Rust buffer | 5 lines |
| **100,000 rows** | PySpark | **1.40s** *(cached; ~50s cold JVM init)* | JVM heap (`local[2]`) | 5 lines |
| **1,000,000 rows** | Pandas | **0.13s** | ~33.2 MB Python heap | 5 lines |
| **1,000,000 rows** | Polars | **0.21s** | Native Arrow/Rust buffer | 5 lines |
| **1,000,000 rows** | PySpark | **2.85s** *(cached; ~52s cold JVM init)* | JVM heap (`local[2]`) | 5 lines |
| **10,000,000 rows** | Pandas | **2.22s** | ~268.5 MB Python heap | 5 lines |
| **10,000,000 rows** | Polars | **1.85s** | Native Arrow/Rust buffer | 5 lines |
| **10,000,000 rows** | PySpark | **Distributed Cluster** | **Distributed** | 5 lines |

> **Reproduce on your machine:** Run `python -m benchmarks.benchmark_suite` to print your hardware specifications, median ± std timings, and generate `benchmarks/latest_benchmark_results.json`.

---

## 💼 Real-World Industry Showcases

Explore production-ready validation examples tailored for major data domains in `examples/showcase/`:

- 👤 **[Customer 360 Validation](examples/showcase/showcase_customer_360.py)** — Demographic bounds, email pattern checks, loyalty tier rules.
- 🛒 **[Sales ETL Pipeline Validation](examples/showcase/showcase_sales_etl.py)** — Order revenue consistency, non-negative quantities, ISO timestamps.
- 🏦 **[Banking Transaction Audit](examples/showcase/showcase_banking_transactions.py)** — UUID ledger checks, strict currency sets, Z-score outlier detection.
- 🏥 **[Healthcare Claims Quality Gate](examples/showcase/showcase_healthcare_claims.py)** — ICD-10 diagnosis formatting, admission/discharge date ordering.
- 📦 **[Retail Inventory & Supply Chain](examples/showcase/showcase_retail_inventory.py)** — Multi-warehouse inventory, SKU regex checks, reorder point auditing.

---

## 🎯 Who Is This For?

- **Startup & Modern Data Teams** — Ship data quality gates and producer–consumer contracts in minutes, not weeks
- **Analytics Engineers (dbt users)** — Import/export dbt `schema.yml` files and run warehouse-native Push-Down SQL validations
- **ML & AI Engineers** — Validate feature stores, check data freshness, and detect distribution drift (PSI) before model training
- **CI/CD Pipelines** — Block pull requests when data quality drops or a Data Contract introduces an unversioned breaking change
- **Data Platform & Governance Teams** — Track ownership (`team` / `steward`) and monitor multi-dataset health in a self-hosted dashboard

---

## ✨ Features

| Feature | Description |
|---------|-------------|
| **50+ Built-in Expectations** | Column-level, table-level, format, statistical, conditional, and sequential cross-validations |
| **📜 Data Contracts Engine (v1)** | Versioned producer–consumer contracts with schema validation, rule ownership, semver registry, and breaking-change detection |
| **🔭 Historical Observability** | SQLite `RunStore` tracking quality score trends, failing expectation streaks, schema evolution, data freshness, and volume anomalies |
| **📈 Embedded HTML Trend Charts** | Single-file HTML reports automatically render interactive inline SVG quality trend lines, streaks, and schema history |
| **🧱 Native dbt Integration** | Two-way bridge for dbt `schema.yml` & `manifest.json` + Push-Down SQL runner (`validatex dbt`) |
| **🖥️ Multi-Dataset Web Dashboard** | Built-in WSGI control plane & static HTML exporter (`validatex dashboard`) for datasets and contracts |
| **🔁 Great Expectations Migrator** | Convert legacy GE JSON suites into ValidateX suites or Data Contracts in one CLI command (`validatex migrate`) |
| **Push-Down SQL Native** | Execute validation via SQLAlchemy directly on Postgres, Snowflake, BigQuery, SQLite, or DuckDB |
| **Quad-Engine Parity** | Semantically consistent validation across Pandas, Polars, PySpark, and Push-Down SQL |
| **🔔 Policy & Webhook Alerting** | Threshold-based `AlertPolicy` (quality drops, streaks, volume spikes, stale data) with Slack & Microsoft Teams dispatch |
| **🎯 Data Quality Score** | Weighted score (0–100) based on severity of checks (`result.quality_score`) |
| **📈 Data Drift Detection** | Calculate Population Stability Index (PSI) and schema shifts between datasets |
| **🧩 Airflow Integration** | Natively gate data pipelines via `ValidateXOperator` |
| **Clean Output** | All values are native Python types — zero NumPy leakage |

---

## 📦 Installation

```bash
# Basic install
pip install validatex

# With PySpark support
pip install "validatex[spark]"

# With database support (SQLAlchemy)
pip install "validatex[database]"

# Full install
pip install "validatex[all]"

# Development
pip install "validatex[dev]"
```

---

## 🏁 Quick Start

### Python API (5 Lines)

```python
import pandas as pd
import validatex as vx

df = pd.DataFrame({
    "user_id": [1, 2, 3, 4, 5],
    "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
    "age": [25, 30, 35, 28, 42],
    "email": ["alice@test.com", "bob@test.com", "charlie@test.com",
              "diana@test.com", "eve@test.com"],
    "status": ["active", "active", "inactive", "active", "pending"],
})

suite = (
    vx.ExpectationSuite("user_quality")
    .add("expect_column_to_not_be_null", column="user_id")
    .add("expect_column_values_to_be_unique", column="user_id")
    .add("expect_column_values_to_be_between", column="age", min_value=0, max_value=150)
    .add("expect_column_values_to_be_in_set",
         column="status", value_set=["active", "inactive", "pending"])
    .add("expect_column_values_to_match_regex",
         column="email", regex=r"^[\w.]+@[\w]+\.\w+$")
)

# Validate & record run in SQLite RunStore
result = vx.validate(
    data=df,
    suite=suite,
    run_name="daily_customer_validation",
    dataset_name="customers",
    store=".validatex/runs.db",
)

print(result.summary())
print(f"Quality Score: {result.quality_score}/100")

# Generate standalone HTML report (automatically embeds SVG historical trend chart!)
result.to_html("report.html")
result.to_json_file("report.json")
```

### CLI Reference

```bash
# 1. Initialize a project & auto-profile data
validatex init
validatex profile --data data.csv --suggest --output auto_suite.yaml

# 2. Validate a dataset against a suite
validatex validate --data data.csv --suite suite.yaml --report report.html

# 3. Lint, validate, diff, and register Data Contracts
validatex contract lint contracts/customer_master.yaml
validatex contract validate -c contracts/customer_master.yaml -d data.csv --store .validatex/runs.db --report report.html
validatex contract diff contracts/v1.yaml contracts/v2.yaml --fail-on-breaking --check-version
validatex contract register contracts/customer_master.yaml -r contracts_registry/

# 4. Inspect historical runs, failing expectation streaks, and schema changes
validatex history --store .validatex/runs.db --dataset customers
validatex history --store .validatex/runs.db --dataset customers --failures
validatex history --store .validatex/runs.db --dataset customers --schema

# 5. Import/export dbt schema.yml & run Push-Down SQL checks on dbt models
validatex dbt import-schema models/schema.yml -o contracts/
validatex dbt export-schema contracts/fct_orders.yaml -o models/schema.yml
validatex dbt test --schema models/schema.yml --db sqlite:///warehouse.db --store .validatex/runs.db

# 6. Launch Multi-Dataset Web Dashboard (or export static HTML)
validatex dashboard --store .validatex/runs.db --registry contracts/ --port 8501
validatex dashboard --store .validatex/runs.db --registry contracts/ --export dashboard.html

# 7. Migrate a Great Expectations JSON suite in one command
validatex migrate --from-ge ge_suite.json --output suite.yaml
validatex migrate --from-ge ge_suite.json --output contract.yaml --as-contract
```

---

## 📜 Data Contracts Engine (v1)

Define reusable, version-controlled data contracts combining **schema validation**, **business quality rules**, **team/steward ownership**, **quality score thresholds**, and **breaking-change detection**.

```yaml
contract:
  name: customer_master
  version: 1.0

  owner:
    team: customer_data
    steward: customer_operations

  schema:
    - name: customer_id
      type: integer
      nullable: false

    - name: email
      type: string
      nullable: false

  quality_rules:
    - expectation: expect_column_values_to_be_unique
      column: customer_id
      severity: critical

    - expectation: expect_column_values_to_match_regex
      column: email
      regex: "^[^@]+@[^@]+\\.[^@]+$"
      severity: warning

  thresholds:
    minimum_quality_score: 95
```

```python
import validatex as vx

# 1. Load & validate a dataset against the contract
contract = vx.DataContract.load("customer_master.yaml")
result = vx.validate_contract(df, contract, store=".validatex/runs.db")
print(result.success, result.quality_score)
print("Failures by owner:", result.failures_by_owner())

# 2. Compare contract versions & detect breaking changes
diff = vx.compare_contracts(old_contract, new_contract)
print(diff.summary())
print("Breaking?", diff.has_breaking_changes, "| Required bump:", diff.required_bump)

# 3. Publish into an immutable semver-checked registry
registry = vx.ContractRegistry("contracts/")
registry.register(contract)
```

---

## 🔭 Data Quality Observability

Track historical validation runs, quality score trends, chronically failing expectations, schema evolution, data freshness, volume anomalies, and threshold alerts via the built-in SQLite `RunStore`.

When `store` is passed to `vx.validate()`, calling `result.to_html("report.html")` automatically embeds an **interactive SVG Historical Quality Trend Chart**, **Top Failing Expectation Streaks table**, **Schema Evolution History**, and **Triggered Alerts Banner** inside the HTML report.

```python
import validatex as vx

store = vx.RunStore(".validatex/runs.db")
policy = vx.AlertPolicy(
    min_quality_score=95.0,
    alert_on_critical_failure=True,
    max_quality_drop=5.0,
    max_consecutive_failures=3,
    volume_anomaly=True,
    alert_on_schema_change=True,
)

# Record validation run & evaluate alert policy
result = vx.validate(
    data=df,
    suite=suite,
    run_name="daily_customer_validation",
    dataset_name="customers",
    store=store,
    alert_policy=policy,
)

# Query historical trends & anomalies programmatically
trend = store.quality_trend("customers", limit=30)          # improving / degrading / stable
failures = store.failed_expectation_trends("customers")     # failure rates & active streaks
schema_log = store.schema_changes("customers")              # added / removed / re-typed columns
freshness = vx.check_freshness(df, "updated_at", max_age="24h")
volume = vx.detect_volume_anomaly(store, "customers", current_count=len(df))

# Render HTML report with embedded SVG trend line & observability tables
result.to_html("customers_report.html")
```

---

## 🧱 dbt Integration

ValidateX natively bridges **dbt `schema.yml`** and **`target/manifest.json`** files with ValidateX `DataContract` definitions and executes warehouse-native Push-Down SQL checks against materialized dbt models.

```python
import validatex as vx

# 1. Import dbt schema.yml models & sources as ValidateX DataContracts
contracts = vx.contracts_from_dbt_schema("models/schema.yml")
orders_contract = vx.contract_from_dbt_model("models/schema.yml", "fct_orders")

# 2. Export ValidateX DataContracts back to a dbt version: 2 schema.yml
vx.save_dbt_schema(contracts, "models/generated_schema.yml")

# 3. Validate materialized dbt models directly in your SQL warehouse (or DataFrame dict)
results = vx.run_dbt_validations(
    source="models/schema.yml",
    data_or_engine="postgresql://user:pass@host/warehouse",
    models=["fct_orders"],
    engine="sql",
    store=".validatex/runs.db",
)
print(results["fct_orders"].summary())
```

---

## 🖥️ Multi-Dataset Web Dashboard

Monitor all datasets, historical quality trends, failing streaks, schema changes, and registered Data Contracts in one place—either as a live local web server or a static HTML artifact for CI/CD & GitHub Pages.

```python
import validatex as vx

# Export a standalone static HTML dashboard
vx.export_dashboard_html(
    "dashboard.html",
    store=".validatex/runs.db",
    registry="contracts/",
)

# Or launch the live web server (with REST JSON endpoints at /api/summary, /api/datasets, /api/contracts)
vx.serve_dashboard(host="127.0.0.1", port=8501, store=".validatex/runs.db", registry="contracts/")
```

---

## 🔁 Great Expectations 1-Command Migrator

Already have Great Expectations JSON suites? Convert them to ValidateX suites or Data Contracts in one line:

```python
import validatex as vx

# Convert to ExpectationSuite
suite, report = vx.migrate_ge_suite("legacy_ge_suite.json")
suite.save("validatex_suite.yaml")

# Or convert directly into a versioned DataContract
contract, report = vx.migrate_ge_to_contract("legacy_ge_suite.json", version="1.0.0", team="data_platform")
contract.save("customer_contract.yaml")
```

---

## 🗄️ Push-Down SQL Native Validation

ValidateX can validate terabytes of data directly inside your database without ever loading DataFrames into Python memory. This generates optimized native queries (like `SELECT COUNT(*)`) under the hood.

```python
import validatex as vx
from sqlalchemy import create_engine

# 1. Connect to any database (PostgreSQL, Snowflake, BigQuery, DuckDB, SQLite)
engine = create_engine("postgresql://user:pass@host/db")

# 2. Build your expectation suite
suite = (
    vx.ExpectationSuite("users_table_checks")
    .add("expect_table_row_count_to_be_between", min_value=1_000_000, max_value=5_000_000)
    .add("expect_column_to_not_be_null", column="email")
    .add("expect_column_values_to_be_unique", column="user_id")
    .add("expect_column_values_to_be_between", column="age", min_value=18, max_value=120)
)

# 3. Validate directly against the SQL table (Zero Pandas overhead!)
result = vx.validate(
    data="prod_users",     # Just pass the table name
    suite=suite,
    engine="sql",          # Tells ValidateX to use Push-Down SQL
    sql_engine=engine      # The SQLAlchemy database connection
)

print(f"Data Quality Score: {result.quality_score}/100")
```

---

## 🤖 Automate with CI/CD

ValidateX is designed to be lightweight and CI-friendly. Integrate it into GitHub Actions, GitLab CI, or Jenkins pipelines to gate deployments on data quality and data contract evolution.

```yaml
name: Data Quality & Contract Gate
on: [push, pull_request]

jobs:
  validate-data:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'
          
      - name: Install ValidateX
        run: pip install validatex
        
      - name: Check Data Contract for Breaking Changes
        run: |
          validatex contract diff \
            contracts/customer_master_v1.yaml \
            contracts/customer_master_v2.yaml \
            --fail-on-breaking \
            --check-version

      - name: Run Data Validation
        run: |
          validatex validate \
            --data data/production_data.csv \
            --suite tests/data_quality/suite.yaml \
            --report dq_report.html
            
      - name: Archive production artifacts
        uses: actions/upload-artifact@v4
        if: always()
        with:
          name: validatex-report
          path: dq_report.html
```

---

## 🧩 Apache Airflow Integration

ValidateX includes a native Apache Airflow operator to gate your ETL pipelines based on Data Quality Scores.

```python
from validatex.integrations.airflow import ValidateXOperator

# This task will FAIL the Airflow DAG if the data quality score is < 95.0
validate_data = ValidateXOperator(
    task_id="ensure_data_quality",
    suite=suite,
    data_path="s3://my-bucket/daily_users.parquet",
    data_format="parquet",
    min_score=95.0, 
    report_path="/tmp/validatex_daily_report.html"
)
```

---

## 🔔 Slack & Teams Webhook Alerts

Send real-time notifications to engineering channels whenever validations run or data quality thresholds fail.

```python
import validatex as vx

result = vx.validate(df, suite)

# Send Slack alert (Block Kit format)
result.send_slack(webhook_url="https://hooks.slack.com/services/...", notify_on="failure")

# Send Teams alert (MessageCard format)
result.send_teams(webhook_url="https://outlook.office.com/webhook/...", notify_on="failure")
```

Or trigger alerts automatically via the CLI:
```bash
validatex validate --data data.csv --suite suite.yaml --slack-webhook $SLACK_URL --notify-on failure
```

---

## 📈 Data Drift Detection (PSI)

Stop guessing if distributions have changed. Calculate **Population Stability Index (PSI)** and exact schema changes natively without heavy dependencies.

```python
import validatex as vx

# Compare Yesterday's data vs Today's data
detector = vx.DriftDetector(psi_threshold=0.2)
report = detector.compare(yesterday_df, today_df)

print(report.summary())
```
**Output:**
```
============================================================
  ValidateX Data Drift Report
============================================================
[1] Schema Changes:
  No schema changes detected.
[2] Feature Drift (PSI):
  🔴 DRIFTED | income               | PSI: 5.6120 (numerical)
  🟢 STABLE  | age                  | PSI: 0.0034 (numerical)
```

---

## 🎯 Data Quality Score

ValidateX computes a **weighted rule pass score** (0–100) based on the severity of each configured expectation:

| Severity | Weight | Example Expectations |
|----------|--------|---------------------|
| 🔴 **Critical** | ×3 | Null checks, uniqueness, column existence, row count |
| 🟡 **Warning** | ×2 | Range checks, set membership, regex, type checks |
| 🔵 **Info** | ×1 | Mean/stdev bounds, string lengths, distinct values |

**Formula:** `Score = 100 × (weighted_passed / weighted_total)`

> ⚠️ **Important — Configured Checks vs. Absolute Data Quality:**
> The Quality Score measures **pass rates of configured checks only** (`quality_score_scope: "configured_checks_only"`), **not** absolute dataset quality. Always evaluate `result.column_coverage_percent` and `result.total_expectations` alongside `result.quality_score`.

```python
result = vx.validate(df, suite)
print(f"Configured Quality Score: {result.quality_score}/100")
print(f"Column Coverage: {result.checked_columns}/{result.total_columns} ({result.column_coverage_percent}%)")
```

---

## 📊 Column Health Summary

The HTML report includes a **Column Health Summary** that aggregates all expectations per column:

| Column | Checks | Passed | Failed | Health | Null % | Unique % |
|--------|--------|--------|--------|--------|--------|----------|
| user_id | 3 | 3 | 0 | 100% ███ | 0.0% | 100.0% ███ |
| email | 4 | 4 | 0 | 100% ███ | 0.0% | 100.0% ███ |
| status | 1 | 1 | 0 | 100% ███ | — | — |

```python
for col in result.column_health():
    print(f"{col.column}: {col.health_score}% health, {col.passed}/{col.checks} passed")
```

---

## 📋 Available Expectations

### Column-Level (36)
| Expectation | Severity | Description |
|------------|----------|-------------|
| `expect_column_to_exist` | 🔴 Critical | Column exists in DataFrame |
| `expect_column_to_not_be_null` | 🔴 Critical | No null values |
| `expect_column_values_to_be_unique` | 🔴 Critical | All values unique |
| `expect_column_values_to_be_between` | 🟡 Warning | Values within range |
| `expect_column_values_to_be_in_set` | 🟡 Warning | Values in allowed set |
| `expect_column_values_to_not_be_in_set` | 🟡 Warning | Values not in forbidden set |
| `expect_column_values_to_match_regex` | 🟡 Warning | Values match regex pattern |
| `expect_column_values_to_be_of_type` | 🟡 Warning | Column dtype matches |
| `expect_column_values_to_be_dateutil_parseable` | 🟡 Warning | Values parseable as dates |
| `expect_column_value_lengths_to_be_between` | 🔵 Info | String lengths within range |
| `expect_column_max_to_be_between` | 🔵 Info | Column max within bounds |
| `expect_column_min_to_be_between` | 🔵 Info | Column min within bounds |
| `expect_column_mean_to_be_between` | 🔵 Info | Column mean within bounds |
| `expect_column_stdev_to_be_between` | 🔵 Info | Column std dev within bounds |
| `expect_column_distinct_values_to_be_in_set` | 🔵 Info | All distinct values in set |
| `expect_column_proportion_of_unique_values_to_be_between` | 🔵 Info | Uniqueness ratio in range |
| `expect_column_values_to_not_match_regex` | 🟡 Warning | Values do not match regex |
| `expect_column_values_to_be_valid_email` | 🟡 Warning | Values parse as valid emails |
| `expect_column_values_to_be_json_parseable` | 🟡 Warning | Values are parseable JSON |
| `expect_column_sum_to_be_between` | 🔵 Info | Column sum within bounds |
| `expect_column_median_to_be_between` | 🔵 Info | Column median within bounds |
| `expect_column_value_lengths_to_equal` | 🔵 Info | String lengths exact match |
| `expect_column_quantile_values_to_be_between` | 🔵 Info | Per-quantile range checks |
| `expect_column_null_percentage_to_be_less_than` | 🟡 Warning | Null rate < threshold |
| `expect_column_values_to_be_positive` | 🟡 Warning | All values > 0 |
| `expect_column_values_to_be_negative` | 🟡 Warning | All values < 0 |
| `expect_column_values_to_be_in_range_of_std_devs` | 🔵 Info | Outlier / Z-score detection |
| `expect_column_correlation_to_be_between` | 🔵 Info | Pearson correlation in range |
| `expect_column_values_to_have_no_whitespace` | 🟡 Warning | No leading/trailing whitespace |
| `expect_column_values_to_be_valid_url` | 🟡 Warning | Valid HTTP/HTTPS/FTP URLs |
| `expect_column_values_to_be_valid_ip_address` | 🟡 Warning | Valid IPv4 / IPv6 addresses |
| `expect_column_values_to_be_valid_uuid` | 🟡 Warning | Valid UUID (any version) |
| `expect_column_values_to_be_valid_iso_date` | 🟡 Warning | Valid ISO 8601 dates |
| `expect_column_values_to_be_valid_phone_number` | 🟡 Warning | Valid international phone |
| `expect_column_values_to_be_all_uppercase` | 🔵 Info | All values UPPERCASED |
| `expect_column_values_to_be_all_lowercase` | 🔵 Info | All values lowercased |

### Table-Level (5)
| Expectation | Severity | Description |
|------------|----------|-------------|
| `expect_table_row_count_to_equal` | 🔴 Critical | Exact row count |
| `expect_table_row_count_to_be_between` | 🔴 Critical | Row count in range |
| `expect_table_columns_to_match_ordered_list` | 🔴 Critical | Column order matches |
| `expect_table_columns_to_match_set` | 🔴 Critical | Column names match (unordered) |
| `expect_table_column_count_to_equal` | 🔴 Critical | Exact column count |

### Aggregate / Cross-Column (4)
| Expectation | Severity | Description |
|------------|----------|-------------|
| `expect_column_pair_values_a_to_be_greater_than_b` | 🟡 Warning | Column A > Column B |
| `expect_column_pair_values_to_be_equal` | 🟡 Warning | Two columns equal |
| `expect_multicolumn_sum_to_equal` | 🟡 Warning | Row-wise sum equals target |
| `expect_compound_columns_to_be_unique` | 🔴 Critical | Compound key uniqueness |

### Sequential / Time-Series (2)
| Expectation | Severity | Description |
|------------|----------|-------------|
| `expect_column_values_to_be_increasing` | 🔵 Info | Monotonically increasing |
| `expect_column_values_to_be_decreasing` | 🔵 Info | Monotonically decreasing |

### Conditional / Cross-Row (3)
| Expectation | Severity | Description |
|------------|----------|-------------|
| `expect_column_values_to_be_null_when` | 🟡 Warning | Column must be null given condition |
| `expect_column_values_to_be_not_null_when` | 🔴 Critical | Column must not be null given condition |
| `expect_column_values_to_satisfy` | 🟡 Warning | Pass a Python lambda as custom validation |

---

## 📚 Documentation Hub

Explore comprehensive guides, FAQs, and migration documentation:

- 📜 **[Data Contracts Guide (v1)](docs/DATA_CONTRACTS_GUIDE.md)** — Schema contracts, ownership routing, semver registry, and breaking-change detection.
- 🔭 **[Data Quality Observability Guide](docs/OBSERVABILITY_GUIDE.md)** — Historical `RunStore`, quality/failure trends, schema history, freshness, volume anomalies, and `AlertPolicy`.
- 🧪 **[Cross-Engine Compatibility Matrix](docs/CROSS_ENGINE_COMPATIBILITY.md)** — Verified semantic parity across Pandas, Polars, PySpark, and Push-Down SQL.
- 🔁 **[Migration Guide (Great Expectations & AWS Deequ)](docs/MIGRATION_GUIDE.md)** — Step-by-step instructions and automated CLI migration for converting legacy suites to ValidateX.
- 🏛️ **[Architectural Decisions & Safeguards (ADR)](docs/ARCHITECTURAL_DECISIONS.md)** — Compute alignment, memory safeguards, Data Contracts, and `RunStore`.
- 🛠️ **[Custom Expectations Developer Guide](docs/CUSTOM_EXPECTATIONS_GUIDE.md)** — Extend ValidateX with domain-specific rules.
- ❓ **[Frequently Asked Questions (FAQ)](docs/FAQ.md)** — Security, engine performance, custom expectations, contracts, and alert setup.
- ⚙️ **[GitHub Action Setup](action.yml)** — Automate data quality gates in CI/CD.

### 📰 Technical Articles & Guides
- 🛡️ **[10 Data Quality Checks Every Data Engineer Needs](docs/articles/10_data_quality_checks_every_data_engineer_needs.md)**
- ⚡ **[Validate Pandas DataFrames in Minutes](docs/articles/validate_pandas_dataframes_in_minutes.md)**
- ⚖️ **[ValidateX vs Great Expectations Architectural Comparison](docs/articles/validatex_vs_great_expectations.md)**
- 📊 **[How to Generate Beautiful HTML Data Quality Reports](docs/articles/how_to_generate_beautiful_html_data_quality_reports.md)**

---

## 🏗️ Architecture

```
validatex/
├── core/
│   ├── expectation.py     # Base class + registry
│   ├── introspect.py      # Cross-engine schema & logical type introspection
│   ├── result.py          # ValidationResult, QualityScore, Severity, ColumnHealth
│   ├── suite.py           # ExpectationSuite (fluent API)
│   └── validator.py       # Validation orchestrator
├── contracts/
│   ├── model.py           # DataContract, SchemaField, QualityRule, ContractOwner, Thresholds
│   ├── validator.py       # validate_contract() & ContractResult (failures_by_owner)
│   ├── diff.py            # compare_contracts() & breaking-change detection
│   └── registry.py        # ContractRegistry (immutable semver-governed storage)
├── observability/
│   ├── trends.py          # quality_trend(), failed_expectation_trends(), schema_changes()
│   ├── freshness.py       # check_freshness() & check_run_freshness()
│   ├── volume.py          # detect_volume_anomaly() (median/MAD modified z-score)
│   ├── alerting.py        # AlertPolicy & dispatch_alerts()
│   └── recorder.py        # Automatic RunStore logging & alert evaluation
├── dashboard/
│   └── server.py          # Multi-dataset WSGI Web Dashboard & static HTML exporter
├── integrations/
│   ├── airflow.py         # ValidateXOperator for Apache Airflow DAGs
│   └── dbt.py             # Two-way dbt schema.yml / manifest.json bridge & SQL runner
├── migrations/
│   └── ge_migrator.py     # Great Expectations suite & contract migrator
├── storage/
│   └── run_store.py       # SQLite RunStore (runs, expectation_runs, schema_snapshots)
├── expectations/          # 50+ built-in expectations across 7 modules
├── datasources/           # CSV, Parquet, SQLAlchemy Database, and DataFrame sources
├── profiler/              # Auto-profiling, suggestion engine & PSI DriftDetector
├── reporting/             # Standalone HTML (with SVG trend charts), JSON & Slack/Teams alerts
├── config/                # YAML/JSON checkpoint & suite loader
└── cli/
    └── main.py            # CLI (validate, run, profile, contract, history, dbt, dashboard, migrate, init, list-expectations)
```

---

## 🧪 Testing

```bash
# Run all tests (292 tests)
pytest tests/ -v

# Run with coverage
pytest tests/ -v --cov=validatex --cov-report=html

# Unit tests only
pytest tests/unit/ -v

# Integration tests
pytest tests/integration/ -v
```

---

## 🚀 Roadmap

- [x] 50+ built-in expectations (column, table, aggregate, statistical, sequential, format, conditional)
- [x] Quad-engine support with cross-engine semantic parity (Pandas, Polars, PySpark, Push-Down SQL)
- [x] Severity modeling (Critical / Warning / Info)
- [x] Weighted data quality score (`0–100`, `result.quality_score`)
- [x] Column health summary with mini charts
- [x] Modern HTML reports with dark theme
- [x] Data Drift Detection (Population Stability Index / Schema checks)
- [x] Apache Airflow Integration via `ValidateXOperator`
- [x] Sequential & Time-Series Anomaly features
- [x] Data profiler with auto-suggestion
- [x] CLI with `validate`, `profile`, `run`, `contract`, `history`, `dbt`, `dashboard`, `migrate`, `init`, `list-expectations` commands
- [x] YAML/JSON declarative configuration
- [x] Native Python type sanitization
- [x] Slack / Teams notifications on failure
- [x] GitHub Action template for CI/CD
- [x] Polars engine support
- [x] Data Contract Engine v1 (versioned YAML/JSON contracts, ownership, breaking-change diff, `ContractRegistry`)
- [x] Historical Data Quality Observability (`RunStore`, quality/failure trends, schema evolution, freshness, volume anomalies, `AlertPolicy`)
- [x] HTML report embedded historical SVG trend charts, failing streaks & schema evolution
- [x] Great Expectations suite import/migration CLI (`validatex migrate`)
- [x] Multi-dataset Web Dashboard (`validatex dashboard`)
- [x] dbt integration (`validatex dbt`)

### Versioning
ValidateX follows [Semantic Versioning](https://semver.org/). 
- **MAJOR** version for incompatible API changes
- **MINOR** version for backwards-compatible new functionality
- **PATCH** version for backwards-compatible bug fixes

---

## 📄 License

MIT License

---

<p align="center">
  <strong>Built with ❤️ by Kaviarasan </strong>
  <br>
  <sub>If this project helps you, consider giving it a ⭐</sub>
</p>
