# ValidateX User Guide

> A complete step-by-step guide to installing, configuring, and using ValidateX
> — a powerful data quality validation framework for Python.

---

## Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [Installation](#2-installation)
3. [Quick Start (5 minutes)](#3-quick-start-5-minutes)
4. [Core Concepts](#4-core-concepts)
5. [Building Expectation Suites](#5-building-expectation-suites)
6. [Running Validation](#6-running-validation)
7. [Reading Results](#7-reading-results)
8. [Generating Reports](#8-generating-reports)
9. [YAML / JSON Configuration](#9-yaml--json-configuration)
10. [Data Profiler & Auto-Suggest](#10-data-profiler--auto-suggest)
11. [Data Drift Detection](#11-data-drift-detection)
12. [PySpark Engine](#12-pyspark-engine)
13. [SQL Push-Down Engine](#13-sql-push-down-engine)
14. [Apache Airflow Integration](#14-apache-airflow-integration)
15. [CLI Reference](#15-cli-reference)
16. [All Available Expectations](#16-all-available-expectations)
17. [Custom Expectations](#17-custom-expectations)
18. [Severity Levels & Quality Score](#18-severity-levels--quality-score)
19. [CI/CD Integration](#19-cicd-integration)
20. [Troubleshooting](#20-troubleshooting)

---

## 1. Prerequisites

| Requirement | Minimum Version | Notes |
|-------------|----------------|-------|
| Python | 3.9+ | 3.11 recommended |
| pip | 22+ | included with Python |
| Java (JDK) | 11+ | **Only needed for PySpark engine** |

---

## 2. Installation

### Basic install (Pandas only)

```bash
pip install validatex
```

### With PySpark support

```bash
pip install "validatex[spark]"
```

### With database (SQL) support

```bash
pip install "validatex[database]"
```

### Full install (everything)

```bash
pip install "validatex[all]"
```

### Development install (from source)

```bash
git clone https://github.com/kaviarasanmani/ValidateX.git
cd ValidateX
python -m venv .venv

# Windows
.venv\Scripts\activate

# macOS / Linux
source .venv/bin/activate

pip install -e ".[dev]"
```

> **Verify installation:**
> ```bash
> python -c "import validatex; print(validatex.__version__)"
> validatex --help
> ```

---

## 3. Quick Start (5 minutes)

```python
import pandas as pd
import validatex as vx

# 1. Create (or load) your DataFrame
df = pd.DataFrame({
    "user_id": [1, 2, 3, 4, 5],
    "name":    ["Alice", "Bob", "Charlie", "Diana", "Eve"],
    "age":     [25, 30, 35, 28, 42],
    "email":   ["alice@test.com", "bob@test.com", "charlie@test.com",
                "diana@test.com", "eve@test.com"],
    "status":  ["active", "active", "inactive", "active", "pending"],
})

# 2. Define your expectations (rules)
suite = (
    vx.ExpectationSuite("user_quality")
    .add("expect_column_to_not_be_null",        column="user_id")
    .add("expect_column_values_to_be_unique",    column="user_id")
    .add("expect_column_values_to_be_between",   column="age", min_value=0, max_value=150)
    .add("expect_column_values_to_be_in_set",    column="status",
         value_set=["active", "inactive", "pending"])
    .add("expect_column_values_to_match_regex",  column="email",
         regex=r"^[\w.]+@[\w]+\.\w+$")
)

# 3. Run validation
result = vx.validate(df, suite)

# 4. View results
print(result.summary())

# 5. Generate reports
result.to_html("report.html")       # Open in browser
result.to_json_file("report.json")  # Machine-readable
```

**Expected output:**
```
============================================================
  ValidateX Validation Report — user_quality
============================================================
  Status            : PASSED
  Quality Score     : 100.0 / 100
  Total Expectations: 5
  Passed            : 5
  Failed            : 0
  Success Rate      : 100.00%
============================================================
```

---

## 4. Core Concepts

### ExpectationSuite
A named collection of rules (expectations) to apply to your data.
Think of it as a test suite for your DataFrame.

### Expectation
A single data quality rule. e.g. "column `age` must be between 0 and 150".
Each expectation has a **severity** (Critical / Warning / Info).

### ValidationResult
The output of running a suite against data. Contains:
- Pass / fail per expectation
- Overall quality score (0–100)
- Column health summary
- Detailed unexpected values

### Engine
How the validation is executed:
- `pandas` — default, runs in Python memory
- `spark` — distributed via PySpark
- `sql` — push-down queries directly inside your database

---

## 5. Building Expectation Suites

### Fluent (chained) API

```python
suite = (
    vx.ExpectationSuite("my_suite")
    .add("expect_column_to_not_be_null",           column="id")
    .add("expect_column_values_to_be_unique",       column="id")
    .add("expect_column_values_to_be_between",      column="age",
         min_value=0, max_value=150)
    .add("expect_column_values_to_be_in_set",       column="status",
         value_set=["active", "inactive"])
    .add("expect_table_row_count_to_be_between",
         min_value=100, max_value=10000)
)
```

### Passing extra kwargs

Some expectations accept named keyword arguments directly:

```python
# Direct kwargs (shorthand)
.add("expect_column_values_to_be_between", column="age",
     min_value=0, max_value=150)

# Via kwargs dict (explicit)
.add("expect_column_values_to_be_between", column="age",
     kwargs={"min_value": 0, "max_value": 150})
```

### Override severity

```python
.add("expect_column_mean_to_be_between", column="revenue",
     min_value=1000, max_value=50000,
     meta={"severity": "critical"})   # default is "info" → override to "critical"
```

### Removing expectations

```python
suite.remove("expect_column_to_not_be_null", column="id")
suite.clear()   # remove all
```

### Saving and loading suites

```python
# Save
suite.save("my_suite.yaml")
suite.save("my_suite.json")

# Load
suite = vx.ExpectationSuite.load("my_suite.yaml")
```

---

## 6. Running Validation

### Pandas DataFrame (default)

```python
result = vx.validate(df, suite)
result = vx.validate(df, suite, engine="pandas")  # explicit
```

### With a data source label

```python
result = vx.validate(df, suite, data_source="production_users_2024_01")
```

### From a CSV file

```python
import pandas as pd
df = pd.read_csv("data.csv")
result = vx.validate(df, suite)
```

### From a Parquet file

```python
df = pd.read_parquet("data.parquet")
result = vx.validate(df, suite)
```

### PySpark DataFrame

```python
result = vx.validate(spark_df, suite, engine="spark")
```

### SQL Push-Down

```python
from sqlalchemy import create_engine
engine = create_engine("postgresql://user:pass@host/db")
result = vx.validate("my_table", suite, engine="sql", sql_engine=engine)
```

---

## 7. Reading Results

### Summary string

```python
print(result.summary())
```

### Quality score (0–100)

```python
score = result.compute_quality_score()
print(f"Score: {score}/100")
```

### Check overall pass/fail

```python
if result.success:
    print("All checks passed!")
else:
    print("Some checks failed.")
```

### Iterate individual results

```python
for r in result.results:
    status = "PASS" if r.success else "FAIL"
    print(f"[{status}] {r.expectation_type} | column={r.column}")
    if not r.success:
        print(f"       unexpected_count={r.unexpected_count} ({r.unexpected_percent:.1f}%)")
        print(f"       sample values: {r.unexpected_values[:5]}")
```

### Column health summary

```python
for col in result.column_health():
    print(f"{col.column}: {col.health_score:.0f}% health "
          f"({col.passed}/{col.checks} passed)")
```

### Convert to dict / JSON string

```python
data = result.to_dict()      # Python dict
json_str = result.to_json()  # JSON string
```

---

## 8. Generating Reports

### HTML report (recommended)

```python
result.to_html("report.html")
```

Opens in any browser. Includes:
- Quality score gauge
- Pass/fail summary
- Column health bar charts
- Detailed expectation table with severity badges
- Unexpected value samples
- Download buttons (JSON / CSV)

### JSON report

```python
result.to_json_file("report.json")
```

Machine-readable. Useful for CI/CD pipelines and dashboards.

---

## 9. YAML / JSON Configuration

Define suites declaratively — no Python required.

### YAML suite file

```yaml
# suite.yaml
suite_name: production_orders

meta:
  description: "Daily quality checks for orders table"
  owner: "data-engineering"

expectations:
  - expectation_type: expect_column_to_not_be_null
    column: order_id
    meta:
      severity: critical

  - expectation_type: expect_column_values_to_be_unique
    column: order_id

  - expectation_type: expect_column_values_to_be_between
    column: amount
    kwargs:
      min_value: 0.01
      max_value: 99999.99

  - expectation_type: expect_column_values_to_be_in_set
    column: status
    kwargs:
      value_set: ["pending", "shipped", "completed", "refunded"]

  - expectation_type: expect_table_row_count_to_be_between
    kwargs:
      min_value: 1
      max_value: 1000000
```

### Load and use in Python

```python
suite = vx.ExpectationSuite.load("suite.yaml")
result = vx.validate(df, suite)
```

### Run via CLI

```bash
validatex validate --data orders.csv --suite suite.yaml --report report.html
```

---

## 10. Data Profiler & Auto-Suggest

The profiler analyses your dataset and automatically suggests expectations.

### Profile a DataFrame

```python
from validatex import DataProfiler

profiler = DataProfiler()
profile = profiler.profile(df)
print(profile.summary())
```

**Output includes:**
- Row and column counts
- Null percentages per column
- Min / max / mean / std for numeric columns
- Top values for categorical columns
- String length stats

### Auto-suggest expectations

```python
# Generate a suite automatically based on the data profile
auto_suite = profiler.suggest_expectations(df, suite_name="auto_suite")
print(f"Suggested {len(auto_suite)} expectations")

# Save for review / editing
auto_suite.save("auto_suite.yaml")
```

### Profile via CLI

```bash
validatex profile --data data.csv --suggest --output auto_suite.yaml
```

---

## 11. Data Drift Detection

Compare two datasets to detect distribution changes (Population Stability Index).

```python
import validatex as vx

# Compare yesterday's data vs today's data
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
  DRIFTED | income    | PSI: 5.6120 (numerical)
  STABLE  | age       | PSI: 0.0034 (numerical)
  STABLE  | country   | PSI: 0.0810 (categorical)
```

### PSI Thresholds

| PSI Value | Meaning |
|-----------|---------|
| < 0.1 | No significant drift |
| 0.1 – 0.2 | Moderate drift — monitor |
| > 0.2 | Significant drift — investigate |

### Detect schema changes only

```python
report = detector.compare(old_df, new_df)
schema_changes = report.schema_changes
# Returns list of added/removed/changed columns
```

---

## 12. PySpark Engine

### Setup (Windows)

```powershell
# 1. Install Java (OpenJDK 21)
winget install --id Microsoft.OpenJDK.21

# 2. Install PySpark
pip install pyspark

# 3. Set environment variables
$env:JAVA_HOME = "C:\Program Files\Microsoft\jdk-21.0.11.10-hotspot"
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"
```

### Setup (macOS / Linux)

```bash
# Install Java
brew install openjdk@21         # macOS
sudo apt install openjdk-21-jdk # Ubuntu

# Install PySpark
pip install pyspark

# Set env vars
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk
export PATH=$JAVA_HOME/bin:$PATH
```

### Full PySpark example

```python
from pyspark.sql import SparkSession
import validatex as vx

# Start Spark
spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("ValidateX")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

# Create a Spark DataFrame
data = [(1, "Alice", 25), (2, "Bob", 30), (3, None, 22)]
df = spark.createDataFrame(data, ["id", "name", "age"])

# Build suite and validate
suite = (
    vx.ExpectationSuite("spark_checks")
    .add("expect_column_to_not_be_null",      column="id")
    .add("expect_column_values_to_be_unique", column="id")
    .add("expect_column_to_not_be_null",      column="name")
    .add("expect_column_values_to_be_between",
         column="age", kwargs={"min_value": 0, "max_value": 120})
)

result = vx.validate(df, suite, engine="spark")

print(result.summary())
print(f"Score: {result.compute_quality_score()}/100")
result.to_html("spark_report.html")

spark.stop()
```

> **Note:** The first Spark run takes 30–60 seconds to start the JVM. Subsequent runs in the same session are faster.

---

## 13. SQL Push-Down Engine

Validate data directly inside your database — no Python memory needed.
Generates optimized `SELECT COUNT(*)` and similar queries under the hood.

### Supported databases

| Database | Connection string |
|----------|-------------------|
| PostgreSQL | `postgresql://user:pass@host/db` |
| MySQL | `mysql+pymysql://user:pass@host/db` |
| SQLite | `sqlite:///path/to/file.db` |
| Snowflake | `snowflake://user:pass@account/db/schema` |
| BigQuery | `bigquery://project/dataset` |

### Install database driver

```bash
pip install "validatex[database]"   # installs SQLAlchemy
pip install psycopg2-binary          # PostgreSQL
pip install pymysql                  # MySQL
```

### Example

```python
import validatex as vx
from sqlalchemy import create_engine

engine = create_engine("postgresql://user:pass@localhost/mydb")

suite = (
    vx.ExpectationSuite("orders_db_checks")
    .add("expect_table_row_count_to_be_between",
         kwargs={"min_value": 1000, "max_value": 5000000})
    .add("expect_column_to_not_be_null",       column="order_id")
    .add("expect_column_values_to_be_unique",  column="order_id")
    .add("expect_column_values_to_be_between", column="amount",
         kwargs={"min_value": 0.01, "max_value": 99999.99})
)

# Pass table name (string) — NOT a DataFrame
result = vx.validate(
    data="orders",     # table name
    suite=suite,
    engine="sql",
    sql_engine=engine
)

print(f"Score: {result.compute_quality_score()}/100")
result.to_html("db_report.html")
```

---

## 14. Apache Airflow Integration

Gate your ETL pipelines based on data quality scores.

### Install

```bash
pip install apache-airflow
pip install validatex
```

### ValidateXOperator parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `suite` | `ExpectationSuite` | required | Your validation rules |
| `data_path` | `str` | required | Path to data file |
| `data_format` | `str` | `"csv"` | `"csv"` or `"parquet"` |
| `min_score` | `float` | `100.0` | Minimum quality score to pass |
| `report_path` | `str` | `None` | Where to save the HTML report |

### Example DAG

```python
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from validatex.integrations.airflow import ValidateXOperator
import validatex as vx

suite = (
    vx.ExpectationSuite("daily_orders_checks")
    .add("expect_column_to_not_be_null",       column="order_id")
    .add("expect_column_values_to_be_unique",  column="order_id")
    .add("expect_column_to_not_be_null",       column="email")
    .add("expect_column_values_to_match_regex",
         column="email", regex=r"^[\w.]+@[\w]+\.\w+$")
    .add("expect_column_values_to_be_between",
         column="amount", kwargs={"min_value": 0.01, "max_value": 99999.99})
)

with DAG(
    dag_id="validatex_etl_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    # Task 1: Extract data
    extract = BashOperator(
        task_id="extract_data",
        bash_command="python scripts/extract_orders.py"
    )

    # Task 2: Validate quality — FAILS DAG if score < 95
    validate = ValidateXOperator(
        task_id="validate_data_quality",
        suite=suite,
        data_path="/tmp/orders_today.csv",
        data_format="csv",
        min_score=95.0,
        report_path="/tmp/dq_report.html"
    )

    # Task 3: Only runs if validation passes
    publish = BashOperator(
        task_id="publish_to_warehouse",
        bash_command="python scripts/load_to_warehouse.py"
    )

    extract >> validate >> publish
```

### XCom output

The operator pushes these values to Airflow XComs for downstream tasks:

```python
{
    "validatex_score": 97.5,
    "passed_expectations": 5,
    "failed_expectations": 0,
    "report_path": "/tmp/dq_report.html"
}
```

---

## 15. CLI Reference

```bash
# Show all commands
validatex --help

# Initialize a project directory
validatex init

# Profile a dataset and optionally auto-suggest a suite
validatex profile \
    --data data.csv \
    --suggest \
    --output auto_suite.yaml

# Run validation and generate a report
validatex validate \
    --data data.csv \
    --suite suite.yaml \
    --report report.html

# Run a checkpoint (suite + data path defined in a YAML config)
validatex run --checkpoint checkpoint.yaml

# List all available expectation types
validatex list-expectations
```

### Checkpoint YAML

```yaml
# checkpoint.yaml
suite: suite.yaml
data: data/orders.csv
report: reports/orders_report.html
min_score: 90.0
```

```bash
validatex run --checkpoint checkpoint.yaml
```

---

## 16. All Available Expectations

### Column-Level (36 expectations)

| Expectation | Severity | Description |
|-------------|----------|-------------|
| `expect_column_to_exist` | Critical | Column exists in DataFrame |
| `expect_column_to_not_be_null` | Critical | No null values |
| `expect_column_values_to_be_unique` | Critical | All values unique |
| `expect_column_values_to_be_between` | Warning | Values within `[min_value, max_value]` |
| `expect_column_values_to_be_in_set` | Warning | Values in allowed set |
| `expect_column_values_to_not_be_in_set` | Warning | Values not in forbidden set |
| `expect_column_values_to_match_regex` | Warning | Values match regex pattern |
| `expect_column_values_to_not_match_regex` | Warning | Values do NOT match regex |
| `expect_column_values_to_be_of_type` | Warning | Column dtype matches expected |
| `expect_column_values_to_be_dateutil_parseable` | Warning | Values parseable as dates |
| `expect_column_values_to_be_valid_email` | Warning | Values are valid email addresses |
| `expect_column_values_to_be_json_parseable` | Warning | Values are parseable JSON |
| `expect_column_values_to_be_positive` | Warning | All values > 0 |
| `expect_column_values_to_be_negative` | Warning | All values < 0 |
| `expect_column_null_percentage_to_be_less_than` | Warning | Null rate < threshold |
| `expect_column_values_to_have_no_whitespace` | Warning | No leading/trailing whitespace |
| `expect_column_values_to_be_valid_url` | Warning | Valid HTTP/HTTPS/FTP URLs |
| `expect_column_values_to_be_valid_ip_address` | Warning | Valid IPv4/IPv6 addresses |
| `expect_column_values_to_be_valid_uuid` | Warning | Valid UUID (any version) |
| `expect_column_values_to_be_valid_iso_date` | Warning | Valid ISO 8601 dates |
| `expect_column_values_to_be_valid_phone_number` | Warning | Valid international phone |
| `expect_column_value_lengths_to_be_between` | Info | String lengths within range |
| `expect_column_value_lengths_to_equal` | Info | String lengths exact match |
| `expect_column_max_to_be_between` | Info | Column max within bounds |
| `expect_column_min_to_be_between` | Info | Column min within bounds |
| `expect_column_mean_to_be_between` | Info | Column mean within bounds |
| `expect_column_stdev_to_be_between` | Info | Column std dev within bounds |
| `expect_column_sum_to_be_between` | Info | Column sum within bounds |
| `expect_column_median_to_be_between` | Info | Column median within bounds |
| `expect_column_distinct_values_to_be_in_set` | Info | All distinct values in set |
| `expect_column_proportion_of_unique_values_to_be_between` | Info | Uniqueness ratio in range |
| `expect_column_quantile_values_to_be_between` | Info | Per-quantile range checks |
| `expect_column_values_to_be_in_range_of_std_devs` | Info | Z-score / outlier detection |
| `expect_column_correlation_to_be_between` | Info | Pearson correlation in range |
| `expect_column_values_to_be_all_uppercase` | Info | All values UPPERCASED |
| `expect_column_values_to_be_all_lowercase` | Info | All values lowercased |

### Table-Level (5 expectations)

| Expectation | Severity | Description |
|-------------|----------|-------------|
| `expect_table_row_count_to_equal` | Critical | Exact row count |
| `expect_table_row_count_to_be_between` | Critical | Row count in range |
| `expect_table_columns_to_match_ordered_list` | Critical | Column order matches exactly |
| `expect_table_columns_to_match_set` | Critical | Column names match (unordered) |
| `expect_table_column_count_to_equal` | Critical | Exact column count |

### Aggregate / Cross-Column (4 expectations)

| Expectation | Severity | Description |
|-------------|----------|-------------|
| `expect_column_pair_values_a_to_be_greater_than_b` | Warning | Column A > Column B row-wise |
| `expect_column_pair_values_to_be_equal` | Warning | Two columns equal row-wise |
| `expect_multicolumn_sum_to_equal` | Warning | Row-wise sum equals target |
| `expect_compound_columns_to_be_unique` | Critical | Compound key uniqueness |

### Sequential / Time-Series (2 expectations)

| Expectation | Severity | Description |
|-------------|----------|-------------|
| `expect_column_values_to_be_increasing` | Info | Monotonically increasing |
| `expect_column_values_to_be_decreasing` | Info | Monotonically decreasing |

### Conditional / Cross-Row (3 expectations)

| Expectation | Severity | Description |
|-------------|----------|-------------|
| `expect_column_values_to_be_null_when` | Warning | Column must be null given condition |
| `expect_column_values_to_be_not_null_when` | Critical | Column must not be null given condition |
| `expect_column_values_to_satisfy` | Warning | Custom Python lambda validation |

---

## 17. Custom Expectations

Create your own expectations by subclassing `Expectation`:

```python
from dataclasses import dataclass, field
from validatex.core.expectation import Expectation, register_expectation
from validatex.core.result import ExpectationResult

@register_expectation
@dataclass
class ExpectColumnValuesToStartWith(Expectation):
    """Expect all string values in a column to start with a given prefix."""

    expectation_type: str = field(
        init=False, default="expect_column_values_to_start_with"
    )
    prefix: str = ""

    def _validate_pandas(self, df) -> ExpectationResult:
        series = df[self.column].dropna().astype(str)
        total = len(series)
        bad_mask = ~series.str.startswith(self.prefix)
        unexpected_count = int(bad_mask.sum())
        pct = (unexpected_count / total * 100) if total > 0 else 0.0

        return self._build_result(
            success=(unexpected_count == 0),
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            unexpected_values=series[bad_mask].tolist()[:20],
        )
```

### Use your custom expectation

```python
suite = (
    vx.ExpectationSuite("custom_checks")
    .add("expect_column_values_to_start_with",
         column="product_id", prefix="PROD-")
)
result = vx.validate(df, suite)
```

---

## 18. Severity Levels & Quality Score

Every expectation has a severity that affects the weighted quality score.

| Severity | Weight | Typical Expectations |
|----------|--------|----------------------|
| Critical (red) | ×3 | Null checks, uniqueness, column existence, row count |
| Warning (yellow) | ×2 | Range checks, set membership, regex, type checks |
| Info (blue) | ×1 | Mean/stdev bounds, string lengths, distinct values |

**Score formula:**
```
Score = 100 × (weighted_passed / weighted_total)
```

A critical failure counts 3× more than an info failure.

### Override severity in Python

```python
.add("expect_column_mean_to_be_between", column="revenue",
     min_value=1000, max_value=50000,
     meta={"severity": "critical"})  # override info → critical
```

### Override severity in YAML

```yaml
- expectation_type: expect_column_mean_to_be_between
  column: revenue
  kwargs:
    min_value: 1000
    max_value: 50000
  meta:
    severity: critical
```

---

## 19. CI/CD Integration

### GitHub Actions

```yaml
name: Data Quality Check
on: [push, pull_request]

jobs:
  validate-data:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install ValidateX
        run: pip install validatex

      - name: Run Data Validation
        run: |
          validatex validate \
            --data data/production_data.csv \
            --suite tests/suite.yaml \
            --report dq_report.html

      - name: Upload Report
        uses: actions/upload-artifact@v4
        if: always()
        with:
          name: validatex-report
          path: dq_report.html
```

### GitLab CI

```yaml
validate-data:
  image: python:3.11
  script:
    - pip install validatex
    - validatex validate --data data.csv --suite suite.yaml --report report.html
  artifacts:
    paths:
      - report.html
    when: always
```

### Python script with exit code

```python
import sys
import validatex as vx
import pandas as pd

df = pd.read_csv("data.csv")
suite = vx.ExpectationSuite.load("suite.yaml")
result = vx.validate(df, suite)

score = result.compute_quality_score()
print(result.summary())

if score < 90.0:
    print(f"FAILED: Score {score:.1f} is below 90.0 threshold")
    sys.exit(1)   # non-zero exit fails the CI job

print("PASSED: Data quality is acceptable")
sys.exit(0)
```

---

## 20. Troubleshooting

### `ModuleNotFoundError: No module named 'validatex'`

Make sure you installed in the right environment:
```bash
pip install validatex
python -c "import validatex"
```

### PySpark: `JAVA_HOME is not set`

Set JAVA_HOME before running your script:
```bash
# Linux / macOS
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk
export PATH=$JAVA_HOME/bin:$PATH

# Windows PowerShell
$env:JAVA_HOME = "C:\Program Files\Microsoft\jdk-21.0.11.10-hotspot"
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"
```

### PySpark on Windows: `winutils.exe not found`

Download winutils for your Hadoop version:
```powershell
New-Item -ItemType Directory -Force -Path ".hadoop\bin"
Invoke-WebRequest `
  -Uri "https://github.com/steveloughran/winutils/raw/master/hadoop-3.0.0/bin/winutils.exe" `
  -OutFile ".hadoop\bin\winutils.exe"
$env:HADOOP_HOME = (Resolve-Path ".\.hadoop").Path
```

### SQL engine: `No module named 'sqlalchemy'`

```bash
pip install sqlalchemy
pip install psycopg2-binary   # for PostgreSQL
```

### Parquet: `Unable to find a usable engine`

```bash
pip install pyarrow   # or: pip install fastparquet
```

### Airflow operator: `ImportError: To use the ValidateXOperator...`

```bash
pip install apache-airflow
```

### `UnicodeEncodeError` on Windows terminal

Run Python with UTF-8 mode:
```powershell
$env:PYTHONUTF8 = "1"
python -X utf8 your_script.py
```

### Tests not collecting

Ensure test files are named `test_*.py` and pytest is configured:
```bash
pytest tests/ -v
```

---

## Further Reading

- [README](README.md) — Project overview and feature comparison
- [examples/basic_usage.py](examples/basic_usage.py) — Pandas validation walkthrough
- [examples/spark_data_quality.py](examples/spark_data_quality.py) — PySpark end-to-end example
- [examples/airflow_dag_example.py](examples/airflow_dag_example.py) — Full Airflow DAG
- [examples/drift_detection_example.py](examples/drift_detection_example.py) — Drift detection
- [examples/sample_suite.yaml](examples/sample_suite.yaml) — YAML suite template
- [CHANGELOG.md](CHANGELOG.md) — Version history

---

*Built with love by the ValidateX Team.
If this project helps you, give it a* ⭐ *on GitHub!*
