# ValidateX Architectural Decisions & Safeguards (ADR) 🏛️

This document outlines the core architectural decisions, compute alignment strategies, and memory safeguards designed into **ValidateX**.

---

## 1. Compute Alignment & Cross-Engine Semantic Parity

### The Challenge
Validating across multiple engines (Pandas, Polars, PySpark, Push-Down SQL) presents a structural risk: divergent semantics for `NaN` (floating-point IEEE NaN), `None` (Python object), `null` (Arrow/Polars), and `NULL` (SQL), as well as empty dataset (`0`-row) behavior.

### The ValidateX Solution
* **Unified Coercion Protocol**: ValidateX enforces a standard coercion layer (`to_native()` in `validatex.core.result`).
* **Explicit Null Filtering**: Expectations use explicit engine-native null hooks (`.isna()` for Pandas, `.is_null()` for Polars, `.isNull()` for PySpark, `IS NULL` for SQL) rather than raw value comparisons.
* **Cross-Engine Introspection**: `validatex.core.introspect` normalizes physical dtypes across Pandas, Polars, PySpark, and SQLAlchemy into canonical logical types (`integer`, `float`, `string`, `boolean`, `date`, `timestamp`).
* **Type Normalization**: All observed outputs and unexpected values are cast to native Python primitives (`int`, `float`, `str`, `bool`) before serialization, guaranteeing identical JSON and HTML reports regardless of the execution engine.

---

## 2. Single-Node Reporting Bottleneck Safeguards

### The Challenge
When executing on massive PySpark clusters or distributed SQL databases (Snowflake/BigQuery), streaming millions of unexpected failing rows back to a single driver node to generate HTML reports can cause Driver Out-Of-Memory (OOM) failures.

### The ValidateX Solution
* **Driver Memory Capping**: All expectations strictly cap `unexpected_values` at `20` items using head slicing (`[:20]` in Python and `LIMIT 20` in SQL).
* **Summary Counts at Scale**: Aggregations (`element_count`, `unexpected_count`, `unexpected_percent`) are computed in parallel on the cluster/database nodes, and only scalar metrics (bytes, not DataFrames) are returned to the driver node.

---

## 3. State Management & Historical Observability (`RunStore`)

### The Challenge
Teams need historical quality scores, failing expectation streaks, schema evolution tracking, freshness checks, and volume anomaly detection across runs without deploying heavy external metadata infrastructure.

### The ValidateX Solution
* **Three-Table SQLite RunStore**: `RunStore` (`validatex.storage.run_store`) maintains three normalized tables in `.validatex/runs.db` with automatic in-place schema migration:
  1. `runs` — `run_id`, `dataset_name`, `run_name`, `suite_name`, `suite_version`, `engine`, `execution_time`, `total_expectations`, `passed_expectations`, `failed_expectations`, `quality_score`, `row_count`, and `timestamp`.
  2. `expectation_runs` — Per-expectation pass/fail outcomes, `severity`, and `unexpected_percent` linked by `run_id`.
  3. `schema_snapshots` — Per-run JSON snapshots of dataset columns and canonical logical types.
* **Built-In Analytics & Anomaly Detection**:
  * `quality_trend()` computes chronological scores, rolling mean, delta, OLS slope, and direction (`improving`, `degrading`, `stable`).
  * `failed_expectation_trends()` tracks failure rates, active consecutive failure streaks, and trajectories (`worsening`, `improving`, `steady`).
  * `schema_changes()` diffs consecutive schema snapshots to surface added, removed, and re-typed columns.
  * `detect_volume_anomaly()` uses median + MAD (Median Absolute Deviation) modified z-scores to flag row-count spikes and drops robustly even in the presence of historical outliers.
  * `AlertPolicy` evaluates quality thresholds, baseline drops, streaks, volume anomalies, schema changes, and freshness in one pass.

---

## 4. Data Contracts & Semantic Version Governance

### The Challenge
Traditional expectation suites check rules at runtime but lack formal schema contracts, ownership attribution, and static breaking-change detection when producers evolve schemas or relax rules.

### The ValidateX Solution
* **Declarative Data Contracts (`validatex.contracts`)**: Combines schema definitions (`name`, `type`, `nullable`), `quality_rules` with `severity`, contract/rule-level `owner` metadata, and `thresholds` in a single YAML/JSON artifact.
* **Polarity-Aware Breaking-Change Detection (`compare_contracts`)**: Classifies every change between two contract versions as `breaking` (major bump required), `non_breaking` (minor bump required), or `info` (patch bump required), accounting for rule polarity (e.g. expanding an allow-set is breaking for consumers, whereas expanding a forbid-set is non-breaking).
* **Immutable Registry (`ContractRegistry`)**: Persists published versions on disk, verifies content hashes to prevent silent mutation of published versions, and enforces semantic version progression.

---

## 5. Extensibility Framework Overheads

### The Challenge
A rigid extensibility architecture forces users to submit PRs directly to core engine implementations whenever custom business rules are needed.

### The ValidateX Solution
* **Modular Engine Dispatch Hooks**: Custom expectations inherit from `Expectation` and register via `@register_expectation`.
* **Fallback Hooks**: Users only need to implement `_validate_pandas()`. If an execution engine hook (like `_validate_polars()`) is not implemented, ValidateX gracefully raises a clear `NotImplementedError` wrapped in an `ExpectationResult` without crashing the suite.
