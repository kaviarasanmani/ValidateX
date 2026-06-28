# ValidateX Architectural Decisions & Safeguards (ADR) 🏛️

This document outlines the core architectural decisions, compute alignment strategies, and memory safeguards designed into **ValidateX**.

---

## 1. Compute Alignment & Memory Management Leakage

### The Challenge
Validating across multiple engines (Pandas, Polars, PySpark, Push-Down SQL) presents a structural risk: divergent semantics for `NaN` (floating-point IEEE NaN), `None` (Python object), `null` (Arrow/Polars), and `NULL` (SQL).

### The ValidateX Solution
* **Unified Coercion Protocol**: ValidateX enforces a standard coercion layer (`to_native()` in `validatex.core.result`).
* **Explicit Null Filtering**: Expectations use explicit engine-native null hooks (`.isna()` for Pandas, `.is_null()` for Polars, `.isNull()` for PySpark, `IS NULL` for SQL) rather than raw value comparisons.
* **Type Normalization**: All observed outputs and unexpected values are cast to native Python primitives (`int`, `float`, `str`, `bool`) before serialization, guaranteeing identical JSON schemas regardless of the execution engine.

---

## 2. Single-Node Reporting Bottleneck Safeguards

### The Challenge
When executing on massive PySpark clusters or distributed SQL databases (Snowflake/BigQuery), streaming millions of unexpected failing rows back to a single driver node to generate HTML reports can cause Driver Out-Of-Memory (OOM) failures.

### The ValidateX Solution
* **Driver Memory Capping**: All expectations strictly cap `unexpected_values` at `20` items using head slicing (`[:20]` in Python and `LIMIT 20` in SQL).
* **Summary Counts at Scale**: Aggregations (`element_count`, `unexpected_count`, `unexpected_percent`) are computed in parallel on the cluster/database nodes, and only scalar metrics (bytes, not DataFrames) are returned to the driver node.

---

## 3. State Management & Time-Series RunStore

### The Challenge
Users need historical trend tracking and performance metrics across days or weeks. Storing state in heavy external metadata databases adds setup friction, while parsing raw HTML/JSON files repeatedly degrades query performance.

### The ValidateX Solution
* **Lightweight Local RunStore**: ValidateX includes a built-in zero-dependency SQLite metrics index (`RunStore` under `validatex.storage`).
* **Instant Trend Queries**: Invoking `.save_to_store()` records run metrics (score, pass/fail counts, engine, duration) into a local SQLite database (`.validatex/runs.db`). Users can query 30-day historical trends instantly without parsing raw report files.

---

## 4. Extensibility Framework Overheads

### The Challenge
A rigid extensibility architecture forces users to submit PRs directly to core engine implementations whenever custom business rules are needed.

### The ValidateX Solution
* **Modular Engine Dispatch Hooks**: Custom expectations inherit from `Expectation` and register via `@register_expectation`.
* **Fallback Hooks**: Users only need to implement `_validate_pandas()`. If an execution engine hook (like `_validate_polars()`) is not implemented, ValidateX gracefully falls back or raises a clear notification.
