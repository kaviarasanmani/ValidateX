# ValidateX Cross-Engine Compatibility Report (Phase 1)

**Version:** 1.0.0  
**Scope:** All 50 built-in Expectations across Pandas, Polars, PySpark, and SQL (PostgreSQL, Snowflake, BigQuery, SQLite, MySQL)  
**Target:** 100% semantic parity across all supported engine/rule combinations, with execution modes and dialect nuances explicitly documented.

---

## 1. Core Scenario Compatibility Matrix

Every scenario below is verified by automated tests in `tests/unit/test_phase1_trustworthiness.py` and `tests/unit/test_cross_engine_consistency.py`.

| Test Case | Pandas | Polars | PySpark | SQL (PostgreSQL / SQLite) | Snowflake (`ValidateXSnowflake`) | BigQuery (`ValidateXBigQuery`) | Semantic Contract |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :--- |
| **Null Check** (`expect_column_to_not_be_null`, `expect_column_null_percentage_to_be_less_than`) | ✅ Pass | ✅ Pass | ✅ Pass | ✅ Pass (Native Push-Down) | ✅ Pass (Native Push-Down) | ✅ Pass (Native Push-Down) | Counts `NULL`/`NaN`/`None` rows out of total table rows (`COUNT(*)`). Empty table (`0` rows) has `0` nulls (`0.0%`, `PASSED`). |
| **Unique Check** (`expect_column_values_to_be_unique`) | ✅ Pass | ✅ Pass | ✅ Pass | ✅ Pass (Native Push-Down) | ✅ Pass (Native Push-Down) | ✅ Pass (Native Push-Down) | `element_count` equals total rows (`len(df)`). A single `NULL` is treated as 1 distinct value; multiple `NULL`s are counted as duplicates (`keep=False`). |
| **Range Check** (`expect_column_values_to_be_between`, `min`/`max`/`mean`/`sum`/`median`) | ✅ Pass | ✅ Pass | ✅ Pass | ✅ Pass (Native Push-Down) | ✅ Pass (Native Push-Down) | ✅ Pass (Native Push-Down) | Row-level range checks ignore `NULL`s (`element_count` = non-null count) and support inclusive (`[min, max]`) and strict (`(min, max)`) bounds. Aggregate range checks fail (`success=False, observed_value=None`) when a column has `0` non-null values. |
| **Duplicate Check** (`expect_column_values_to_be_unique`, `expect_compound_columns_to_be_unique`) | ✅ Pass | ✅ Pass | ✅ Pass | ✅ Pass (Native Push-Down) | ✅ Pass (Native Push-Down) | ✅ Pass (Native Push-Down) | Counts all rows belonging to any key or compound key with multiplicity `> 1` (`SUM(cnt) WHERE cnt > 1`). |
| **Empty Dataset (`0` rows)** (All 50 expectations) | ✅ Pass | ✅ Pass | ✅ Pass | ✅ Pass | ✅ Pass | ✅ Pass | Zero unhandled exceptions across all 50 expectations. Row-level filters report `element_count=0, unexpected_count=0, success=True`; statistical aggregates (`min`, `max`, `mean`, `stdev`, `sum`, `median`, `quantile`, `correlation`) report `observed_value=None, success=False`. |
| **Null-Only Columns** (All 50 expectations) | ✅ Pass | ✅ Pass | ✅ Pass | ✅ Pass | ✅ Pass | ✅ Pass | Zero unhandled exceptions across all 50 expectations. `NaN` never leaks into JSON/HTML reports (`to_native()` normalizes `NaN` to `None`). |

---

## 2. Full 50-Expectation Cross-Engine Support Matrix

**Legend:**
- **Native**: Vectorized / DataFrame-native or Push-Down SQL (`SELECT ...` aggregate query executed inside the database).
- **SQL Fallback**: Safely queries the table/subquery via SQLAlchemy and evaluates in-memory via Pandas (`Expectation._validate_sql`), ensuring 100% rule coverage even for complex regex, Python UDF, or multi-quantile checks.

| # | Expectation Type | Category | Severity | Pandas | Polars | PySpark | SQL / Postgres / Snowflake / BigQuery |
| :--- | :--- | :--- | :--- | :---: | :---: | :---: | :---: |
| 1 | `expect_table_row_count_to_equal` | Table | Critical | Native | Native | Native | Native Push-Down SQL |
| 2 | `expect_table_row_count_to_be_between` | Table | Critical | Native | Native | Native | Native Push-Down SQL |
| 3 | `expect_table_columns_to_match_ordered_list` | Table | Critical | Native | Native | Native | Native Push-Down SQL (`LIMIT 0`) |
| 4 | `expect_table_columns_to_match_set` | Table | Critical | Native | Native | Native | Native Push-Down SQL (`LIMIT 0`) |
| 5 | `expect_table_column_count_to_equal` | Table | Critical | Native | Native | Native | Native Push-Down SQL (`LIMIT 0`) |
| 6 | `expect_column_to_exist` | Column | Critical | Native | Native | Native | Native Push-Down SQL |
| 7 | `expect_column_to_not_be_null` | Column | Critical | Native | Native | Native | Native Push-Down SQL |
| 8 | `expect_column_values_to_be_unique` | Column | Critical | Native | Native | Native | Native Push-Down SQL |
| 9 | `expect_column_values_to_be_between` | Column | Warning | Native | Native | Native | Native Push-Down SQL |
| 10 | `expect_column_values_to_be_in_set` | Column | Warning | Native | Native | Native | Native Push-Down SQL |
| 11 | `expect_column_values_to_not_be_in_set` | Column | Warning | Native | Native | Native | Native Push-Down SQL |
| 12 | `expect_column_values_to_match_regex` | Column | Warning | Native | Native | Native (`rlike`) | SQL Fallback |
| 13 | `expect_column_values_to_be_of_type` | Column | Warning | Native | Native | Native | SQL Fallback *(see Note 1)* |
| 14 | `expect_column_values_to_be_dateutil_parseable` | Column | Warning | Native (`dateutil`) | Native (`dateutil`) | Native (`dateutil` UDF) | SQL Fallback |
| 15 | `expect_column_value_lengths_to_be_between` | Column | Info | Native | Native | Native | Native Push-Down SQL |
| 16 | `expect_column_max_to_be_between` | Column | Info | Native | Native | Native | Native Push-Down SQL |
| 17 | `expect_column_min_to_be_between` | Column | Info | Native | Native | Native | Native Push-Down SQL |
| 18 | `expect_column_mean_to_be_between` | Column | Info | Native | Native | Native | Native Push-Down SQL |
| 19 | `expect_column_stdev_to_be_between` | Column | Info | Native (`ddof=1`) | Native (`ddof=1`) | Native (`F.stddev`) | SQL Fallback (`ddof=1`) *(see Note 2)* |
| 20 | `expect_column_distinct_values_to_be_in_set` | Column | Info | Native | Native | Native | Native Push-Down SQL |
| 21 | `expect_column_proportion_of_unique_values_to_be_between` | Column | Info | Native | Native | Native | Native Push-Down SQL |
| 22 | `expect_column_values_to_not_match_regex` | Advanced | Warning | Native | Native | Native (`rlike`) | SQL Fallback |
| 23 | `expect_column_values_to_be_valid_email` | Advanced | Warning | Native | Native | Native (`rlike`) | SQL Fallback |
| 24 | `expect_column_values_to_be_json_parseable` | Advanced | Warning | Native | Native | Native (UDF) | SQL Fallback |
| 25 | `expect_column_sum_to_be_between` | Advanced | Info | Native | Native | Native | Native Push-Down SQL |
| 26 | `expect_column_median_to_be_between` | Advanced | Info | Native | Native | Native (`approxQuantile`) | SQL Fallback *(see Note 3)* |
| 27 | `expect_column_value_lengths_to_equal` | Advanced | Info | Native | Native | Native | Native Push-Down SQL |
| 28 | `expect_column_quantile_values_to_be_between` | Statistical | Info | Native | Native | Native (`approxQuantile`) | SQL Fallback *(see Note 3)* |
| 29 | `expect_column_null_percentage_to_be_less_than` | Statistical | Warning | Native | Native | Native | Native Push-Down SQL |
| 30 | `expect_column_correlation_to_be_between` | Statistical | Info | Native | Native | Native (`stat.corr`) | SQL Fallback |
| 31 | `expect_column_values_to_have_no_whitespace` | Statistical | Warning | Native | Native | Native (`F.trim`) | Native Push-Down SQL |
| 32 | `expect_column_values_to_be_positive` | Statistical | Warning | Native | Native | Native | Native Push-Down SQL |
| 33 | `expect_column_values_to_be_negative` | Statistical | Warning | Native | Native | Native | Native Push-Down SQL |
| 34 | `expect_column_values_to_be_in_range_of_std_devs` | Statistical | Info | Native | Native | Native | SQL Fallback |
| 35 | `expect_column_values_to_be_valid_url` | Format | Warning | Native | Native | Native (`rlike`) | SQL Fallback |
| 36 | `expect_column_values_to_be_valid_ip_address` | Format | Warning | Native | Native | Native (`rlike`) | SQL Fallback |
| 37 | `expect_column_values_to_be_valid_uuid` | Format | Warning | Native | Native | Native (`rlike`) | SQL Fallback |
| 38 | `expect_column_values_to_be_valid_iso_date` | Format | Warning | Native | Native | Native (`rlike`) | SQL Fallback |
| 39 | `expect_column_values_to_be_valid_phone_number` | Format | Warning | Native | Native | Native (`rlike`) | SQL Fallback |
| 40 | `expect_column_values_to_be_all_uppercase` | Format | Info | Native | Native | Native (`F.upper`) | Native Push-Down SQL |
| 41 | `expect_column_values_to_be_all_lowercase` | Format | Info | Native | Native | Native (`F.lower`) | Native Push-Down SQL |
| 42 | `expect_column_pair_values_a_to_be_greater_than_b` | Aggregate | Warning | Native | Native | Native | Native Push-Down SQL |
| 43 | `expect_column_pair_values_to_be_equal` | Aggregate | Warning | Native | Native | Native | Native Push-Down SQL |
| 44 | `expect_multicolumn_sum_to_equal` | Aggregate | Warning | Native | Native | Native | Native Push-Down SQL |
| 45 | `expect_compound_columns_to_be_unique` | Aggregate | Critical | Native | Native | Native | Native Push-Down SQL |
| 46 | `expect_column_values_to_be_increasing` | Sequential | Info | Native | Native | Native (`Window.lag`) | SQL Fallback |
| 47 | `expect_column_values_to_be_decreasing` | Sequential | Info | Native | Native | Native (`Window.lag`) | SQL Fallback |
| 48 | `expect_column_values_to_be_null_when` | Conditional | Warning | Native | Native | Native | Native Push-Down SQL |
| 49 | `expect_column_values_to_be_not_null_when` | Conditional | Critical | Native | Native | Native | Native Push-Down SQL |
| 50 | `expect_column_values_to_satisfy` | Conditional | Warning | Native | Native | Native (UDF) | SQL Fallback *(Python API only)* |

---

## 3. Documented Engine Nuances & Limitations

1. **Note 1 — `expect_column_values_to_be_of_type` on Empty / All-Null SQLite Tables:**
   - Each engine uses its native type system string representation (`int64` in Pandas, `Int64` in Polars, `LongType()` in PySpark; matched case-insensitively via substring).
   - In SQLite, columns use dynamic typing; reading an empty (`0`-row) or all-`NULL` table into Pandas without explicit SQLAlchemy type reflection yields `object` dtype.
2. **Note 2 — Standard Deviation (`expect_column_stdev_to_be_between` and `expect_column_values_to_be_in_range_of_std_devs`):**
   - All engines standardize on **sample standard deviation (`ddof=1`)**. Columns with fewer than 2 non-null rows (`total < 2`) have undefined sample standard deviation (`observed_value=None`).
   - Because SQLite does not ship a built-in `STDDEV_SAMP` function, SQL execution delegates to the Pandas `ddof=1` implementation for exact numerical parity.
3. **Note 3 — Quantiles & Medians in PySpark (`approxQuantile`):**
   - PySpark computes medians and quantiles using the Greenwald-Khanna algorithm (`approxQuantile` with relative error `0.0001`–`0.001`) to avoid full-cluster single-partition shuffles on massive datasets. On very large datasets (`> 100,000` rows), quantile values may differ by up to `0.01%` from exact in-memory sorting.
4. **Note 4 — `expect_column_values_to_satisfy` Serialization:**
   - Because `expect_column_values_to_satisfy` accepts an arbitrary Python `Callable` (`lambda`), it is designed for the Python API (`ExpectationSuite.add(...)`) and cannot be serialized to static YAML/JSON config files.

---

## 4. Phase 1 Audit Fixes Summary

During the Phase 1 audit of all 50 expectations, the following cross-engine discrepancies and edge cases were identified and resolved:

1. **Duplicate `NULL` Handling in `expect_column_values_to_be_unique` (SQL vs Pandas/Polars/Spark):**
   - *Before:* `_validate_sql` used `COUNT(col)` and `WHERE col IS NOT NULL`, ignoring `NULL`s and reporting `element_count=2, unexpected_count=0, success=True` on `[1, 2, None, None]` while Pandas, Polars, and PySpark reported `element_count=4, unexpected_count=2, success=False`.
   - *After:* `_validate_sql` groups by `col` (including `NULL` groups) and computes `total`, `distinct_count`, and `dup_count` identically across all 4 engines.
2. **Empty Dataset (`0` Rows) & Null-Only Column Parity on Statistical Expectations:**
   - *Before:* `expect_column_sum_to_be_between` and `expect_column_median_to_be_between` returned `success=True` on empty Polars/Spark/SQL datasets even when `min_value > 0`, while Pandas returned `success=False` (or `NaN`). `expect_column_quantile_values_to_be_between` and `expect_column_values_to_be_in_range_of_std_devs` raised `TypeError: float() argument must be a string or a real number, not 'NoneType'` on empty or 1-row Polars/Spark series.
   - *After:* All statistical range expectations (`max`, `min`, `mean`, `stdev`, `sum`, `median`, `quantile`, `correlation`) uniformly return `success=False, observed_value=None` when a column has `0` valid data points (or `< 2` for `stdev` and `correlation`), and `to_native()` converts any floating-point `NaN` to `None`.
3. **Date Parsing Parity in `expect_column_values_to_be_dateutil_parseable`:**
   - *Before:* Pandas used `pd.to_datetime(..., errors="coerce")` while Polars and Spark used `dateutil.parser.parse(str(x))`, causing mixed date formats (e.g. `["2026-06-15", "June 16, 2026"]`) to behave differently across engines.
   - *After:* All engines use `dateutil.parser.parse(str(x))` for 100% identical date parsing behavior.
4. **Conditional `not_equal` Null Semantics (`expect_column_values_to_be_null_when` / `not_null_when`):**
   - *Before:* In Pandas, `df[cond_col] != cond_val` evaluated to `True` for `NaN`/`None` rows in `cond_col`, whereas SQL `WHERE cond_col != :val`, Polars, and PySpark excluded `NULL` condition rows.
   - *After:* Pandas now uses `df[cond_col].notna() & (df[cond_col] != cond_val)` for exact parity with SQL, Polars, and PySpark.
5. **JSON and HTML Report Agreement:**
   - HTML report now renders `Column Coverage`, `Suite Version`, and `Owner` in the metadata grid and embeds the exact `ValidationResult.to_dict()` JSON payload, verified by automated parity tests.
