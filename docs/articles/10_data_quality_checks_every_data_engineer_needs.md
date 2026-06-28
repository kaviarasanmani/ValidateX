# 10 Data Quality Checks Every Data Engineer Needs 🛡️

Data pipelines are only as reliable as the data flowing through them. Bad data causes downstream dashboard outages, corrupts ML models, and leads to incorrect business decisions.

Here are the **10 essential data quality checks** every data engineering team should implement in production.

---

### 1. Primary Key Uniqueness Check
Every relational dataset must have a unique identifier. Duplicate primary keys break join operations and inflate aggregations.
```python
suite.add("expect_column_values_to_be_unique", column="customer_id")
```

### 2. Mandatory Non-Null Checks
Critical fields like user IDs, timestamps, and order totals should never contain missing values.
```python
suite.add("expect_column_to_not_be_null", column="order_id")
```

### 3. Numerical Range & Bound Validation
Ensure metrics fall within logical boundaries (e.g., customer age between 18 and 120).
```python
suite.add("expect_column_values_to_be_between", column="age", min_value=18, max_value=120)
```

### 4. Categorical Set Membership
Categorical columns like status or country codes must belong to an approved enumerated list.
```python
suite.add("expect_column_values_to_be_in_set", column="status", value_set=["ACTIVE", "PENDING", "CLOSED"])
```

### 5. Email & Pattern Regex Verification
Verify raw inputs adhere to expected string structures before loading into staging.
```python
suite.add("expect_column_values_to_be_valid_email", column="email")
```

### 6. Positive Financial Amounts
Financial ledgers, quantities, and item prices must strictly be greater than zero.
```python
suite.add("expect_column_values_to_be_positive", column="unit_price")
```

### 7. ISO Timestamp Standardization
Avoid date parsing failures by enforcing ISO 8601 formatting across event streams.
```python
suite.add("expect_column_values_to_be_valid_iso_date", column="timestamp")
```

### 8. Cross-Column Date Chronology
In transactional data, events must occur in logical order (e.g., discharge date after admission date).
```python
suite.add("expect_column_pair_values_a_to_be_greater_than_b", column="discharge_date", column_b="admission_date")
```

### 9. Population Stability & Data Drift (PSI)
Detect distribution shifts between historical training data and live inferencing streams.
```python
detector = vx.DriftDetector(psi_threshold=0.2)
report = detector.compare(baseline_df, current_df)
```

### 10. Table-Level Volume Expectations
Protect downstream tables from sudden empty ingestion batches or unintended massive data spikes.
```python
suite.add("expect_table_row_count_to_be_between", min_value=10_000, max_value=500_000)
```
