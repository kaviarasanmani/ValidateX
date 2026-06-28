# Validate Pandas DataFrames in Minutes ⚡

Pandas is the foundation of data analysis and machine learning in Python. However, raw CSV files or API responses often contain missing values, unexpected data types, or invalid strings.

In this guide, you will learn how to set up production-grade DataFrame validation using **ValidateX** in less than 5 minutes.

---

### Step 1: Installation
Install ValidateX via pip:
```bash
pip install validatex
```

### Step 2: Define your Expectations
Construct a fluent expectation suite to validate customer data:
```python
import pandas as pd
import validatex as vx

df = pd.read_csv("customers.csv")

suite = (
    vx.ExpectationSuite("customer_data_validation")
    .add("expect_column_to_not_be_null", column="user_id")
    .add("expect_column_values_to_be_unique", column="user_id")
    .add("expect_column_values_to_be_between", column="age", min_value=18, max_value=100)
    .add("expect_column_values_to_be_valid_email", column="email")
)
```

### Step 3: Run Validation and Generate Reports
Execute the validation runner and export modern HTML or JSON reports:
```python
result = vx.validate(df, suite)

print(f"Quality Score: {result.compute_quality_score()}/100")
result.to_html("reports/customer_report.html")
```
That's it! You have successfully gated your Pandas pipeline with automated quality scoring and HTML reports.
