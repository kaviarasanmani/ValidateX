# ValidateX Migration Guide 🚀

Switching from **Great Expectations** or **AWS Deequ** to **ValidateX** gives you faster validation speeds, 90% less setup boilerplate, and zero-configuration HTML reporting.

This guide provides step-by-step instructions and code equivalencies to migrate your existing data quality pipelines.

---

## 🔁 Migrating from Great Expectations

Great Expectations requires Data Contexts, Execution Environments, and Datasource configurations. In ValidateX, you validate any DataFrame or SQL table directly in 5 lines of code.

### Code Comparison

#### ❌ Before (Great Expectations)
```python
import great_expectations as ge
from great_expectations.core.expectation_suite import ExpectationSuite

context = ge.get_context()
datasource = context.sources.add_pandas("my_datasource")
asset = datasource.add_dataframe_asset("my_asset")
request = asset.build_batch_request(df)

validator = context.get_validator(
    batch_request=request,
    expectation_suite_name="my_suite"
)
validator.expect_column_values_to_not_be_null(column="user_id")
validator.expect_column_values_to_be_between(column="age", min_value=18, max_value=120)
validator.save_expectation_suite()

checkpoint = context.add_or_update_checkpoint(name="my_checkpoint", validator=validator)
results = checkpoint.run()
```

#### ✅ After (ValidateX)
```python
import validatex as vx

suite = (
    vx.ExpectationSuite("my_suite")
    .add("expect_column_to_not_be_null", column="user_id")
    .add("expect_column_values_to_be_between", column="age", min_value=18, max_value=120)
)

result = vx.validate(df, suite)
result.to_html("reports/validation_report.html")
```

### Expectation Mapping Table

| Great Expectations | ValidateX Equivalent |
|---|---|
| `expect_column_values_to_not_be_null` | `expect_column_to_not_be_null` |
| `expect_column_values_to_be_unique` | `expect_column_values_to_be_unique` |
| `expect_column_values_to_be_between` | `expect_column_values_to_be_between` |
| `expect_column_values_to_be_in_set` | `expect_column_values_to_be_in_set` |
| `expect_column_values_to_match_regex` | `expect_column_values_to_match_regex` |
| `expect_table_row_count_to_equal` | `expect_table_row_count_to_equal` |

---

## 🔁 Migrating from AWS Deequ

AWS Deequ is a Scala/Java library commonly used in PySpark jobs. ValidateX provides Python-native PySpark validation with native Python type output and zero JVM leaks.

### Code Comparison

#### ❌ Before (AWS Deequ PySpark)
```python
from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite, VerificationResult

check = Check(spark, CheckLevel.Error, "Review Check")
checkResult = (
    VerificationSuite(spark)
    .onData(df)
    .addCheck(
        check.hasSize(lambda x: x >= 100)
             .isComplete("user_id")
             .isUnique("user_id")
    )
    .run()
)
```

#### ✅ After (ValidateX PySpark)
```python
import validatex as vx

suite = (
    vx.ExpectationSuite("deequ_migration_suite")
    .add("expect_table_row_count_to_be_between", min_value=100)
    .add("expect_column_to_not_be_null", column="user_id")
    .add("expect_column_values_to_be_unique", column="user_id")
)

# Runs natively on PySpark DataFrames!
result = vx.validate(df, suite, engine="spark")
```


---

## ⚡ Automated Great Expectations Migrator (ValidateX v1.3.0+)

ValidateX provides an automated migration tool to translate Great Expectations JSON and YAML expectation suites directly into ValidateX suites or versioned Data Contracts.

### Command Line Migration

```bash
# Convert a GE JSON suite to a ValidateX YAML suite
validatex migrate --from-ge great_expectations/expectations/customers.json --output customers_suite.yaml

# Convert a GE suite directly into a ValidateX Data Contract
validatex migrate --from-ge customers.json --output customer_contract.yaml \
  --as-contract --name customer_master --version 1.0.0 --team customer_data
```

### Python API Migration

```python
from validatex.migrations import import_great_expectations_suite, ge_suite_to_contract

# Load GE suite dict or JSON file directly
suite = import_great_expectations_suite("great_expectations/expectations/customers.json")
suite.save_yaml("validatex_suite.yaml")

# Or convert directly into a DataContract object
contract = ge_suite_to_contract(
    "great_expectations/expectations/customers.json",
    name="customer_master",
    version="1.0.0",
    team="customer_ops",
)
contract.save_yaml("customer_contract.yaml")
```

---

## 🔄 Upgrading from ValidateX v1.2.0 to v1.3.0

ValidateX v1.3.0 is 100% backward-compatible with all existing v1.2.0 expectation suites and Python code.

### New Features to Adopt:
1. **Polars Engine**: Validate Polars DataFrames directly without converting to Pandas:
   ```python
   result = vx.validate(polars_df, suite)  # Auto-detected engine="polars"
   ```
2. **Data Contracts**: Upgrade suites into declarative contracts with schema validation and breaking-change detection:
   ```python
   contract = vx.DataContract.from_yaml("contract.yaml")
   result = vx.validate_contract(df, contract)
   ```
3. **Observability**: Track historical trends and alert on volume anomalies or failing expectation streaks by passing `store=...`:
   ```python
   result = vx.validate(df, suite, run_name="daily_run", store=".validatex/runs.db")
   ```
4. **dbt Integration**: Use ValidateX tests inside your dbt workflows:
   ```bash
   validatex dbt import-schema models/schema.yml --output contracts/
   validatex dbt test --models customers,orders
   ```
5. **Web Dashboard**: View your quality trends across datasets:
   ```bash
   validatex dashboard --port 8080 --browser
   ```
