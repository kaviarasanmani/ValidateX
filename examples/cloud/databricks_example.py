"""
ValidateX Cloud Showcase: Databricks & Delta Lake PySpark Validation

Demonstrates executing distributed PySpark data quality validations on Databricks clusters.
"""

import sys
import validatex as vx

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

def run_databricks_validation(spark_session=None):
    print("🚀 Running ValidateX on Databricks Delta Lake...")

    # 1. Access active Spark session
    if spark_session is None:
        try:
            from pyspark.sql import SparkSession
            spark_session = SparkSession.builder.appName("ValidateXDatabricksDemo").getOrCreate()
        except ImportError:
            print("⚠️ PySpark not installed locally. Skipping live execution.")
            return

    # 2. Sample Delta Lake DataFrame
    df = spark_session.createDataFrame([
        (1, "ORD-9901", 150.0, "COMPLETED"),
        (2, "ORD-9902", 420.5, "COMPLETED"),
        (3, "ORD-9903", 89.0, "PENDING"),
    ], ["id", "order_number", "amount", "status"])

    # 3. Define Quality Suite
    suite = (
        vx.ExpectationSuite("databricks_delta_checks")
        .add("expect_column_to_not_be_null", column="id")
        .add("expect_column_values_to_be_unique", column="order_number")
        .add("expect_column_values_to_be_positive", column="amount")
        .add("expect_column_values_to_be_in_set", column="status", value_set=["COMPLETED", "PENDING", "CANCELLED"])
    )

    # 4. Validate distributed DataFrame
    result = vx.validate(df, suite, engine="spark")
    print(result.summary())

if __name__ == "__main__":
    run_databricks_validation()
