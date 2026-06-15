"""
ValidateX - PySpark Data Quality Example
=========================================
Creates a realistic e-commerce orders Spark DataFrame,
runs 15+ data quality checks, and generates an HTML + JSON report.
"""

import os
import sys

# ── Environment setup (Windows: point to Java & Hadoop winutils) ──────────────
os.environ.setdefault(
    "JAVA_HOME", r"C:\Program Files\Microsoft\jdk-21.0.11.10-hotspot"
)
os.environ.setdefault(
    "HADOOP_HOME", os.path.join(os.path.dirname(__file__), "..", ".hadoop")
)
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

# Force UTF-8 output on Windows terminals
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

# ── Imports ───────────────────────────────────────────────────────────────────
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, DoubleType, TimestampType,
)
from datetime import datetime
import validatex as vx

# ── 1. Start Spark ─────────────────────────────────────────────────────────────
print("\n" + "=" * 60)
print("  ValidateX - PySpark Data Quality Demo")
print("=" * 60)

print("\n[1/4] Starting Spark session...")
spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("ValidateX-DQ-Demo")
    .config("spark.sql.shuffle.partitions", "2")   # keep it lightweight
    .config("spark.ui.enabled", "false")           # disable Spark Web UI
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
print("      [OK] Spark started (local[2])")

# ── 2. Create a realistic e-commerce Orders DataFrame ─────────────────────────
print("\n[2/4] Building sample e-commerce Orders dataset...")

schema = StructType([
    StructField("order_id",       IntegerType(),   False),
    StructField("customer_id",    IntegerType(),   True),
    StructField("email",          StringType(),    True),
    StructField("status",         StringType(),    True),
    StructField("amount",         DoubleType(),    True),
    StructField("quantity",       IntegerType(),   True),
    StructField("product_id",     StringType(),    True),
    StructField("country_code",   StringType(),    True),
    StructField("created_at",     TimestampType(), True),
])

data = [
    # Good rows
    (1001, 201, "alice@shop.com",    "completed", 149.99,  2, "PROD-001", "US", datetime(2024, 1, 10)),
    (1002, 202, "bob@shop.com",      "completed", 299.50,  1, "PROD-002", "GB", datetime(2024, 1, 11)),
    (1003, 203, "carol@shop.com",    "shipped",    89.00,  3, "PROD-003", "CA", datetime(2024, 1, 12)),
    (1004, 204, "dave@shop.com",     "pending",    45.00,  1, "PROD-001", "AU", datetime(2024, 1, 13)),
    (1005, 205, "eve@shop.com",      "completed", 520.00,  4, "PROD-004", "US", datetime(2024, 1, 14)),
    (1006, 206, "frank@shop.com",    "completed", 199.99,  2, "PROD-002", "US", datetime(2024, 1, 15)),
    (1007, 207, "grace@shop.com",    "refunded",   75.00,  1, "PROD-005", "DE", datetime(2024, 1, 16)),
    (1008, 208, "henry@shop.com",    "completed", 330.00,  3, "PROD-003", "FR", datetime(2024, 1, 17)),
    (1009, 209, "iris@shop.com",     "shipped",   110.50,  2, "PROD-006", "US", datetime(2024, 1, 18)),
    (1010, 210, "jack@shop.com",     "pending",    60.00,  1, "PROD-001", "CA", datetime(2024, 1, 19)),
    # Intentionally bad rows to trigger failures
    (1011, None, "bad-email-format",  "unknown",  -50.00, -1, "PROD-999", "XX", datetime(2024, 1, 20)),  # null customer, bad email, negative amount, invalid status/country
    (1012, 212, "kate@shop.com",     "completed", 9999.00, 1, "PROD-002", "US", datetime(2024, 1, 21)), # amount too high
    (1001, 213, "dup@shop.com",      "shipped",   200.00,  2, "PROD-003", "GB", datetime(2024, 1, 22)), # duplicate order_id!
]

df = spark.createDataFrame(data, schema)
print(f"      [OK] Created DataFrame with {df.count()} rows, {len(df.columns)} columns")
print()
df.show(truncate=False)

# ── 3. Build Expectation Suite ────────────────────────────────────────────────
print("\n[3/4] Running data quality checks...")

suite = (
    vx.ExpectationSuite("ecommerce_orders_quality")

    # ── Table-level checks
    .add("expect_table_row_count_to_be_between",
         kwargs={"min_value": 5, "max_value": 100})
    .add("expect_table_columns_to_match_set",
         kwargs={"column_set": [
             "order_id", "customer_id", "email", "status",
             "amount", "quantity", "product_id", "country_code", "created_at"
         ], "exact_match": True})

    # ── order_id: must exist, no nulls, unique
    .add("expect_column_to_exist",              column="order_id")
    .add("expect_column_to_not_be_null",        column="order_id")
    .add("expect_column_values_to_be_unique",   column="order_id")

    # ── customer_id: no nulls
    .add("expect_column_to_not_be_null",        column="customer_id")

    # ── status: only known values
    .add("expect_column_values_to_be_in_set",
         column="status",
         kwargs={"value_set": ["pending", "shipped", "completed", "refunded"]})

    # ── amount: must be positive, within realistic bounds
    .add("expect_column_values_to_be_between",
         column="amount",
         kwargs={"min_value": 0.01, "max_value": 5000.0})
    .add("expect_column_to_not_be_null",        column="amount")

    # ── quantity: positive integers
    .add("expect_column_values_to_be_between",
         column="quantity",
         kwargs={"min_value": 1, "max_value": 100})

    # ── country_code: 2-letter ISO codes
    .add("expect_column_values_to_match_regex",
         column="country_code",
         kwargs={"regex": r"^[A-Z]{2}$"})

    # ── created_at: no nulls
    .add("expect_column_to_not_be_null",        column="created_at")
)

# ── 4. Validate against the Spark DataFrame ───────────────────────────────────
result = vx.validate(df, suite, engine="spark")

# ── 5. Print summary ──────────────────────────────────────────────────────────
print("\n" + "=" * 60)
print(result.summary())
print("=" * 60)

score = result.compute_quality_score()
print(f"\n  [SCORE] Data Quality Score : {score:.1f} / 100")

if score >= 90:
    grade = "EXCELLENT"
elif score >= 70:
    grade = "ACCEPTABLE"
else:
    grade = "NEEDS ATTENTION"
print(f"  Grade                 : {grade}\n")

# ── 6. Column health breakdown ─────────────────────────────────────────────────
print("  Column Health Breakdown:")
print(f"  {'Column':<20} {'Health':>8}  {'Passed':>8}  {'Checks':>8}")
print("  " + "-" * 50)
for col in result.column_health():
    bar = "█" * int(col.health_score / 10) + "░" * (10 - int(col.health_score / 10))
    print(f"  {col.column:<20} {col.health_score:>6.0f}%  {col.passed:>8}  {col.checks:>8}  {bar}")

# ── 7. Generate reports ───────────────────────────────────────────────────────
output_dir = os.path.join(os.path.dirname(__file__))
html_path  = os.path.join(output_dir, "spark_dq_report.html")
json_path  = os.path.join(output_dir, "spark_dq_report.json")

result.to_html(html_path)
result.to_json_file(json_path)

print(f"\n  [REPORT] HTML report -> {html_path}")
print(f"  [REPORT] JSON report -> {json_path}")
print("\n" + "=" * 60)
print("  Done! Open spark_dq_report.html in your browser.")
print("=" * 60 + "\n")

spark.stop()
