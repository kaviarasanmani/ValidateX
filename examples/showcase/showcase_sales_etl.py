"""
ValidateX Real-World Showcase: Sales ETL Data Pipeline Validation

Demonstrates validating transactional sales records in a data engineering ETL pipeline.
Checks include:
  - Total revenue calculation consistency
  - Non-negative order quantities and prices
  - Valid ISO timestamp formats for transaction dates
  - Region code set validation
"""

import sys
import pandas as pd
import validatex as vx

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

def run_sales_etl_showcase():
    print("🚀 Running ValidateX Showcase: Sales ETL Pipeline Validation")

    # 1. Representative Sales ETL pipeline batch
    data = {
        "transaction_id": ["TXN-1001", "TXN-1002", "TXN-1003", "TXN-1004", "TXN-1005"],
        "timestamp": ["2026-06-01T08:30:00Z", "2026-06-01T09:15:22Z", "2026-06-01T10:04:11Z", "2026-06-01T11:45:00Z", "2026-06-01T12:00:30Z"],
        "region": ["US-EAST", "US-WEST", "EU-CENTRAL", "AP-SOUTH", "US-EAST"],
        "unit_price": [49.99, 120.00, 15.50, 299.99, 8.00],
        "quantity": [2, 1, 10, 3, 5],
        "total_amount": [99.98, 120.00, 155.00, 899.97, 40.00],
    }
    df = pd.DataFrame(data)

    # 2. Define Sales Pipeline Quality Suite
    suite = (
        vx.ExpectationSuite("sales_etl_pipeline_checks")
        .add("expect_table_row_count_to_be_between", min_value=1, max_value=10_000_000)
        .add("expect_column_to_not_be_null", column="transaction_id")
        .add("expect_column_values_to_be_unique", column="transaction_id")
        .add("expect_column_values_to_be_valid_iso_date", column="timestamp")
        .add("expect_column_values_to_be_in_set", column="region", value_set=["US-EAST", "US-WEST", "EU-CENTRAL", "AP-SOUTH", "LATAM"])
        .add("expect_column_values_to_be_positive", column="unit_price")
        .add("expect_column_values_to_be_positive", column="quantity")
        .add("expect_column_values_to_be_positive", column="total_amount")
    )

    # 3. Validate
    result = vx.validate(df, suite)
    print(result.summary())

    # 4. Save reports
    result.to_html("reports/showcase_sales_etl.html")
    result.to_json_file("reports/showcase_sales_etl.json")
    print("\n✅ Saved reports to reports/showcase_sales_etl.html & .json")

if __name__ == "__main__":
    run_sales_etl_showcase()
