"""
ValidateX Real-World Showcase: Retail Inventory & Supply Chain Quality

Demonstrates auditing multi-warehouse inventory, SKU formats, reorder thresholds, and stock balances.
Checks include:
  - SKU code regex formatting (e.g. SKU-1001-BLK)
  - Non-negative stock quantities and reorder levels
  - Valid warehouse location codes
  - Positive unit costs
"""

import sys
import pandas as pd
import validatex as vx

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

def run_retail_showcase():
    print("🚀 Running ValidateX Showcase: Retail Inventory Quality Gate")

    # 1. Representative Retail Inventory dataset
    data = {
        "sku": ["SKU-1001-RED", "SKU-1002-BLU", "SKU-1003-BLK", "SKU-1004-WHT", "SKU-1005-GRN"],
        "product_name": ["Wireless Mouse", "Mechanical Keyboard", "USB-C Cable", "27-Inch Monitor", "Ergonomic Chair"],
        "warehouse": ["WH-EAST-01", "WH-WEST-02", "WH-EAST-01", "WH-SOUTH-03", "WH-WEST-02"],
        "stock_on_hand": [450, 120, 1500, 45, 85],
        "reorder_point": [100, 50, 300, 20, 30],
        "unit_cost": [12.50, 65.00, 3.20, 185.00, 140.00],
    }
    df = pd.DataFrame(data)

    # 2. Define Retail Inventory Suite
    suite = (
        vx.ExpectationSuite("retail_inventory_checks")
        .add("expect_column_to_not_be_null", column="sku")
        .add("expect_column_values_to_be_unique", column="sku")
        .add("expect_column_values_to_match_regex", column="sku", regex=r"^SKU-\d{4}-[A-Z]{3}$")
        .add("expect_column_to_not_be_null", column="warehouse")
        .add("expect_column_values_to_be_in_set", column="warehouse", value_set=["WH-EAST-01", "WH-WEST-02", "WH-SOUTH-03", "WH-NORTH-04"])
        .add("expect_column_values_to_be_positive", column="stock_on_hand")
        .add("expect_column_values_to_be_positive", column="unit_cost")
        .add("expect_column_pair_values_a_to_be_greater_than_b", column="stock_on_hand", column_b="reorder_point", allow_equal=True)
    )

    # 3. Validate
    result = vx.validate(df, suite)
    print(result.summary())

    # 4. Save reports
    result.to_html("reports/showcase_retail.html")
    result.to_json_file("reports/showcase_retail.json")
    print("\n✅ Saved reports to reports/showcase_retail.html & .json")

if __name__ == "__main__":
    run_retail_showcase()
