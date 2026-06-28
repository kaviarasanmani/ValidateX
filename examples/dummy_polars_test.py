"""
Simple dummy test file for validating Polars DataFrames using ValidateX.
"""

import sys
import polars as pl
import validatex as vx

def run_dummy_checks():
    # Setup Windows console encoding if needed
    if sys.platform == "win32":
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except AttributeError:
            pass

    print("--- Running Polars Quality Checks Dummy Test ---")

    # 1. Create a dummy Polars DataFrame
    df = pl.DataFrame({
        "product_id": [101, 102, 103, 104, 105],
        "product_name": ["Widget A", "Widget B", "Widget C", None, "Widget E"],
        "price": [19.99, 29.99, -5.00, 49.99, 15.00],  # Negative price is an anomaly
        "in_stock": [True, True, False, True, False]
    })
    print("Dummy DataFrame:")
    print(df)
    print()

    # 2. Build Expectation Suite
    suite = (
        vx.ExpectationSuite("dummy_product_quality")
        # Check column existence
        .add("expect_column_to_exist", column="product_id")
        .add("expect_column_to_exist", column="product_name")
        # Check uniqueness on product_id
        .add("expect_column_values_to_be_unique", column="product_id")
        # Check nulls in product_name (one null exists, should fail)
        .add("expect_column_to_not_be_null", column="product_name")
        # Check values range on price (negative price should fail)
        .add("expect_column_values_to_be_between", column="price", min_value=0.0, max_value=100.0)
    )

    # 3. Validate
    print("Validating with Polars engine...")
    result = vx.validate(df, suite, engine="polars")

    # 4. Print Summary
    print(result.summary())

if __name__ == "__main__":
    run_dummy_checks()
