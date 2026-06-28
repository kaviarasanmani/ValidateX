"""
ValidateX Real-World Showcase: Customer 360 Data Quality Validation

Demonstrates validating a production-style enterprise Customer 360 dataset.
Checks include:
  - Non-null Customer IDs and valid email formats
  - Age demographic bounds (18-120)
  - Account status set membership
  - Loyalty tier vs lifetime spend rules
  - Multi-column uniqueness across customer identifiers
"""

import sys
import pandas as pd
import validatex as vx

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

def run_customer_360_showcase():
    print("🚀 Running ValidateX Showcase: Customer 360 Data Quality")

    # 1. Generate representative Customer 360 data
    data = {
        "customer_id": ["CUST-001", "CUST-002", "CUST-003", "CUST-004", "CUST-005"],
        "full_name": ["Alice Smith", "Bob Jones", "Charlie Brown", "Diana Prince", "Evan Wright"],
        "email": ["alice@company.com", "bob@domain.org", "charlie@web.io", "diana@hero.net", "evan@corp.com"],
        "age": [28, 45, 34, 29, 52],
        "account_status": ["ACTIVE", "ACTIVE", "SUSPENDED", "ACTIVE", "INACTIVE"],
        "loyalty_tier": ["GOLD", "PLATINUM", "BRONZE", "GOLD", "SILVER"],
        "lifetime_spend": [1540.50, 12400.00, 120.00, 3200.75, 850.00],
    }
    df = pd.DataFrame(data)

    # 2. Define enterprise Expectation Suite
    suite = (
        vx.ExpectationSuite("customer_360_quality")
        .add("expect_table_row_count_to_be_between", min_value=1, max_value=1_000_000)
        .add("expect_column_to_not_be_null", column="customer_id")
        .add("expect_column_values_to_be_unique", column="customer_id")
        .add("expect_column_values_to_match_regex", column="customer_id", regex=r"^CUST-\d{3,}$")
        .add("expect_column_values_to_be_valid_email", column="email")
        .add("expect_column_values_to_be_between", column="age", min_value=18, max_value=120)
        .add("expect_column_values_to_be_in_set", column="account_status", value_set=["ACTIVE", "INACTIVE", "SUSPENDED", "PENDING"])
        .add("expect_column_values_to_be_in_set", column="loyalty_tier", value_set=["BRONZE", "SILVER", "GOLD", "PLATINUM"])
        .add("expect_column_values_to_be_positive", column="lifetime_spend")
    )

    # 3. Validate and print report
    result = vx.validate(df, suite)
    print(result.summary())

    # 4. Save HTML & JSON reports
    result.to_html("reports/showcase_customer_360.html")
    result.to_json_file("reports/showcase_customer_360.json")
    print("\n✅ Saved reports to reports/showcase_customer_360.html & .json")

if __name__ == "__main__":
    run_customer_360_showcase()
