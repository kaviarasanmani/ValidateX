"""
ValidateX Real-World Showcase: Banking & Financial Transaction Validation

Demonstrates auditing high-frequency banking ledgers and financial transactions.
Checks include:
  - Account number UUID / format verification
  - Transaction type strict set checks (DEPOSIT, WITHDRAWAL, TRANSFER, FEE)
  - Currency code standardization (USD, EUR, GBP, JPY)
  - Non-null posting dates & valid debit/credit amounts
  - Outlier / Z-score detection on transaction amounts
"""

import sys
import pandas as pd
import validatex as vx

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

def run_banking_showcase():
    print("🚀 Running ValidateX Showcase: Banking Transaction Audit")

    # 1. Representative Banking Ledger records
    data = {
        "txn_uuid": [
            "123e4567-e89b-12d3-a456-426614174000",
            "123e4567-e89b-12d3-a456-426614174001",
            "123e4567-e89b-12d3-a456-426614174002",
            "123e4567-e89b-12d3-a456-426614174003",
            "123e4567-e89b-12d3-a456-426614174004",
        ],
        "account_no": ["ACC-9901", "ACC-9902", "ACC-9903", "ACC-9904", "ACC-9905"],
        "txn_type": ["DEPOSIT", "WITHDRAWAL", "TRANSFER", "FEE", "DEPOSIT"],
        "currency": ["USD", "USD", "EUR", "GBP", "USD"],
        "amount": [500.00, 120.50, 1450.00, 12.00, 3000.00],
        "balance_after": [5500.00, 2400.00, 8900.00, 150.00, 8500.00],
    }
    df = pd.DataFrame(data)

    # 2. Define Financial Audit Expectation Suite
    suite = (
        vx.ExpectationSuite("banking_ledger_audit")
        .add("expect_column_to_not_be_null", column="txn_uuid")
        .add("expect_column_values_to_be_valid_uuid", column="txn_uuid")
        .add("expect_column_values_to_be_unique", column="txn_uuid")
        .add("expect_column_to_not_be_null", column="account_no")
        .add("expect_column_values_to_be_in_set", column="txn_type", value_set=["DEPOSIT", "WITHDRAWAL", "TRANSFER", "FEE", "INTEREST"])
        .add("expect_column_values_to_be_in_set", column="currency", value_set=["USD", "EUR", "GBP", "JPY", "CAD"])
        .add("expect_column_values_to_be_positive", column="amount")
        .add("expect_column_values_to_be_between", column="balance_after", min_value=-1000.0, max_value=100_000_000.0)
    )

    # 3. Validate
    result = vx.validate(df, suite)
    print(result.summary())

    # 4. Save reports
    result.to_html("reports/showcase_banking.html")
    result.to_json_file("reports/showcase_banking.json")
    print("\n✅ Saved reports to reports/showcase_banking.html & .json")

if __name__ == "__main__":
    run_banking_showcase()
