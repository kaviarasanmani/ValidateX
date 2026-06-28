"""
ValidateX — Polars Validation Engine Usage Example

This example demonstrates:
1. Creating a Polars DataFrame
2. Building a comprehensive expectation suite
3. Running quality checks using the Polars validation engine
4. Reviewing the validation report and generating output files
"""

import sys
import polars as pl
import validatex as vx


def main():
    # Reconfigure stdout to support UTF-8 characters (like emojis and colors) on Windows
    if sys.platform == "win32":
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except AttributeError:
            pass

    # ── 1. Create sample Polars data ──────────────────────────────────────
    print("[1] Creating sample Polars dataset...")
    df = pl.DataFrame({
        "user_id": list(range(1, 101)),
        "username": [f"user_{i}" for i in range(1, 101)],
        "email": [f"user{i}@example.com" for i in range(1, 101)],
        "age": [20 + (i % 50) for i in range(100)],
        "status": ["active"] * 60 + ["inactive"] * 25 + ["pending"] * 15,
        "balance": [round(100.0 + i * 10.5, 2) for i in range(100)],
        "signup_date": [f"2026-06-{10 + (i % 20):02d}" for i in range(100)],
        "uuid": [f"123e4567-e89b-12d3-a456-4266141740{i:02d}" for i in range(100)],
        "website": ["https://validatex.io" if i % 2 == 0 else "http://example.com" for i in range(100)]
    })

    print(f"   Shape: {df.height} rows x {df.width} columns\n")

    # ── 2. Build an expectation suite ────────────────────────────────────
    print("[2] Building expectation suite...")
    suite = (
        vx.ExpectationSuite("polars_user_data_quality")
        # Table-level expectations
        .add("expect_table_row_count_to_be_between", min_value=50, max_value=500)
        .add("expect_table_column_count_to_equal", value=9)
        .add("expect_table_columns_to_match_ordered_list", column_list=[
            "user_id", "username", "email", "age", "status", "balance", "signup_date", "uuid", "website"
        ])
        # Column presence & Null checks
        .add("expect_column_to_exist", column="user_id")
        .add("expect_column_to_exist", column="email")
        .add("expect_column_to_not_be_null", column="user_id")
        .add("expect_column_to_not_be_null", column="username")
        # Uniqueness & Ranges
        .add("expect_column_values_to_be_unique", column="user_id")
        .add("expect_column_values_to_be_unique", column="email")
        .add("expect_column_values_to_be_between", column="age", min_value=15, max_value=120)
        # Set membership & String formats
        .add("expect_column_values_to_be_in_set", column="status", value_set=["active", "inactive", "pending"])
        .add("expect_column_values_to_be_valid_url", column="website")
        .add("expect_column_values_to_be_valid_uuid", column="uuid")
        .add("expect_column_values_to_be_valid_iso_date", column="signup_date")
        # Advanced format validation
        .add("expect_column_values_to_be_valid_email", column="email")
        .add("expect_column_values_to_be_of_type", column="user_id", expected_type="Int")
        # Statistical checks
        .add("expect_column_mean_to_be_between", column="age", min_value=30, max_value=60)
        .add("expect_column_sum_to_be_between", column="balance", min_value=50000, max_value=80000)
        # Aggregate comparison (Will purposefully fail for demonstration)
        .add("expect_column_pair_values_a_to_be_greater_than_b", column_a="age", column_b="user_id", or_equal=True)
    )
    print(f"   Created suite with {len(suite)} expectations\n")

    # ── 3. Run validation using Polars engine ─────────────────────────────
    print("[3] Running validation on Polars engine...")
    result = vx.validate(df, suite, engine="polars", data_source="polars_sample_users")

    # Print summary
    print(result.summary())

    # ── 4. Generate reports ──────────────────────────────────────────────
    result.to_html("examples/polars_dq_report.html")
    print("\n[4] HTML report saved: examples/polars_dq_report.html")

    result.to_json_file("examples/polars_dq_report.json")
    print("   JSON report saved: examples/polars_dq_report.json")

    print("\n[*] Done! Quality check run completed successfully.")


if __name__ == "__main__":
    main()
