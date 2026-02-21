"""
ValidateX — Basic Usage Example

This example demonstrates:
1. Creating an expectation suite programmatically
2. Validating a Pandas DataFrame
3. Generating HTML and JSON reports
4. Using the data profiler
"""

import pandas as pd

import validatex as vx


def main():
    # ── 1. Create sample data ────────────────────────────────────────────
    print("📦 Creating sample dataset...")
    df = pd.DataFrame({
        "user_id": range(1, 101),
        "username": [f"user_{i}" for i in range(1, 101)],
        "email": [f"user{i}@example.com" for i in range(1, 101)],
        "age": [20 + (i % 50) for i in range(100)],
        "status": ["active"] * 60 + ["inactive"] * 25 + ["pending"] * 15,
        "balance": [round(100 + i * 10.5, 2) for i in range(100)],
        "signup_date": pd.date_range("2023-01-01", periods=100).astype(str).tolist(),
    })

    print(f"   Shape: {df.shape[0]} rows × {df.shape[1]} columns\n")

    # ── 2. Build an expectation suite ────────────────────────────────────
    print("📝 Building expectation suite...")
    suite = (
        vx.ExpectationSuite("user_data_quality")
        # Table-level
        .add("expect_table_row_count_to_be_between", min_value=50, max_value=500)
        .add("expect_table_column_count_to_equal", value=7)
        # Column existence
        .add("expect_column_to_exist", column="user_id")
        .add("expect_column_to_exist", column="email")
        # Null checks
        .add("expect_column_to_not_be_null", column="user_id")
        .add("expect_column_to_not_be_null", column="username")
        .add("expect_column_to_not_be_null", column="email")
        # Uniqueness
        .add("expect_column_values_to_be_unique", column="user_id")
        .add("expect_column_values_to_be_unique", column="email")
        # Range validation
        .add("expect_column_values_to_be_between", column="age", min_value=0, max_value=150)
        .add("expect_column_values_to_be_between", column="balance", min_value=0, max_value=10000)
        # Set membership
        .add("expect_column_values_to_be_in_set",
             column="status",
             value_set=["active", "inactive", "pending"])
        # Regex validation
        .add("expect_column_values_to_match_regex",
             column="email",
             regex=r"^[\w.]+@[\w]+\.\w+$")
        # Statistical checks
        .add("expect_column_mean_to_be_between", column="age", min_value=20, max_value=80)
        .add("expect_column_mean_to_be_between", column="balance", min_value=0, max_value=2000)
    )
    print(f"   Created suite with {len(suite)} expectations\n")

    # ── 3. Run validation ────────────────────────────────────────────────
    print("⏳ Running validation...")
    result = vx.validate(df, suite, data_source="sample_users_dataframe")

    # Print summary
    print(result.summary())

    # ── 4. Generate reports ──────────────────────────────────────────────
    result.to_html("validation_report.html")
    print("\n📊 HTML report saved: validation_report.html")

    result.to_json_file("validation_report.json")
    print("📄 JSON report saved: validation_report.json")

    # ── 5. Save the suite for reuse ──────────────────────────────────────
    suite.save("user_data_suite.yaml")
    print("💾 Suite saved: user_data_suite.yaml")

    # ── 6. Data profiling ────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("  Running Data Profiler...")
    print("=" * 60)
    profiler = vx.DataProfiler()
    profile = profiler.profile(df)
    print(profile.summary())

    # Auto-suggest expectations
    auto_suite = profiler.suggest_expectations(df, suite_name="auto_users")
    print(f"\n💡 Auto-suggested {len(auto_suite)} expectations")
    auto_suite.save("auto_suggested_suite.yaml")
    print("💾 Saved: auto_suggested_suite.yaml")

    print("\n✅ Done! Open validation_report.html in your browser.")


if __name__ == "__main__":
    main()
