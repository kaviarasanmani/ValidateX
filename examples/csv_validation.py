"""
ValidateX — CSV Validation Example

Demonstrates loading a CSV file, validating it against a YAML suite,
and generating an HTML report.
"""

import pandas as pd

import validatex as vx
from validatex.datasources.csv_source import CSVDataSource


def main():
    # ── Create sample CSV ────────────────────────────────────────────────
    print("📦 Creating sample CSV...")
    df = pd.DataFrame({
        "order_id": range(1001, 1051),
        "customer_name": [f"Customer_{i}" for i in range(50)],
        "product": ["Widget A"] * 20 + ["Widget B"] * 15 + ["Widget C"] * 15,
        "quantity": [i % 10 + 1 for i in range(50)],
        "price": [round(9.99 + i * 0.5, 2) for i in range(50)],
        "status": ["shipped"] * 30 + ["processing"] * 10 + ["delivered"] * 10,
    })
    df.to_csv("sample_orders.csv", index=False)
    print("   Saved: sample_orders.csv\n")

    # ── Load via DataSource ──────────────────────────────────────────────
    source = CSVDataSource("sample_orders.csv", name="orders")
    data = source.load_pandas()
    print(f"📂 Loaded: {data.shape[0]} rows × {data.shape[1]} columns\n")

    # ── Build & validate ─────────────────────────────────────────────────
    suite = (
        vx.ExpectationSuite("order_validation")
        .add("expect_table_row_count_to_be_between", min_value=10, max_value=1000)
        .add("expect_column_to_not_be_null", column="order_id")
        .add("expect_column_values_to_be_unique", column="order_id")
        .add("expect_column_values_to_be_between", column="quantity", min_value=1, max_value=100)
        .add("expect_column_values_to_be_between", column="price", min_value=0, max_value=500)
        .add("expect_column_values_to_be_in_set",
             column="status",
             value_set=["shipped", "processing", "delivered", "returned"])
        .add("expect_column_values_to_be_in_set",
             column="product",
             value_set=["Widget A", "Widget B", "Widget C"])
    )

    result = vx.validate(data, suite, data_source="sample_orders.csv")
    print(result.summary())

    # ── Reports ──────────────────────────────────────────────────────────
    result.to_html("order_report.html")
    print("\n📊 HTML report: order_report.html")


if __name__ == "__main__":
    main()
