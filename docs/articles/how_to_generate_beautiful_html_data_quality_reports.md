# How to Generate Beautiful HTML Data Quality Reports 📊

Stakeholders and engineering managers need visibility into data health. Sending raw JSON logs or terminal outputs is rarely sufficient for executive reviews.

ValidateX includes a built-in, standalone HTML report generator that produces dark-themed reports with mini CSS bar charts, quality gauges, and interactive CSV/JSON export buttons.

---

## Generating an HTML Report

To generate an HTML report, invoke `.to_html()` on any `ValidationResult`:

```python
import pandas as pd
import validatex as vx

df = pd.read_csv("sales.csv")
suite = vx.ExpectationSuite("sales_checks").add("expect_column_to_not_be_null", column="order_id")

result = vx.validate(df, suite)
result.to_html("reports/sales_quality_report.html")
```

## Features of ValidateX HTML Reports

1. **Quality Score Gauge**: Displays overall dataset score (0–100) color-coded by performance threshold.
2. **Column Health Summary**: Displays per-column health percentages with mini CSS bar charts.
3. **Download Buttons**: Built-in interactive buttons allow users to export raw JSON, CSV, or copy summary text to clipboard without server calls.
4. **Standalone & Portable**: Generates self-contained HTML files with embedded CSS/JS that can be uploaded directly to AWS S3, Google Cloud Storage, or shared via email.
