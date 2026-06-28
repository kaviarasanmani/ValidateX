# ValidateX Frequently Asked Questions (FAQ) ❓

---

### Q1: Does ValidateX send my data to external servers?
**No.** ValidateX is 100% open-source and executes completely locally or within your private cloud/VPC environment. No dataset rows or sensitive telemetry are ever transmitted outside your network.

### Q2: What execution engines are supported?
ValidateX supports four execution engines:
1. **Pandas** (Default, zero-copy for in-memory DataFrames)
2. **Polars** (High-performance multi-threaded Rust engine)
3. **PySpark** (Distributed Big Data cluster execution)
4. **Push-Down SQL** (Native SQLAlchemy queries for Postgres, Snowflake, BigQuery, and DuckDB)

### Q3: How does the Data Quality Score work?
ValidateX computes a weighted score between 0 and 100 based on expectation severity:
- **🔴 Critical (×3 weight)**: Null checks, uniqueness, column existence, row count.
- **🟡 Warning (×2 weight)**: Range bounds, regex patterns, set membership.
- **🔵 Info (×1 weight)**: Mean/stdev statistical checks, string lengths.

`Score = 100 × (weighted_passed / weighted_total)`

### Q4: Can I run ValidateX in my CI/CD pipelines?
**Yes!** ValidateX includes a built-in GitHub Action (`kaviarasanmani/ValidateX@main`) and CLI command (`validatex validate`) that exits with code `0` on success and `1` on failure, making it ideal for gating deployments.

### Q5: How do Slack and Microsoft Teams alerts work?
You can pass `--slack-webhook` or `--teams-webhook` to the CLI or call `.send_slack()` / `.send_teams()` on any `ValidationResult`. Notifications format rich Block Kit (Slack) or MessageCard (Teams) alerts with score breakdowns and failure details.

### Q6: Can I write custom validation rules?
**Yes.** Decorate any class with `@register_expectation` and inherit from `Expectation`. You can implement `_validate_pandas` and `_validate_polars` hooks to add custom business logic.
