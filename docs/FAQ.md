# ValidateX Frequently Asked Questions (FAQ) ❓

---

### Q1: Does ValidateX send my data to external servers?
**No.** ValidateX is 100% open-source and executes completely locally or within your private cloud/VPC environment. No dataset rows or sensitive telemetry are ever transmitted outside your network.

### Q2: What execution engines are supported?
ValidateX supports four execution engines with verified semantic parity (see [`docs/CROSS_ENGINE_COMPATIBILITY.md`](CROSS_ENGINE_COMPATIBILITY.md)):
1. **Pandas** (Default, zero-copy for in-memory DataFrames)
2. **Polars** (High-performance multi-threaded Rust engine)
3. **PySpark** (Distributed Big Data cluster execution)
4. **Push-Down SQL** (Native SQLAlchemy queries for Postgres, Snowflake, BigQuery, SQLite, and DuckDB)

### Q3: How does the Data Quality Score work?
ValidateX computes a weighted score between 0 and 100 based on expectation severity:
- **🔴 Critical (×3 weight)**: Null checks, uniqueness, column existence, row count.
- **🟡 Warning (×2 weight)**: Range bounds, regex patterns, set membership.
- **🔵 Info (×1 weight)**: Mean/stdev statistical checks, string lengths.

`Score = 100 × (weighted_passed / weighted_total)`

Access it directly via `result.quality_score` (or `result.compute_quality_score()`).

### Q4: Can I run ValidateX in my CI/CD pipelines?
**Yes!** ValidateX includes a built-in GitHub Action (`kaviarasanmani/ValidateX@main`) and CLI commands (`validatex validate`, `validatex contract validate`, `validatex contract diff --fail-on-breaking --check-version`) that exit with code `0` on success and `1` on failure, making them ideal for gating deployments.

### Q5: How do Slack and Microsoft Teams alerts work?
You can pass `--slack-webhook` or `--teams-webhook` to the CLI, call `.send_slack()` / `.send_teams()` on any `ValidationResult`, or attach an `AlertPolicy(slack_webhook=..., teams_webhook=...)` to `vx.validate()`. Notifications format rich Block Kit (Slack) or MessageCard (Teams) payloads with score breakdowns and violated threshold rules.

### Q6: Can I write custom validation rules?
**Yes.** Decorate any class with `@register_expectation` and inherit from `Expectation`. You can implement `_validate_pandas`, `_validate_polars`, `_validate_spark`, and `_validate_sql` hooks to add custom business logic.

### Q7: How do Data Contracts differ from ExpectationSuites?
An `ExpectationSuite` is a list of runtime checks. A `DataContract` (`validatex.contracts`) is a versioned producer–consumer agreement that combines:
- Explicit **schema fields** (`name`, logical `type`, `nullable`)
- **Business quality rules** with `severity`
- **Team & steward ownership** (with `result.failures_by_owner()` routing)
- **Quality score & failure thresholds** (`minimum_quality_score`, `max_critical_failures`)
- **Breaking-change detection** (`vx.compare_contracts(old, new)`) and immutable semver registration (`ContractRegistry`).

### Q8: How do I track historical quality scores, freshness, and volume anomalies?
Pass `store=".validatex/runs.db"` (or a `RunStore` instance) to `vx.validate()` or `vx.validate_contract()`. You can then query `store.quality_trend()`, `store.failed_expectation_trends()`, `store.schema_changes()`, `vx.check_freshness()`, and `vx.detect_volume_anomaly()`, or inspect them from the terminal using `validatex history`.
