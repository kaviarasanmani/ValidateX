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


### Q9: Does ValidateX have a Web Dashboard?
**Yes!** ValidateX includes a built-in, zero-dependency WSGI local web dashboard and static HTML export. Run:
```bash
validatex dashboard --port 8080 --browser
```
or generate an offline multi-dataset dashboard:
```bash
validatex dashboard --export dashboard.html
```

### Q10: How does ValidateX integrate with dbt?
ValidateX provides a bi-directional bridge between dbt and ValidateX:
- Convert dbt `schema.yml` models and tests into ValidateX `DataContract` objects (`validatex dbt import-schema`).
- Export ValidateX contracts back to dbt `schema.yml` test configurations (`validatex dbt export-schema`).
- Run native push-down SQL tests directly against your dbt warehouse (`validatex dbt test`).

### Q11: How do I migrate existing Great Expectations suites to ValidateX?
ValidateX includes an automated migrator CLI:
```bash
# Convert a GE suite into a ValidateX suite
validatex migrate --from-ge great_expectations/expectations/my_suite.json --output my_suite.yaml

# Convert a GE suite directly into a Data Contract
validatex migrate --from-ge my_suite.json --output my_contract.yaml --as-contract --name customer_master --version 1.0.0
```
