# ValidateX Data Quality Observability Guide 🔭

ValidateX turns individual validation runs into a continuous historical monitoring system backed by a lightweight SQLite `RunStore`.

---

## 1. Recording Validation Runs

Every recorded execution persists:
- `run_id`, `dataset_name`, `run_name`, `suite_name`, `suite_version`
- `engine`, `execution_time`, `row_count`, `timestamp`
- `total_expectations`, `passed_expectations`, `failed_expectations`, `quality_score`
- Per-expectation outcomes (`expectation_runs`) and dataset schema snapshots (`schema_snapshots`)

```python
import validatex as vx

store = vx.RunStore(".validatex/runs.db")

result = vx.validate(
    data=df,
    suite=suite,
    run_name="daily_customer_validation",
    dataset_name="customers",
    store=store,
)

print(f"Run ID: {result.run_id} | Quality Score: {result.quality_score}")
```

---

## 2. Historical Quality Score Trends

```python
trend = store.quality_trend(dataset_name="customers", limit=30)
print(trend.direction)        # "improving", "degrading", or "stable"
print(trend.latest, trend.mean, trend.delta_from_mean, trend.slope)
```

---

## 3. Failed Expectation Trends & Streaks

Identify chronically failing or worsening rules across historical runs:

```python
for item in store.failed_expectation_trends(dataset_name="customers", limit=30):
    print(
        f"{item.expectation_type} ({item.column}): "
        f"{item.failures}/{item.evaluations} failed ({item.failure_rate}%), "
        f"streak={item.current_streak}, trend={item.trend}"
    )
```

---

## 4. Schema Change History

Every recorded run snapshots column names and canonical logical types, enabling automatic schema change auditing:

```python
for change in store.schema_changes("customers"):
    print(change.run_id, change.timestamp, "Added:", change.added, "Removed:", change.removed, "Types:", change.type_changes)
```

---

## 5. Data Freshness Checks

Check either the maximum timestamp inside a dataset column (across Pandas, Polars, PySpark, and SQL) or the time elapsed since the last validation run:

```python
# 1. Dataset timestamp column freshness
freshness = vx.check_freshness(df, column="updated_at", max_age="24h")
print(freshness.fresh, freshness.age_seconds, freshness.message)

# 2. Validation run freshness in RunStore
run_fresh = vx.check_run_freshness(store, dataset_name="customers", max_age="6h")
print(run_fresh.fresh, run_fresh.message)
```

---

## 6. Volume Anomaly Detection

Detect row-count spikes and drops using a robust median + MAD (Median Absolute Deviation) modified z-score with a minimum percentage-change floor:

```python
anomaly = vx.detect_volume_anomaly(
    store,
    dataset_name="customers",
    current_count=len(df),
    z_threshold=3.0,
    min_change_pct=20.0,
)
if anomaly.anomalous:
    print(f"Volume {anomaly.direction}: {anomaly.message}")
```

---

## 7. Threshold-Based Alerting (`AlertPolicy`)

Combine static score thresholds, critical failure gates, baseline quality drop detection, consecutive failure streaks, volume anomaly detection, schema change alerts, and freshness checks into a single policy:

```python
policy = vx.AlertPolicy(
    min_quality_score=95.0,
    alert_on_critical_failure=True,
    max_quality_drop=5.0,
    max_consecutive_failures=3,
    volume_anomaly=True,
    alert_on_schema_change=True,
    slack_webhook="https://hooks.slack.com/services/...",
)

result = vx.validate(
    data=df,
    suite=suite,
    dataset_name="customers",
    store=store,
    alert_policy=policy,
)

for alert in result.alerts:
    print(f"[{alert.severity.upper()}] {alert.rule}: {alert.message}")
```

---

## 8. CLI Observability Commands

```bash
# View recent validation runs and quality trend summary
validatex history --store .validatex/runs.db --dataset customers --limit 20

# View most frequently failing expectations and streaks
validatex history --store .validatex/runs.db --dataset customers --failures

# View schema evolution history
validatex history --store .validatex/runs.db --dataset customers --schema
```
