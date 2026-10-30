"""Phase 3 - Data Quality Observability tests."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest
from sqlalchemy import create_engine

import validatex as vx
from validatex.contracts import DataContract, validate_contract
from validatex.core.exceptions import ValidateXError
from validatex.observability import (
    AlertPolicy,
    check_freshness,
    check_run_freshness,
    detect_volume_anomaly,
    detect_volume_anomaly_from_counts,
    dispatch_alerts,
    parse_age,
)
from validatex.storage.run_store import RunStore

try:
    import polars as pl

    HAS_POLARS = True
except ImportError:  # pragma: no cover
    HAS_POLARS = False


@pytest.fixture
def store(tmp_path) -> RunStore:
    return RunStore(db_path=str(tmp_path / "runs.db"))


def make_suite(name: str = "customers_suite", version: str = "1.0.0") -> vx.ExpectationSuite:
    return (
        vx.ExpectationSuite(name, version=version)
        .add("expect_column_to_not_be_null", column="id", meta={"severity": "critical"})
        .add("expect_column_values_to_be_unique", column="id")
        .add("expect_column_values_to_be_between", column="age", min_value=0, max_value=120)
    )


def df_ok(n: int = 10) -> pd.DataFrame:
    return pd.DataFrame({"id": list(range(n)), "age": [30] * n})


def df_bad(n: int = 10) -> pd.DataFrame:
    d = df_ok(n)
    d.loc[0, "age"] = 999  # breaks the range rule
    return d


# ---------------------------------------------------------------------------
# Run recording
# ---------------------------------------------------------------------------


def test_validate_records_run_with_required_fields(store):
    result = vx.validate(
        df_ok(), make_suite(), run_name="daily_customer_validation", dataset_name="customers", store=store
    )
    assert result.quality_score == 100.0  # spec: print(result.quality_score)
    assert result.run_id == 1 and result.run_name == "daily_customer_validation"

    run = store.get_runs(dataset_name="customers")[0]
    expected_keys = {
        "run_id",
        "dataset_name",
        "suite_version",
        "execution_time",
        "total_expectations",
        "passed_expectations",
        "failed_expectations",
        "quality_score",
        "timestamp",
    }
    assert expected_keys <= set(run)
    assert run["dataset_name"] == "customers"
    assert run["suite_version"] == "1.0.0"
    assert run["total_expectations"] == 3 and run["passed_expectations"] == 3 and run["failed_expectations"] == 0
    assert run["run_name"] == "daily_customer_validation"
    assert run["row_count"] == 10
    assert run["execution_time"] >= 0
    datetime.fromisoformat(run["timestamp"])  # valid ISO timestamp


def test_failed_run_recorded_with_counts_and_per_expectation_rows(store):
    result = vx.validate(df_bad(), make_suite(), dataset_name="customers", store=store)
    run = store.get_run(result.run_id)
    assert run["failed_expectations"] == 1 and run["passed_expectations"] == 2
    assert run["success"] == 0 and run["quality_score"] < 100
    rows = store.get_expectation_results(result.run_id)
    assert len(rows) == 3
    failed = [r for r in rows if not r["success"]]
    assert [r["expectation_type"] for r in failed] == ["expect_column_values_to_be_between"]
    assert failed[0]["severity"] == "warning" and failed[0]["unexpected_count"] == 1


def test_store_argument_accepts_path_true_and_instance(tmp_path, monkeypatch):
    p = str(tmp_path / "a" / "runs.db")
    assert vx.validate(df_ok(), make_suite(), store=p).run_id == 1
    monkeypatch.chdir(tmp_path)
    assert vx.validate(df_ok(), make_suite(), store=True).run_id == 1
    assert (tmp_path / ".validatex" / "runs.db").exists()
    assert vx.validate(df_ok(), make_suite()).run_id is None  # no recording unless asked


def test_dataset_name_defaults(store):
    vx.validate(df_ok(), make_suite("s1"), store=store)  # falls back to the suite name
    vx.validate(df_ok(), make_suite("s2"), data_source="orders.csv", store=store)  # then data_source
    names = {r["dataset_name"] for r in store.get_runs()}
    assert names == {"s1", "orders.csv"}


def test_run_metadata_in_json_report(store):
    result = vx.validate(df_ok(), make_suite(), run_name="nightly", dataset_name="customers", store=store)
    d = result.to_dict()
    assert d["run"] == {"run_id": 1, "run_name": "nightly", "dataset_name": "customers", "row_count": 10}


def test_filters_and_ordering(store):
    for i in range(3):
        vx.validate(df_ok(), make_suite(), run_name=f"r{i}", dataset_name="a", store=store)
    vx.validate(df_ok(), make_suite("other"), dataset_name="b", store=store)
    runs = store.get_runs(dataset_name="a")
    assert [r["run_name"] for r in runs] == ["r2", "r1", "r0"]  # newest first
    assert len(store.get_runs(limit=2)) == 2
    assert len(store.get_runs(run_name="r1")) == 1
    assert len(store.get_runs(suite_name="other")) == 1
    future = (datetime.now() + timedelta(days=1)).isoformat()
    assert store.get_runs(since=future) == []
    assert store.latest_run(dataset_name="a")["run_name"] == "r2"
    assert store.get_run(9999) is None


def test_works_with_polars_and_sql_engines(tmp_path):
    store = RunStore(db_path=str(tmp_path / "x.db"))
    if HAS_POLARS:
        r = vx.validate(pl.from_pandas(df_ok()), make_suite(), engine="polars", dataset_name="p", store=store)
        assert r.row_count == 10 and set(r.schema) == {"id", "age"}
    engine = create_engine("sqlite:///:memory:")
    df_ok().to_sql("t", engine, index=False)
    r = vx.validate("t", make_suite(), engine="sql", sql_engine=engine, dataset_name="s", store=store)
    assert r.row_count == 10 and set(r.schema) == {"id", "age"}
    assert store.get_runs(dataset_name="s")[0]["total_expectations"] == 3


def test_contract_runs_are_recorded_with_contract_identity(store):
    c = DataContract.create("cust", "2.1").add_field("id", "integer", nullable=False)
    result = validate_contract(df_ok(), c, dataset_name="customers", store=store, run_name="contract_run")
    row = store.get_run(result.validation.run_id)
    assert row["contract_name"] == "cust" and row["contract_version"] == "2.1.0"
    stored = store.get_expectation_results(row["run_id"])
    assert any(r["rule_id"] == "schema.id.exists" for r in stored)  # schema checks are tracked like rules


def test_legacy_database_is_migrated_in_place(tmp_path):
    path = str(tmp_path / "old.db")
    conn = sqlite3.connect(path)
    conn.execute("""
        CREATE TABLE validation_runs (
            run_id INTEGER PRIMARY KEY AUTOINCREMENT, suite_name TEXT NOT NULL, timestamp TEXT NOT NULL,
            quality_score REAL NOT NULL, total_expectations INTEGER NOT NULL, passed INTEGER NOT NULL,
            failed INTEGER NOT NULL, errors INTEGER NOT NULL, success_percent REAL NOT NULL,
            engine TEXT NOT NULL, data_source TEXT, duration_seconds REAL NOT NULL)
        """)
    conn.execute(
        "INSERT INTO validation_runs (suite_name, timestamp, quality_score, total_expectations, passed, failed, "
        "errors, success_percent, engine, data_source, duration_seconds) "
        "VALUES ('legacy', '2024-01-01T00:00:00', 80, 5, 4, 1, 0, 80, 'pandas', 'old.csv', 0.5)"
    )
    conn.commit()
    conn.close()

    store = RunStore(db_path=path)
    old = store.get_runs()[0]
    assert old["suite_name"] == "legacy" and old["dataset_name"] == "old.csv" and old["failed_expectations"] == 1
    assert vx.validate(df_ok(), make_suite(), dataset_name="new", store=store).run_id == 2
    assert store.get_history(suite_name="legacy")[0]["quality_score"] == 80  # original API still works


def test_save_to_store_backwards_compatible(tmp_path):
    result = vx.validate(df_ok(), make_suite())
    assert result.save_to_store(str(tmp_path / "s.db")) == 1


# ---------------------------------------------------------------------------
# Quality trends & failing expectation trends
# ---------------------------------------------------------------------------


def run_series(store, frames, dataset="customers"):
    for frame in frames:
        vx.validate(frame, make_suite(), dataset_name=dataset, store=store)


def test_quality_trend_statistics_and_direction(store):
    run_series(store, [df_bad(), df_bad(), df_bad(), df_ok(), df_ok(), df_ok()])
    t = store.quality_trend(dataset_name="customers")
    assert t.runs == 6 and t.latest == 100.0 and t.previous == 100.0 and t.delta == 0.0
    assert t.minimum < 100 and t.maximum == 100.0
    assert t.direction == "improving" and t.slope > 0
    assert t.pass_rate == 50.0
    assert [p["run_id"] for p in t.points] == [1, 2, 3, 4, 5, 6]  # oldest first
    assert t.to_dict()["direction"] == "improving"


def test_quality_trend_degrading_and_stable(store):
    run_series(store, [df_ok(), df_ok(), df_ok(), df_bad(), df_bad(), df_bad()], dataset="d1")
    assert store.quality_trend(dataset_name="d1").direction == "degrading"
    run_series(store, [df_ok()] * 5, dataset="d2")
    assert store.quality_trend(dataset_name="d2").direction == "stable"


def test_quality_trend_empty_history(store):
    t = store.quality_trend(dataset_name="nothing")
    assert t.runs == 0 and t.latest is None and t.delta is None and t.mean is None and t.direction == "stable"


def test_failed_expectation_trends_rank_and_streak(store):
    run_series(store, [df_ok(), df_ok(), df_bad(), df_bad(), df_bad(), df_bad()])
    trends = store.failed_expectation_trends(dataset_name="customers")
    assert len(trends) == 1  # only expectations that ever failed are listed
    t = trends[0]
    assert t.expectation_type == "expect_column_values_to_be_between" and t.column == "age"
    assert t.evaluations == 6 and t.failures == 4 and t.failure_rate == 66.7
    assert t.current_streak == 4 and t.last_failed_run_id == 6
    assert t.trend == "worsening"


def test_failed_expectation_trend_improving_and_limit(store):
    run_series(store, [df_bad(), df_bad(), df_bad(), df_ok(), df_ok(), df_ok()])
    assert store.failed_expectation_trends(dataset_name="customers")[0].trend == "improving"
    assert store.failed_expectation_trends(dataset_name="customers")[0].current_streak == 0
    only_recent = store.failed_expectation_trends(dataset_name="customers", limit=3)
    assert only_recent == []  # the last three runs were clean


def test_failed_expectation_trends_sorted_worst_first(store):
    suite = (
        vx.ExpectationSuite("s")
        .add("expect_column_values_to_be_between", column="a", min_value=0, max_value=1)
        .add("expect_column_values_to_be_between", column="b", min_value=0, max_value=1)
    )
    frames = [
        pd.DataFrame({"a": [5], "b": [0]}),
        pd.DataFrame({"a": [5], "b": [5]}),
        pd.DataFrame({"a": [5], "b": [0]}),
    ]
    for f in frames:
        vx.validate(f, suite, dataset_name="x", store=store)
    trends = store.failed_expectation_trends(dataset_name="x")
    assert [t.column for t in trends] == ["a", "b"] and trends[0].failures == 3 and trends[1].failures == 1
    assert len(store.failed_expectation_trends(dataset_name="x", top=1)) == 1


# ---------------------------------------------------------------------------
# Schema change history
# ---------------------------------------------------------------------------


def test_schema_change_history(store):
    suite = vx.ExpectationSuite("s").add("expect_table_row_count_to_be_between", min_value=0, max_value=100)
    base = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})
    vx.validate(base, suite, dataset_name="t", store=store)
    vx.validate(base, suite, dataset_name="t", store=store)  # unchanged: no new snapshot
    vx.validate(base.assign(country="IN"), suite, dataset_name="t", store=store)  # column added
    vx.validate(base.assign(country="IN").drop(columns=["name"]), suite, dataset_name="t", store=store)  # removed
    vx.validate(
        base.assign(country="IN").drop(columns=["name"]).astype({"id": "float64"}), suite, dataset_name="t", store=store
    )

    assert len(store.get_schema_snapshots("t")) == 4
    changes = store.schema_changes("t")
    assert [c.initial for c in changes] == [True, False, False, False]
    assert changes[0].added == ["id", "name"]
    assert changes[1].added == ["country"] and not changes[1].is_breaking
    assert changes[2].removed == ["name"] and changes[2].is_breaking
    assert changes[3].type_changes == {"id": {"from": "int64", "to": "float64"}} and changes[3].is_breaking
    assert changes[1].to_dict()["breaking"] is False


def test_schema_history_is_per_dataset(store):
    suite = vx.ExpectationSuite("s").add("expect_table_row_count_to_be_between", min_value=0, max_value=100)
    vx.validate(pd.DataFrame({"a": [1]}), suite, dataset_name="one", store=store)
    vx.validate(pd.DataFrame({"b": [1]}), suite, dataset_name="two", store=store)
    assert len(store.schema_changes("one")) == 1 and len(store.schema_changes("two")) == 1
    assert store.schema_changes("missing") == []


# ---------------------------------------------------------------------------
# Volume anomaly detection
# ---------------------------------------------------------------------------


def test_volume_spike_and_drop_detected():
    history = [1000, 1020, 990, 1010, 1005, 995, 1015]
    spike = detect_volume_anomaly_from_counts(history, 5000)
    assert spike.anomaly and spike.direction == "spike" and spike.pct_change > 300
    drop = detect_volume_anomaly_from_counts(history, 100)
    assert drop.anomaly and drop.direction == "drop"
    assert bool(drop) is True


def test_volume_normal_variation_not_flagged():
    history = [1000, 1020, 990, 1010, 1005, 995, 1015]
    ok = detect_volume_anomaly_from_counts(history, 1008)
    assert not ok.anomaly and ok.direction is None and ok.expected == 1005
    assert bool(ok) is False


def test_volume_needs_minimum_history():
    r = detect_volume_anomaly_from_counts([100, 100, 100], 1, min_history=5)
    assert not r.anomaly and r.insufficient_history and "history" in r.message


def test_volume_constant_history_uses_percent_floor():
    flat = [500] * 8
    assert not detect_volume_anomaly_from_counts(flat, 500).anomaly
    assert not detect_volume_anomaly_from_counts(flat, 510).anomaly  # +2% < 5% floor
    assert detect_volume_anomaly_from_counts(flat, 650).anomaly  # +30%
    zero = detect_volume_anomaly_from_counts(flat, 0)
    assert zero.anomaly and zero.direction == "drop" and zero.pct_change == -100.0


def test_volume_outlier_in_history_does_not_mask_detection():
    history = [100, 102, 98, 101, 99, 5000, 100, 103]  # robust median/MAD ignore the old outlier
    assert detect_volume_anomaly_from_counts(history, 300).anomaly


def test_volume_anomaly_from_store(store):
    for n in (100, 102, 98, 101, 99, 100):
        vx.validate(df_ok(n), make_suite(), dataset_name="v", store=store)
    assert detect_volume_anomaly(store, "v", 100).anomaly is False
    assert detect_volume_anomaly(store, "v", 1000).anomaly is True
    assert detect_volume_anomaly(store, "no_history", 1000).insufficient_history is True


# ---------------------------------------------------------------------------
# Freshness
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw,seconds",
    [("90s", 90), ("30m", 1800), ("24h", 86400), ("7d", 604800), ("2w", 1209600), (60, 60), ("1.5h", 5400)],
)
def test_parse_age(raw, seconds):
    assert parse_age(raw).total_seconds() == seconds


def test_parse_age_rejects_garbage():
    with pytest.raises(ValidateXError):
        parse_age("soon")
    assert parse_age(timedelta(minutes=5)).total_seconds() == 300


NOW = datetime(2026, 1, 10, 12, 0, tzinfo=timezone.utc)


def test_freshness_fresh_and_stale_pandas():
    df = pd.DataFrame({"updated_at": pd.to_datetime(["2026-01-10 08:00", "2026-01-10 11:30"])})
    fresh = check_freshness(df, "updated_at", "1h", now=NOW)
    assert fresh.fresh and bool(fresh) and fresh.age_seconds == 1800
    stale = check_freshness(df, "updated_at", "10m", now=NOW)
    assert not stale.fresh and "exceeds" in stale.message
    assert stale.to_dict()["fresh"] is False


def test_freshness_timezone_aware_and_string_timestamps():
    aware = pd.DataFrame({"t": pd.to_datetime(["2026-01-10T16:30:00+05:30"])})  # 11:00 UTC
    assert check_freshness(aware, "t", "2h", now=NOW).fresh
    strings = pd.DataFrame({"t": ["2026-01-09T00:00:00"]})
    assert not check_freshness(strings.assign(t=pd.to_datetime(strings["t"])), "t", "6h", now=NOW).fresh


def test_freshness_empty_and_all_null_are_not_fresh():
    empty = pd.DataFrame({"t": pd.Series([], dtype="datetime64[ns]")})
    r = check_freshness(empty, "t", "1d", now=NOW)
    assert not r.fresh and r.latest_timestamp is None
    nulls = pd.DataFrame({"t": pd.to_datetime([None, None])})
    assert not check_freshness(nulls, "t", "1d", now=NOW).fresh


def test_freshness_future_timestamp_counts_as_fresh():
    df = pd.DataFrame({"t": pd.to_datetime(["2026-01-11"])})
    assert check_freshness(df, "t", "1h", now=NOW).fresh


@pytest.mark.skipif(not HAS_POLARS, reason="Polars not installed")
def test_freshness_polars():
    df = pl.DataFrame({"t": [datetime(2026, 1, 10, 11, 0)]})
    assert check_freshness(df, "t", "2h", engine="polars", now=NOW).fresh
    assert not check_freshness(df, "t", "30m", engine="polars", now=NOW).fresh


def test_freshness_sql():
    engine = create_engine("sqlite:///:memory:")
    pd.DataFrame({"t": pd.to_datetime(["2026-01-10 11:00:00"])}).to_sql("ev", engine, index=False)
    assert check_freshness((engine, "ev"), "t", "2h", engine="sql", now=NOW).fresh
    assert not check_freshness((engine, "ev"), "t", "10m", engine="sql", now=NOW).fresh


def test_non_timestamp_column_raises_clear_error():
    with pytest.raises(ValidateXError):
        check_freshness(pd.DataFrame({"n": [1, 2]}), "n", "1h", now=NOW)


def test_run_freshness(store):
    assert not check_run_freshness(store, "customers", "1h").fresh  # nothing recorded yet
    vx.validate(df_ok(), make_suite(), dataset_name="customers", store=store)
    assert check_run_freshness(store, "customers", "1h").fresh
    later = datetime.now(timezone.utc) + timedelta(hours=5)
    stale = check_run_freshness(store, "customers", "1h", now=later)
    assert not stale.fresh and stale.age_seconds > 3600


# ---------------------------------------------------------------------------
# Alerting
# ---------------------------------------------------------------------------


def rules(alerts):
    return {a.rule for a in alerts}


def test_alert_min_quality_score_and_critical_failure(store):
    bad = df_ok().assign(id=[None] + list(range(1, 10)))
    policy = AlertPolicy(min_quality_score=99, alert_on_critical_failure=True)
    result = vx.validate(bad, make_suite(), dataset_name="c", store=store, alert_policy=policy)
    assert rules(result.alerts) == {"min_quality_score", "critical_failure"}
    assert all(a.severity == "critical" and a.dataset_name == "c" for a in result.alerts)
    assert result.to_dict()["alerts"][0]["rule"] in {"min_quality_score", "critical_failure"}


def test_no_alerts_for_healthy_run(store):
    policy = AlertPolicy(min_quality_score=90, alert_on_critical_failure=True, max_quality_drop=5, volume_anomaly=True)
    result = vx.validate(df_ok(), make_suite(), dataset_name="c", store=store, alert_policy=policy)
    assert result.alerts == []


def test_alert_quality_drop_vs_baseline(store):
    policy = AlertPolicy(max_quality_drop=5, baseline_runs=3)
    for _ in range(3):
        vx.validate(df_ok(), make_suite(), dataset_name="c", store=store, alert_policy=policy)
    result = vx.validate(df_bad(), make_suite(), dataset_name="c", store=store, alert_policy=policy)
    drop = [a for a in result.alerts if a.rule == "quality_drop"]
    assert drop and drop[0].severity == "warning" and drop[0].details["drop"] > 5


def test_alert_quality_drop_ignores_first_run_without_baseline(store):
    result = vx.validate(
        df_bad(), make_suite(), dataset_name="c", store=store, alert_policy=AlertPolicy(max_quality_drop=1)
    )
    assert result.alerts == []


def test_alert_consecutive_failures(store):
    policy = AlertPolicy(max_consecutive_failures=3)
    outcomes = [
        vx.validate(df_bad(), make_suite(), dataset_name="c", store=store, alert_policy=policy) for _ in range(3)
    ]
    assert [rules(r.alerts) for r in outcomes] == [set(), set(), {"consecutive_failures"}]
    # A passing run breaks the streak.
    ok = vx.validate(df_ok(), make_suite(), dataset_name="c", store=store, alert_policy=policy)
    assert ok.alerts == []


def test_alert_volume_anomaly(store):
    policy = AlertPolicy(volume_anomaly=True)
    for n in (100, 102, 98, 101, 99):
        vx.validate(df_ok(n), make_suite(), dataset_name="c", store=store, alert_policy=policy)
    result = vx.validate(df_ok(900), make_suite(), dataset_name="c", store=store, alert_policy=policy)
    vol = [a for a in result.alerts if a.rule == "volume_anomaly"]
    assert vol and vol[0].details["direction"] == "spike"


def test_alert_schema_change_severity(store):
    suite = vx.ExpectationSuite("s").add("expect_table_row_count_to_be_between", min_value=0, max_value=100)
    policy = AlertPolicy(alert_on_schema_change=True)
    base = pd.DataFrame({"id": [1], "name": ["a"]})
    first = vx.validate(base, suite, dataset_name="t", store=store, alert_policy=policy)
    assert first.alerts == []  # the initial schema is not a change
    same = vx.validate(base, suite, dataset_name="t", store=store, alert_policy=policy)
    assert same.alerts == []
    added = vx.validate(base.assign(x=1), suite, dataset_name="t", store=store, alert_policy=policy)
    assert [a.severity for a in added.alerts] == ["warning"]
    removed = vx.validate(base.drop(columns=["name"]), suite, dataset_name="t", store=store, alert_policy=policy)
    assert [a.severity for a in removed.alerts] == ["critical"] and "removed" in removed.alerts[0].message


def test_alert_stale_data_from_freshness(store):
    df = pd.DataFrame({"id": [1], "age": [1], "t": pd.to_datetime(["2020-01-01"])})
    stale = check_freshness(df, "t", "1d")
    result = vx.validate(df, make_suite(), dataset_name="c", store=store)
    alerts = AlertPolicy().evaluate(result, store=store, freshness=stale)
    assert rules(alerts) == {"stale_data"} and alerts[0].severity == "critical"


def test_alert_policy_works_without_store():
    policy = AlertPolicy(min_quality_score=100, alert_on_critical_failure=True, max_quality_drop=1, volume_anomaly=True)
    result = vx.validate(df_bad(), make_suite(), alert_policy=policy)
    assert rules(result.alerts) == {"min_quality_score"}  # history-based checks are skipped silently


def test_dispatch_alerts_posts_to_webhooks(monkeypatch):
    posted = []

    def fake_send(url, payload):
        posted.append((url, payload))
        return True

    monkeypatch.setattr("validatex.reporting.alerts.send_webhook_request", fake_send)
    from validatex.observability.alerting import Alert

    alerts = [Alert("min_quality_score", "critical", "score too low", dataset_name="c")]
    sent = dispatch_alerts(
        alerts, slack_webhook="https://hooks.slack.com/x", teams_webhook="https://example.webhook.office.com/y"
    )
    assert sent == {"slack": True, "teams": True}
    assert "score too low" in posted[0][1]["text"]
    assert posted[1][1]["@type"] == "MessageCard"
    assert dispatch_alerts([], slack_webhook="https://hooks.slack.com/x") == {}


def test_policy_webhooks_are_used_automatically(store, monkeypatch):
    posted = []
    monkeypatch.setattr("validatex.reporting.alerts.send_webhook_request", lambda u, p: posted.append(u) or True)
    policy = AlertPolicy(min_quality_score=100, slack_webhook="https://hooks.slack.com/x")
    vx.validate(df_bad(), make_suite(), dataset_name="c", store=store, alert_policy=policy)
    assert posted == ["https://hooks.slack.com/x"]
    posted.clear()
    vx.validate(df_ok(), make_suite(), dataset_name="c", store=store, alert_policy=policy)
    assert posted == []  # nothing to say when healthy


def test_cli_history_commands(store):
    from click.testing import CliRunner
    from validatex.cli.main import cli

    vx.validate(df_ok(), make_suite(), dataset_name="customers", store=store)
    vx.validate(df_bad().assign(extra=1), make_suite(), dataset_name="customers", store=store)
    runner = CliRunner()
    db = str(store.db_path)
    assert runner.invoke(cli, ["history", "-s", db, "-d", "customers"]).exit_code == 0
    assert runner.invoke(cli, ["history", "-s", db, "-d", "customers", "--failures"]).exit_code == 0
    assert runner.invoke(cli, ["history", "-s", db, "-d", "customers", "--schema"]).exit_code == 0
