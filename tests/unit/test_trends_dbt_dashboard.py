"""
Unit tests for:
  1. HTML Report Embedded Historical Trend Charts, Failing Streaks, Schema Evolution & Alerts
  2. dbt Integration (schema.yml import/export, manifest.json parser, SQL/DataFrame validation runner, CLI)
  3. Multi-Dataset Web Dashboard (payload builder, static HTML export, WSGI JSON API routes, CLI)
  4. Great Expectations Suite & Contract Migrator (migrate_ge_suite, migrate_ge_to_contract, CLI)
"""

from __future__ import annotations

import io
import json
import pandas as pd
import pytest
from click.testing import CliRunner
from sqlalchemy import create_engine

import validatex as vx
from validatex.cli.main import cli


def _make_suite() -> vx.ExpectationSuite:
    return (
        vx.ExpectationSuite("orders_suite", meta={"version": "1.0.0"})
        .add("expect_column_to_not_be_null", column="order_id")
        .add("expect_column_values_to_be_unique", column="order_id")
        .add("expect_column_values_to_be_between", column="amount", min_value=0, max_value=1000)
    )


# ---------------------------------------------------------------------------
# 1. HTML Report Embedded Trend Charts
# ---------------------------------------------------------------------------


def test_html_report_embeds_svg_trend_chart_failing_streaks_and_schema_changes(tmp_path):
    store = vx.RunStore(str(tmp_path / "runs.db"))
    suite = _make_suite()

    df_ok = pd.DataFrame({"order_id": [1, 2, 3], "amount": [10.0, 20.0, 30.0]})
    df_bad = pd.DataFrame({"order_id": [1, 1, None], "amount": [10.0, 5000.0, 30.0], "promo": ["A", "B", "C"]})

    vx.validate(df_ok, suite, dataset_name="orders", run_name="r1", store=store)
    vx.validate(df_ok, suite, dataset_name="orders", run_name="r2", store=store)
    policy = vx.AlertPolicy(min_quality_score=95.0, alert_on_critical_failure=True)
    res3 = vx.validate(df_bad, suite, dataset_name="orders", run_name="r3", store=store, alert_policy=policy)

    html_path = tmp_path / "orders_report.html"
    # Automatic store pickup from res3._store
    res3.to_html(str(html_path))
    content = html_path.read_text(encoding="utf-8")

    assert 'id="historical-trends"' in content
    assert '<svg class="trend-svg"' in content
    assert "Top Failing Expectations" in content
    assert "Schema Evolution History" in content
    assert "promo" in content
    assert "Triggered Observability Alerts" in content
    assert "min_quality_score" in content


def test_contract_result_to_html_with_explicit_store(tmp_path):
    store = vx.RunStore(str(tmp_path / "runs.db"))
    contract = (
        vx.DataContract.create("users", version="1.0.0", team="core")
        .add_field("id", "integer", nullable=False)
        .add_rule("expect_column_values_to_be_unique", column="id")
    )
    df = pd.DataFrame({"id": [1, 2, 3]})
    c_res = vx.validate_contract(df, contract, store=store)
    out = tmp_path / "contract_report.html"
    c_res.to_html(str(out), store=store)
    html_text = out.read_text(encoding="utf-8")
    assert '<svg class="trend-svg"' in html_text


# ---------------------------------------------------------------------------
# 2. dbt Integration
# ---------------------------------------------------------------------------

SAMPLE_DBT_SCHEMA = """
version: 2

models:
  - name: fct_orders
    description: "Fact orders table"
    meta:
      version: "1.2.0"
      owner:
        team: analytics_eng
        steward: priya
      thresholds:
        minimum_quality_score: 90
    data_tests:
      - unique_combination_of_columns:
          combination_of_columns: [order_id, customer_id]
    columns:
      - name: order_id
        data_type: integer
        description: "Primary key"
        data_tests:
          - not_null
          - unique
      - name: customer_id
        data_type: integer
        data_tests:
          - not_null
      - name: status
        data_type: varchar
        data_tests:
          - accepted_values:
              values: ["placed", "shipped", "completed", "returned"]
      - name: amount
        data_type: numeric
        meta:
          validatex:
            - expectation: expect_column_values_to_be_between
              min_value: 0
              max_value: 10000
              severity: warning

sources:
  - name: raw_stripe
    meta:
      owner: payments_team
    tables:
      - name: payments
        columns:
          - name: payment_id
            data_type: integer
            tests:
              - not_null
              - unique
"""


def test_dbt_schema_import_and_export_roundtrip(tmp_path):
    contracts = vx.contracts_from_dbt_schema(SAMPLE_DBT_SCHEMA)
    assert [c.name for c in contracts] == ["fct_orders", "payments"]

    orders = vx.contract_from_dbt_model(SAMPLE_DBT_SCHEMA, "fct_orders")
    assert orders.version == "1.2.0"
    assert orders.owner.team == "analytics_eng"
    assert orders.thresholds.minimum_quality_score == 90
    field_map = {f.name: f for f in orders.schema}
    assert field_map["order_id"].nullable is False
    assert field_map["order_id"].type == "integer"
    assert field_map["status"].type == "string"
    assert field_map["amount"].type == "float"

    rule_types = [r.expectation for r in orders.quality_rules]
    assert "expect_column_values_to_be_unique" in rule_types
    assert "expect_column_values_to_be_in_set" in rule_types
    assert "expect_column_values_to_be_between" in rule_types
    assert "expect_compound_columns_to_be_unique" in rule_types

    # Export to dbt schema.yml and re-import
    out_schema = tmp_path / "exported_schema.yml"
    vx.save_dbt_schema(orders, out_schema)
    reloaded = vx.contract_from_dbt_model(out_schema, "fct_orders")
    assert reloaded.name == "fct_orders"
    assert reloaded.version == "1.2.0"
    assert len(reloaded.schema) == 4


def test_parse_dbt_manifest_and_run_sql_validations(tmp_path):
    manifest = {
        "nodes": {
            "model.jaffle.stg_customers": {
                "resource_type": "model",
                "name": "stg_customers",
                "description": "Staged customers",
                "meta": {"owner": "jaffle_team", "minimum_quality_score": 95},
                "columns": {
                    "customer_id": {"name": "customer_id", "data_type": "integer"},
                    "tier": {"name": "tier", "data_type": "string"},
                },
            },
            "test.jaffle.not_null_stg_customers_customer_id": {
                "resource_type": "test",
                "column_name": "customer_id",
                "depends_on": {"nodes": ["model.jaffle.stg_customers"]},
                "test_metadata": {"name": "not_null", "kwargs": {}},
            },
            "test.jaffle.unique_stg_customers_customer_id": {
                "resource_type": "test",
                "column_name": "customer_id",
                "depends_on": {"nodes": ["model.jaffle.stg_customers"]},
                "test_metadata": {"name": "unique", "kwargs": {}},
            },
            "test.jaffle.accepted_values_stg_customers_tier": {
                "resource_type": "test",
                "column_name": "tier",
                "depends_on": {"nodes": ["model.jaffle.stg_customers"]},
                "test_metadata": {"name": "accepted_values", "kwargs": {"values": ["free", "pro"]}},
            },
        }
    }
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    contracts = vx.parse_dbt_manifest(manifest_path)
    assert len(contracts) == 1 and contracts[0].name == "stg_customers"

    db_file = tmp_path / "warehouse.db"
    sql_engine = create_engine(f"sqlite:///{db_file}")
    pd.DataFrame({"customer_id": [1, 2, 3], "tier": ["free", "pro", "free"]}).to_sql(
        "stg_customers", sql_engine, index=False
    )

    store = vx.RunStore(str(tmp_path / "runs.db"))
    results = vx.run_dbt_validations(manifest_path, sql_engine, engine="sql", store=store)
    assert results["stg_customers"].success is True
    assert results["stg_customers"].quality_score == 100.0
    assert len(store.get_runs(dataset_name="stg_customers")) == 1


def test_cli_dbt_commands(tmp_path):
    schema_file = tmp_path / "schema.yml"
    schema_file.write_text(SAMPLE_DBT_SCHEMA, encoding="utf-8")
    contracts_dir = tmp_path / "contracts"
    exported_schema = tmp_path / "out_schema.yml"

    db_file = tmp_path / "dbt.db"
    sql_engine = create_engine(f"sqlite:///{db_file}")
    pd.DataFrame(
        {
            "order_id": [1, 2],
            "customer_id": [10, 20],
            "status": ["placed", "shipped"],
            "amount": [50.0, 120.0],
        }
    ).to_sql("fct_orders", sql_engine, index=False)

    runner = CliRunner()
    assert runner.invoke(cli, ["dbt", "import-schema", str(schema_file), "-o", str(contracts_dir)]).exit_code == 0
    assert (contracts_dir / "fct_orders.yaml").exists()
    assert (
        runner.invoke(
            cli, ["dbt", "export-schema", str(contracts_dir / "fct_orders.yaml"), "-o", str(exported_schema)]
        ).exit_code
        == 0
    )
    assert exported_schema.exists()
    res = runner.invoke(
        cli,
        [
            "dbt",
            "test",
            "-s",
            str(schema_file),
            "--db",
            f"sqlite:///{db_file}",
            "-m",
            "fct_orders",
        ],
    )
    assert res.exit_code == 0 and "PASS" in res.output


# ---------------------------------------------------------------------------
# 3. Multi-Dataset Web Dashboard
# ---------------------------------------------------------------------------


def _call_wsgi(app, path: str):
    status_holder = {}

    def start_response(status, headers):
        status_holder["status"] = status
        status_holder["headers"] = dict(headers)

    environ = {
        "REQUEST_METHOD": "GET",
        "PATH_INFO": path,
        "SERVER_NAME": "127.0.0.1",
        "SERVER_PORT": "8501",
        "wsgi.input": io.BytesIO(b""),
    }
    body = b"".join(app(environ, start_response))
    return status_holder["status"], status_holder["headers"], body


def test_dashboard_payload_html_export_and_wsgi_routes(tmp_path):
    store = vx.RunStore(str(tmp_path / "runs.db"))
    reg_dir = tmp_path / "contracts"
    reg = vx.ContractRegistry(reg_dir)

    c1 = (
        vx.DataContract.create("orders", version="1.0.0", team="commerce", minimum_quality_score=95)
        .add_field("order_id", "integer", nullable=False)
        .add_rule("expect_column_values_to_be_unique", column="order_id")
    )
    c2 = (
        c1.with_version("1.1.0")
        if hasattr(c1, "with_version")
        else vx.DataContract.from_dict({**c1.to_dict(), "version": "1.1.0"})
    )
    c2.add_field("amount", "float")
    reg.register(c1)
    reg.register(c2)

    suite = _make_suite()
    vx.validate(pd.DataFrame({"order_id": [1, 2], "amount": [10.0, 20.0]}), suite, dataset_name="orders", store=store)
    vx.validate(
        pd.DataFrame({"order_id": [1, 1], "amount": [-5.0, 20.0]}), suite, dataset_name="customers", store=store
    )

    payload = vx.build_dashboard_payload(store=store, registry=reg)
    assert payload["summary"]["total_datasets"] == 2
    assert payload["summary"]["total_runs"] == 2
    assert payload["summary"]["total_contracts"] == 1

    out_html = tmp_path / "dashboard.html"
    vx.export_dashboard_html(out_html, store=store, registry=reg)
    html_text = out_html.read_text(encoding="utf-8")
    assert "ValidateX Control Plane" in html_text
    assert "orders" in html_text and "customers" in html_text

    app = vx.create_wsgi_app(store=store, registry=reg)
    st, _, body = _call_wsgi(app, "/health")
    assert st == "200 OK" and json.loads(body)["status"] == "ok"

    st, _, body = _call_wsgi(app, "/api/summary")
    assert st == "200 OK" and json.loads(body)["total_datasets"] == 2

    st, _, body = _call_wsgi(app, "/api/datasets")
    assert st == "200 OK" and len(json.loads(body)) == 2

    st, _, body = _call_wsgi(app, "/api/datasets/orders")
    assert st == "200 OK" and json.loads(body)["dataset_name"] == "orders"

    st, _, _ = _call_wsgi(app, "/api/datasets/missing")
    assert st == "404 Not Found"

    st, _, body = _call_wsgi(app, "/api/contracts")
    assert st == "200 OK" and json.loads(body)[0]["name"] == "orders"

    st, headers, body = _call_wsgi(app, "/")
    assert st == "200 OK" and "text/html" in headers["Content-Type"]

    runner = CliRunner()
    cli_out = tmp_path / "cli_dashboard.html"
    res = runner.invoke(
        cli, ["dashboard", "-s", str(tmp_path / "runs.db"), "-r", str(reg_dir), "--export", str(cli_out)]
    )
    assert res.exit_code == 0 and cli_out.exists()


# ---------------------------------------------------------------------------
# 4. Great Expectations Migrator
# ---------------------------------------------------------------------------


def test_ge_migrator_suite_contract_and_cli(tmp_path):
    ge_suite = {
        "expectation_suite_name": "customer_ge_suite",
        "meta": {"great_expectations_version": "0.18.0"},
        "expectations": [
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "customer_id", "mostly": 0.99},
            },
            {
                "expectation_type": "expect_column_values_to_be_unique",
                "kwargs": {"column": "customer_id"},
            },
            {
                "expectation_type": "expect_column_values_to_be_of_type",
                "kwargs": {"column": "customer_id", "type_": "int64"},
            },
            {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {"column": "age", "min_value": 18, "max_value": 120},
            },
            {
                "expectation_type": "expect_unknown_custom_ge_rule",
                "kwargs": {"column": "age"},
            },
        ],
    }
    ge_file = tmp_path / "ge_suite.json"
    ge_file.write_text(json.dumps(ge_suite), encoding="utf-8")

    suite, report = vx.migrate_ge_suite(ge_file)
    assert report.total_ge_expectations == 5
    assert report.migrated_count == 4
    assert report.skipped_expectations == ["expect_unknown_custom_ge_rule"]

    df = pd.DataFrame({"customer_id": [1, 2, 3], "age": [25, 35, 45]})
    assert vx.validate(df, suite).success is True

    contract, creport = vx.migrate_ge_to_contract(ge_file, version="1.0.0", team="growth")
    assert contract.name == "customer_ge_suite"
    assert vx.validate_contract(df, contract).success is True

    runner = CliRunner()
    out_suite = tmp_path / "vx_suite.yaml"
    out_contract = tmp_path / "vx_contract.yaml"
    assert runner.invoke(cli, ["migrate", "--from-ge", str(ge_file), "-o", str(out_suite)]).exit_code == 0
    assert (
        runner.invoke(cli, ["migrate", "--from-ge", str(ge_file), "-o", str(out_contract), "--as-contract"]).exit_code
        == 0
    )
    assert out_suite.exists() and out_contract.exists()
