"""
ValidateX CLI — command-line interface for running validations.

Usage examples
--------------
::

    # Run a checkpoint
    validatex run --checkpoint checkpoint.yaml

    # Validate a CSV with a suite
    validatex validate --data data.csv --suite suite.yaml --report report.html

    # Profile a dataset
    validatex profile --data data.csv

    # List all available expectations
    validatex list-expectations

    # Initialize a new project
    validatex init
"""

from __future__ import annotations

import sys
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

import click
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich import box

console = Console()


@click.group()
@click.version_option(version="1.0.0", prog_name="ValidateX")
def cli():
    """ValidateX — Data Quality Validation Framework 🚀"""


# ── validate ──────────────────────────────────────────────────────────────


@cli.command()
@click.option("--data", "-d", required=True, help="Path to data file (CSV, Parquet).")
@click.option("--suite", "-s", required=True, help="Path to expectation suite (YAML/JSON).")
@click.option("--engine", "-e", default="pandas", help="Engine: pandas, spark, or polars.")
@click.option("--report", "-r", default=None, help="Output HTML report path.")
@click.option("--json-report", "-j", default=None, help="Output JSON report path.")
@click.option("--slack-webhook", default=None, help="Slack webhook URL for alerts.")
@click.option("--teams-webhook", default=None, help="Microsoft Teams webhook URL for alerts.")
@click.option(
    "--notify-on",
    default="failure",
    type=click.Choice(["failure", "always"]),
    help="Trigger alerts on 'failure' or 'always'.",
)
def validate(data, suite, engine, report, json_report, slack_webhook, teams_webhook, notify_on):
    """Validate a dataset against an expectation suite."""
    # Ensure expectations are registered
    import validatex.expectations  # noqa: F401
    from validatex.core.suite import ExpectationSuite
    from validatex.core.validator import Validator

    console.print(
        Panel(
            "[bold magenta]ValidateX[/bold magenta] — Running Validation",
            box=box.DOUBLE_EDGE,
            style="bold blue",
        )
    )

    # Load suite
    console.print(f"  📝 Loading suite: [cyan]{suite}[/cyan]")
    exp_suite = ExpectationSuite.load(suite)
    console.print(f"     Found [bold]{len(exp_suite)}[/bold] expectations")

    # Load data
    console.print(f"  📂 Loading data:  [cyan]{data}[/cyan]")
    df = _load_data_file(data, engine)
    console.print(f"     Shape: [bold]{df.shape[0]}[/bold] rows × [bold]{df.shape[1]}[/bold] columns")

    # Run validation
    console.print("  ⏳ Running validations...\n")
    validator = Validator(exp_suite, engine=engine)
    result = validator.run(df, data_source=data)

    # Print summary
    console.print(result.summary())

    # Save reports
    if report:
        result.to_html(report)
        console.print(f"\n  📊 HTML report saved: [green]{report}[/green]")

    if json_report:
        result.to_json_file(json_report)
        console.print(f"  📄 JSON report saved: [green]{json_report}[/green]")

    # Webhook alerts
    import os

    slack_url = slack_webhook or os.environ.get("VALIDATEX_SLACK_WEBHOOK_URL")
    if slack_url:
        result.send_slack(webhook_url=slack_url, notify_on=notify_on)
        console.print("  🔔 Slack notification sent")

    teams_url = teams_webhook or os.environ.get("VALIDATEX_TEAMS_WEBHOOK_URL")
    if teams_url:
        result.send_teams(webhook_url=teams_url, notify_on=notify_on)
        console.print("  🔔 Teams notification sent")

    # Exit code
    sys.exit(0 if result.success else 1)


# ── run (checkpoint) ─────────────────────────────────────────────────────


@cli.command()
@click.option("--checkpoint", "-c", required=True, help="Path to checkpoint YAML/JSON.")
def run(checkpoint):
    """Run a checkpoint configuration."""
    import validatex.expectations  # noqa: F401
    from validatex.config.loader import load_checkpoint
    from validatex.core.validator import Validator

    console.print(
        Panel(
            "[bold magenta]ValidateX[/bold magenta] — Checkpoint Run",
            box=box.DOUBLE_EDGE,
            style="bold blue",
        )
    )

    config = load_checkpoint(checkpoint)
    console.print(f"  📋 Checkpoint: [cyan]{config.name}[/cyan]")

    suite = config.load_suite()
    console.print(f"  📝 Suite loaded: [bold]{len(suite)}[/bold] expectations")

    df = config.load_data()
    console.print("  📂 Data loaded")

    validator = Validator(suite, engine=config.engine)
    result = validator.run(df)

    console.print(result.summary())

    # Reports
    report_cfg = config.report
    if report_cfg.get("html"):
        result.to_html(report_cfg["html"])
        console.print(f"\n  📊 HTML: [green]{report_cfg['html']}[/green]")
    if report_cfg.get("json"):
        result.to_json_file(report_cfg["json"])
        console.print(f"  📄 JSON: [green]{report_cfg['json']}[/green]")

    # Webhook alerts
    import os

    slack_cfg = report_cfg.get("slack")
    slack_url = None
    slack_notify_on = "failure"
    if isinstance(slack_cfg, dict):
        slack_url = slack_cfg.get("webhook_url")
        slack_notify_on = slack_cfg.get("notify_on", "failure")
    elif isinstance(slack_cfg, str):
        slack_url = slack_cfg

    slack_url = slack_url or os.environ.get("VALIDATEX_SLACK_WEBHOOK_URL")
    if slack_url:
        result.send_slack(webhook_url=slack_url, notify_on=slack_notify_on)
        console.print("  🔔 Slack notification sent")

    teams_cfg = report_cfg.get("teams")
    teams_url = None
    teams_notify_on = "failure"
    if isinstance(teams_cfg, dict):
        teams_url = teams_cfg.get("webhook_url")
        teams_notify_on = teams_cfg.get("notify_on", "failure")
    elif isinstance(teams_cfg, str):
        teams_url = teams_cfg

    teams_url = teams_url or os.environ.get("VALIDATEX_TEAMS_WEBHOOK_URL")
    if teams_url:
        result.send_teams(webhook_url=teams_url, notify_on=teams_notify_on)
        console.print("  🔔 Teams notification sent")

    sys.exit(0 if result.success else 1)


# ── profile ───────────────────────────────────────────────────────────────


@cli.command()
@click.option("--data", "-d", required=True, help="Path to data file.")
@click.option("--suggest", is_flag=True, help="Auto-suggest an expectation suite.")
@click.option("--output", "-o", default=None, help="Save suggested suite to file.")
def profile(data, suggest, output):
    """Profile a dataset and optionally suggest expectations."""
    from validatex.profiler.profiler import DataProfiler

    console.print(
        Panel(
            "[bold magenta]ValidateX[/bold magenta] — Data Profiler",
            box=box.DOUBLE_EDGE,
            style="bold blue",
        )
    )

    df = _load_data_file(data, "pandas")
    profiler = DataProfiler()

    console.print(f"  📂 Profiling: [cyan]{data}[/cyan]")
    profile_result = profiler.profile(df)
    console.print(profile_result.summary())

    if suggest:
        suite = profiler.suggest_expectations(df, suite_name=f"auto_{Path(data).stem}")
        console.print(f"\n  💡 Suggested [bold]{len(suite)}[/bold] expectations")

        if output:
            suite.save(output)
            console.print(f"  💾 Saved to: [green]{output}[/green]")
        else:
            console.print("\n" + suite.to_yaml())


# ── list-expectations ────────────────────────────────────────────────────


@cli.command("list-expectations")
def list_expectations():
    """List all available expectation types."""
    import validatex.expectations  # noqa: F401
    from validatex.core.expectation import (
        list_expectations as _list_exp,
        get_expectation_class,
    )

    table = Table(
        title="Available Expectations",
        box=box.ROUNDED,
        style="bold",
        title_style="bold magenta",
    )
    table.add_column("#", style="dim", width=4)
    table.add_column("Expectation Type", style="cyan")
    table.add_column("Description", style="white")
    table.add_column("Category", style="green")

    for i, name in enumerate(_list_exp(), 1):
        cls = get_expectation_class(name)
        doc = (cls.__doc__ or "").strip().split("\n")[0]
        if "column_pair" in name or "multicolumn" in name or "compound" in name:
            category = "Aggregate"
        elif "table" in name:
            category = "Table"
        else:
            category = "Column"
        table.add_row(str(i), name, doc, category)

    console.print(table)


# ── init ──────────────────────────────────────────────────────────────────


@cli.command()
@click.option("--dir", "-d", "directory", default=".", help="Project directory.")
def init(directory):
    """Initialize a new ValidateX project with sample config files."""
    base = Path(directory)
    dirs = [
        base / "suites",
        base / "checkpoints",
        base / "reports",
    ]

    for d in dirs:
        d.mkdir(parents=True, exist_ok=True)

    # Sample suite
    sample_suite = base / "suites" / "sample_suite.yaml"
    if not sample_suite.exists():
        sample_suite.write_text(
            """suite_name: sample_suite
meta:
  description: "Sample expectation suite"
expectations:
  - expectation_type: expect_column_to_exist
    column: id
  - expectation_type: expect_column_to_not_be_null
    column: id
  - expectation_type: expect_column_values_to_be_between
    column: age
    kwargs:
      min_value: 0
      max_value: 150
  - expectation_type: expect_column_values_to_be_in_set
    column: status
    kwargs:
      value_set: ["active", "inactive", "pending"]
""",
            encoding="utf-8",
        )

    # Sample checkpoint
    sample_cp = base / "checkpoints" / "sample_checkpoint.yaml"
    if not sample_cp.exists():
        sample_cp.write_text(
            """name: sample_checkpoint
suite_path: suites/sample_suite.yaml
data_source:
  type: csv
  path: data/sample.csv
engine: pandas
report:
  html: reports/validation_report.html
  json: reports/validation_report.json
""",
            encoding="utf-8",
        )

    console.print(
        Panel(
            "[bold green]✅ ValidateX project initialized![/bold green]\n\n"
            f"  📁 {base / 'suites'}            — expectation suites\n"
            f"  📁 {base / 'checkpoints'}       — checkpoint configs\n"
            f"  📁 {base / 'reports'}            — generated reports\n\n"
            "  Get started:\n"
            "    1. Edit [cyan]suites/sample_suite.yaml[/cyan]\n"
            "    2. Edit [cyan]checkpoints/sample_checkpoint.yaml[/cyan]\n"
            "    3. Run  [cyan]validatex run -c checkpoints/sample_checkpoint.yaml[/cyan]",
            box=box.DOUBLE_EDGE,
            style="bold blue",
            title="ValidateX",
        )
    )


# ── contract ──────────────────────────────────────────────────────────────


@cli.group()
def contract():
    """Create, validate, compare and publish data contracts."""


@contract.command("lint")
@click.argument("path")
def contract_lint(path):
    """Check a contract file for definition errors (unknown rules, bad types, ...)."""
    import validatex.expectations  # noqa: F401
    from validatex.contracts import ContractError, DataContract

    try:
        dc = DataContract.load(path)
    except ContractError as exc:
        console.print(f"[bold red]✗[/bold red] {exc}")
        sys.exit(1)
    problems = dc.validate_definition()
    if problems:
        console.print(f"[bold red]✗ {dc.name} v{dc.version} has {len(problems)} problem(s):[/bold red]")
        for p in problems:
            console.print(f"   - {p}")
        sys.exit(1)
    console.print(
        f"[bold green]✓[/bold green] {dc.name} v{dc.version} is valid "
        f"({len(dc.schema)} fields, {len(dc.quality_rules)} rules)"
    )


@contract.command("validate")
@click.option("--contract", "-c", "contract_path", required=True, help="Path to the contract (YAML/JSON).")
@click.option("--data", "-d", required=True, help="Path to data file (CSV, Parquet).")
@click.option("--engine", "-e", default="pandas", help="Engine: pandas or polars.")
@click.option("--report", "-r", default=None, help="Output HTML report path.")
@click.option("--json-report", "-j", default=None, help="Output JSON report path.")
@click.option("--store", default=None, help="SQLite file to record this run for historical monitoring.")
@click.option("--run-name", default=None, help="Label stored with the run.")
def contract_validate(contract_path, data, engine, report, json_report, store, run_name):
    """Validate a dataset against a data contract (exit code 1 when the contract is violated)."""
    import validatex.expectations  # noqa: F401
    from validatex.contracts import ContractError, DataContract, validate_contract

    try:
        dc = DataContract.load(contract_path)
        df = _load_data_file(data, engine)
        result = validate_contract(
            df, dc, engine=engine, data_source=data, store=store, run_name=run_name, dataset_name=dc.name
        )
    except ContractError as exc:
        console.print(f"[bold red]✗[/bold red] {exc}")
        sys.exit(2)
    console.print(result.summary())
    if report:
        result.to_html(report)
        console.print(f"  📊 HTML report saved: [green]{report}[/green]")
    if json_report:
        result.to_json_file(json_report)
        console.print(f"  📄 JSON report saved: [green]{json_report}[/green]")
    sys.exit(0 if result.success else 1)


@contract.command("diff")
@click.argument("old")
@click.argument("new")
@click.option("--format", "fmt", type=click.Choice(["text", "json", "markdown"]), default="text")
@click.option("--fail-on-breaking", is_flag=True, help="Exit 1 if any breaking change is found.")
@click.option("--check-version", is_flag=True, help="Exit 1 if the version bump is smaller than the changes require.")
def contract_diff(old, new, fmt, fail_on_breaking, check_version):
    """Compare two contract files and list breaking / non-breaking changes."""
    import json as _json

    import validatex.expectations  # noqa: F401
    from validatex.contracts import ContractError, DataContract, compare_contracts

    try:
        diff = compare_contracts(DataContract.load(old), DataContract.load(new))
    except ContractError as exc:
        console.print(f"[bold red]✗[/bold red] {exc}")
        sys.exit(2)
    if fmt == "json":
        click.echo(_json.dumps(diff.to_dict(), indent=2, default=str))
    elif fmt == "markdown":
        click.echo(diff.to_markdown())
    else:
        click.echo(diff.summary())
    if (fail_on_breaking and diff.has_breaking_changes) or (check_version and not diff.version_ok):
        sys.exit(1)


@contract.command("register")
@click.argument("path")
@click.option("--registry", "-r", default="contracts", help="Registry directory (commit it to git).")
@click.option("--no-semver-check", is_flag=True, help="Skip semantic-version enforcement.")
def contract_register(path, registry, no_semver_check):
    """Publish a contract version into a registry directory."""
    import validatex.expectations  # noqa: F401
    from validatex.contracts import ContractError, ContractRegistry, DataContract

    try:
        dc = DataContract.load(path)
        target = ContractRegistry(registry).register(dc, enforce_semver=not no_semver_check)
    except ContractError as exc:
        console.print(f"[bold red]✗[/bold red] {exc}")
        sys.exit(1)
    console.print(f"[bold green]✓[/bold green] Registered {dc.name} v{dc.version} -> {target}")


# ── history ───────────────────────────────────────────────────────────────


@cli.command()
@click.option("--store", "-s", default=None, help="RunStore SQLite file (default ./.validatex/runs.db).")
@click.option("--dataset", "-d", default=None, help="Filter by dataset name.")
@click.option("--limit", "-n", default=20, show_default=True, help="Number of runs to consider.")
@click.option("--failures", is_flag=True, help="Show the most frequently failing expectations.")
@click.option("--schema", "show_schema", is_flag=True, help="Show schema change history (needs --dataset).")
def history(store, dataset, limit, failures, show_schema):
    """Show validation run history, failure trends, or schema changes."""
    from validatex.storage.run_store import RunStore

    run_store = RunStore(db_path=store)
    if show_schema:
        if not dataset:
            raise click.UsageError("--schema requires --dataset")
        table = Table(title=f"Schema changes - {dataset}", box=box.ROUNDED)
        for col in ("Run", "When", "Added", "Removed", "Type changes"):
            table.add_column(col)
        for c in run_store.schema_changes(dataset):
            table.add_row(
                str(c.run_id),
                c.timestamp[:19],
                ", ".join(c.added) or "-",
                ", ".join(c.removed) or "-",
                ", ".join(f"{k}: {v['from']}->{v['to']}" for k, v in c.type_changes.items()) or "-",
            )
        console.print(table)
        return
    if failures:
        table = Table(title="Failing expectations", box=box.ROUNDED)
        for col in ("Expectation", "Column", "Severity", "Failures", "Rate %", "Streak", "Trend"):
            table.add_column(col)
        for t in run_store.failed_expectation_trends(dataset_name=dataset, limit=limit):
            table.add_row(
                t.expectation_type,
                t.column or "-",
                t.severity or "-",
                f"{t.failures}/{t.evaluations}",
                str(t.failure_rate),
                str(t.current_streak),
                t.trend,
            )
        console.print(table)
        return
    table = Table(title="Validation runs", box=box.ROUNDED)
    for col in ("Run", "When", "Dataset", "Suite v", "Score", "Pass", "Fail", "Rows"):
        table.add_column(col)
    for r in run_store.get_runs(dataset_name=dataset, limit=limit):
        table.add_row(
            str(r["run_id"]),
            str(r["timestamp"])[:19],
            str(r["dataset_name"]),
            str(r.get("suite_version") or "-"),
            str(r["quality_score"]),
            str(r["passed_expectations"]),
            str(r["failed_expectations"]),
            str(r.get("row_count") if r.get("row_count") is not None else "-"),
        )
    console.print(table)
    trend = run_store.quality_trend(dataset_name=dataset, limit=limit)
    if trend.runs:
        console.print(f"  Trend: [bold]{trend.direction}[/bold] (mean {trend.mean}, latest {trend.latest})")


# ── dbt ───────────────────────────────────────────────────────────────────


@cli.group()
def dbt():
    """Import/export dbt schema.yml files and run ValidateX checks on dbt models."""


@dbt.command("import-schema")
@click.argument("schema_path")
@click.option("--output-dir", "-o", default="contracts", help="Directory to write generated DataContract YAML files.")
def dbt_import_schema(schema_path, output_dir):
    """Convert models in a dbt schema.yml into ValidateX DataContract YAML files."""
    from validatex.integrations.dbt import contracts_from_dbt_schema

    contracts = contracts_from_dbt_schema(schema_path)
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    for c in contracts:
        target = c.save(out_dir / f"{c.name}.yaml")
        console.print(f"[bold green]✓[/bold green] Imported {c.name} -> {target}")


@dbt.command("export-schema")
@click.argument("contract_path")
@click.option("--output", "-o", required=True, help="Output dbt schema.yml file path.")
def dbt_export_schema(contract_path, output):
    """Export a ValidateX DataContract (or directory of contracts) to a dbt schema.yml file."""
    from validatex.contracts import DataContract
    from validatex.integrations.dbt import save_dbt_schema

    p = Path(contract_path)
    if p.is_dir():
        contracts = [DataContract.load(f) for f in sorted(p.glob("*.y*ml"))]
    else:
        contracts = [DataContract.load(p)]
    target = save_dbt_schema(contracts, output)
    console.print(f"[bold green]✓[/bold green] Exported {len(contracts)} model(s) -> {target}")


@dbt.command("test")
@click.option("--schema", "-s", "schema_path", required=True, help="Path to dbt schema.yml or manifest.json.")
@click.option("--db", required=True, help="SQLAlchemy database URL (e.g. sqlite:///warehouse.db).")
@click.option("--model", "-m", "models", multiple=True, help="Optional model name(s) to validate.")
@click.option("--store", default=None, help="Optional SQLite RunStore path to record validation runs.")
def dbt_test(schema_path, db, models, store):
    """Validate materialized dbt models via Push-Down SQL against their dbt schema.yml rules."""
    from validatex.integrations.dbt import run_dbt_validations

    results = run_dbt_validations(
        schema_path,
        db,
        models=list(models) if models else None,
        engine="sql",
        store=store,
    )
    all_ok = True
    for name, res in results.items():
        status = "[bold green]PASS[/bold green]" if res.success else "[bold red]FAIL[/bold red]"
        console.print(f"  {status} {name} (score: {res.quality_score}/100)")
        if not res.success:
            all_ok = False
    sys.exit(0 if all_ok else 1)


# ── dashboard ─────────────────────────────────────────────────────────────


@cli.command()
@click.option("--store", "-s", default=None, help="RunStore SQLite path (default ./.validatex/runs.db).")
@click.option("--registry", "-r", default="contracts", help="ContractRegistry directory path.")
@click.option("--host", default="127.0.0.1", show_default=True, help="Host interface to bind.")
@click.option("--port", "-p", default=8501, show_default=True, help="Port to serve dashboard on.")
@click.option("--export", "-o", "export_path", default=None, help="Export static HTML dashboard to file and exit.")
@click.option("--limit", "-n", default=30, show_default=True, help="Max runs per dataset to load.")
def dashboard(store, registry, host, port, export_path, limit):
    """Launch the multi-dataset Web Dashboard or export a static HTML snapshot."""
    from validatex.dashboard import export_dashboard_html, serve_dashboard

    if export_path:
        out = export_dashboard_html(export_path, store=store, registry=registry, limit=limit)
        console.print(f"[bold green]✓[/bold green] Dashboard exported to [cyan]{out}[/cyan]")
        return
    console.print(
        f"[bold green]🚀 ValidateX Dashboard running at[/bold green] [cyan]http://{host}:{port}[/cyan] (Ctrl+C to stop)"
    )
    serve_dashboard(host=host, port=port, store=store, registry=registry, limit=limit)


# ── migrate ───────────────────────────────────────────────────────────────


@cli.command()
@click.option("--from-ge", "ge_path", required=True, help="Path to Great Expectations JSON/YAML suite.")
@click.option("--output", "-o", required=True, help="Output YAML/JSON path.")
@click.option("--as-contract", is_flag=True, help="Convert into a ValidateX DataContract instead of ExpectationSuite.")
def migrate(ge_path, output, as_contract):
    """Migrate a Great Expectations suite to a ValidateX suite or DataContract."""
    from validatex.migrations import migrate_ge_suite, migrate_ge_to_contract

    if as_contract:
        contract, report = migrate_ge_to_contract(ge_path)
        contract.save(output)
    else:
        suite, report = migrate_ge_suite(ge_path)
        suite.save(output)
    console.print(
        f"[bold green]✓[/bold green] Migrated {report.migrated_count}/{report.total_ge_expectations} expectations -> [cyan]{output}[/cyan]"
    )
    if report.skipped_expectations:
        console.print(f"  [yellow]Skipped unsupported: {', '.join(report.skipped_expectations)}[/yellow]")


# ── helpers ───────────────────────────────────────────────────────────────


def _load_data_file(filepath: str, engine: str = "pandas"):
    """Auto-detect file type and load."""
    ext = Path(filepath).suffix.lower()
    if engine.lower() == "polars":
        import polars as pl

        if ext == ".csv":
            return pl.read_csv(filepath)
        elif ext in (".parquet", ".pq"):
            return pl.read_parquet(filepath)
        elif ext in (".json",):
            return pl.read_json(filepath)
        else:
            return pl.read_csv(filepath)

    import pandas as pd

    if ext == ".csv":
        return pd.read_csv(filepath)
    elif ext in (".parquet", ".pq"):
        return pd.read_parquet(filepath)
    elif ext in (".json",):
        return pd.read_json(filepath)
    elif ext in (".xlsx", ".xls"):
        return pd.read_excel(filepath)
    else:
        # Try CSV as default
        return pd.read_csv(filepath)


if __name__ == "__main__":
    cli()
