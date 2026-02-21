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
@click.option(
    "--suite", "-s", required=True, help="Path to expectation suite (YAML/JSON)."
)
@click.option("--engine", "-e", default="pandas", help="Engine: pandas or spark.")
@click.option("--report", "-r", default=None, help="Output HTML report path.")
@click.option("--json-report", "-j", default=None, help="Output JSON report path.")
def validate(data, suite, engine, report, json_report):
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
    console.print(
        f"     Shape: [bold]{df.shape[0]}[/bold] rows × [bold]{df.shape[1]}[/bold] columns"
    )

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


# ── helpers ───────────────────────────────────────────────────────────────


def _load_data_file(filepath: str, engine: str = "pandas"):
    """Auto-detect file type and load."""
    import pandas as pd

    ext = Path(filepath).suffix.lower()
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
