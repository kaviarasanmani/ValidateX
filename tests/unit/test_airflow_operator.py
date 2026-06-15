"""
Unit tests for validatex.integrations.airflow.ValidateXOperator

Since apache-airflow is not installed in this environment, we mock the
airflow package entirely before importing ValidateXOperator.
"""
import sys
import types
import pytest
import tempfile
import os
import pandas as pd
from unittest.mock import MagicMock, patch, call

import validatex as vx


# ---------------------------------------------------------------------------
# Airflow stub — injected into sys.modules before importing the operator
# ---------------------------------------------------------------------------

def _install_airflow_stub():
    """Create a minimal fake airflow package so the operator can be imported."""

    class BaseOperator:
        """Minimal stub for airflow.models.BaseOperator."""
        def __init__(self, **kwargs):
            self.task_id = kwargs.get("task_id", "test_task")
            self.log = MagicMock()

    # Build fake module tree
    airflow_mod          = types.ModuleType("airflow")
    airflow_models_mod   = types.ModuleType("airflow.models")
    airflow_utils_mod    = types.ModuleType("airflow.utils")
    airflow_context_mod  = types.ModuleType("airflow.utils.context")

    airflow_models_mod.BaseOperator      = BaseOperator
    airflow_context_mod.Context          = dict          # Context is just a dict alias

    airflow_mod.models                   = airflow_models_mod
    airflow_mod.utils                    = airflow_utils_mod
    airflow_utils_mod.context            = airflow_context_mod

    sys.modules.setdefault("airflow",               airflow_mod)
    sys.modules.setdefault("airflow.models",        airflow_models_mod)
    sys.modules.setdefault("airflow.utils",         airflow_utils_mod)
    sys.modules.setdefault("airflow.utils.context", airflow_context_mod)

    return BaseOperator


_BaseOperator = _install_airflow_stub()

# Now safe to import
from validatex.integrations.airflow import ValidateXOperator  # noqa: E402


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def clean_suite():
    """Suite whose checks all pass on clean_df."""
    return (
        vx.ExpectationSuite("clean_suite")
        .add("expect_column_to_not_be_null",      column="order_id")
        .add("expect_column_values_to_be_unique", column="order_id")
        .add("expect_column_values_to_be_between",
             column="amount", kwargs={"min_value": 0.0, "max_value": 9999.0})
    )


@pytest.fixture()
def dirty_suite():
    """Suite that will fail on dirty_df (duplicate + negative amount)."""
    return (
        vx.ExpectationSuite("dirty_suite")
        .add("expect_column_values_to_be_unique", column="order_id")
        .add("expect_column_values_to_be_between",
             column="amount", kwargs={"min_value": 0.0, "max_value": 9999.0})
    )


@pytest.fixture()
def clean_csv(tmp_path):
    """Write a clean CSV and return its path."""
    df = pd.DataFrame({
        "order_id": [1, 2, 3, 4, 5],
        "amount":   [10.0, 20.0, 30.0, 40.0, 50.0],
    })
    path = str(tmp_path / "clean.csv")
    df.to_csv(path, index=False)
    return path


@pytest.fixture()
def dirty_csv(tmp_path):
    """Write a dirty CSV (duplicate order_id, negative amount) and return its path."""
    df = pd.DataFrame({
        "order_id": [1, 2, 2, 4, 5],   # duplicate 2
        "amount":   [10.0, 20.0, -5.0, 40.0, 50.0],  # negative
    })
    path = str(tmp_path / "dirty.csv")
    df.to_csv(path, index=False)
    return path


@pytest.fixture()
def clean_parquet(tmp_path):
    """Write a clean Parquet file and return its path."""
    df = pd.DataFrame({
        "order_id": [1, 2, 3],
        "amount":   [99.0, 199.0, 299.0],
    })
    path = str(tmp_path / "clean.parquet")
    df.to_parquet(path, index=False)
    return path


def _make_operator(suite, data_path, data_format="csv",
                   min_score=100.0, report_path=None):
    return ValidateXOperator(
        task_id="dq_gate",
        suite=suite,
        data_path=data_path,
        data_format=data_format,
        min_score=min_score,
        report_path=report_path,
    )


# ---------------------------------------------------------------------------
# 1. Constructor tests
# ---------------------------------------------------------------------------

class TestValidateXOperatorInit:

    def test_stores_all_params(self, clean_suite, clean_csv):
        op = _make_operator(clean_suite, clean_csv, min_score=90.0,
                            report_path="/tmp/report.html")
        assert op.suite is clean_suite
        assert op.data_path == clean_csv
        assert op.data_format == "csv"
        assert op.min_score == 90.0
        assert op.report_path == "/tmp/report.html"

    def test_default_values(self, clean_suite, clean_csv):
        op = _make_operator(clean_suite, clean_csv)
        assert op.data_format == "csv"
        assert op.min_score == 100.0
        assert op.report_path is None


# ---------------------------------------------------------------------------
# 2. Successful validation (score >= min_score) — CSV
# ---------------------------------------------------------------------------

class TestExecutePassCSV:

    def test_returns_xcom_dict(self, clean_suite, clean_csv):
        op = _make_operator(clean_suite, clean_csv, min_score=0.0)
        result = op.execute(context={})

        assert isinstance(result, dict)
        assert "validatex_score" in result
        assert "passed_expectations" in result
        assert "failed_expectations" in result
        assert "report_path" in result

    def test_score_is_100_on_clean_data(self, clean_suite, clean_csv):
        op = _make_operator(clean_suite, clean_csv, min_score=0.0)
        result = op.execute(context={})
        assert result["validatex_score"] == 100.0

    def test_all_expectations_passed(self, clean_suite, clean_csv):
        op = _make_operator(clean_suite, clean_csv, min_score=0.0)
        result = op.execute(context={})
        assert result["failed_expectations"] == 0
        assert result["passed_expectations"] == 3

    def test_report_path_is_none_when_not_set(self, clean_suite, clean_csv):
        op = _make_operator(clean_suite, clean_csv, min_score=0.0)
        result = op.execute(context={})
        assert result["report_path"] is None


# ---------------------------------------------------------------------------
# 3. Successful validation — Parquet format
# ---------------------------------------------------------------------------

class TestExecutePassParquet:

    def test_parquet_loads_and_passes(self, clean_suite, clean_parquet):
        op = _make_operator(clean_suite, clean_parquet,
                            data_format="parquet", min_score=0.0)
        result = op.execute(context={})
        assert result["validatex_score"] == 100.0
        assert result["failed_expectations"] == 0


# ---------------------------------------------------------------------------
# 4. Quality gate — score below min_score raises ValueError
# ---------------------------------------------------------------------------

class TestExecuteGateFails:

    def test_raises_value_error_when_below_threshold(self, dirty_suite, dirty_csv):
        op = _make_operator(dirty_suite, dirty_csv, min_score=99.0)
        with pytest.raises(ValueError, match="Data Quality Gate Failed"):
            op.execute(context={})

    def test_error_message_contains_score(self, dirty_suite, dirty_csv):
        op = _make_operator(dirty_suite, dirty_csv, min_score=99.0)
        with pytest.raises(ValueError) as exc_info:
            op.execute(context={})
        assert "below" in str(exc_info.value).lower()

    def test_error_message_contains_failed_columns(self, dirty_suite, dirty_csv):
        op = _make_operator(dirty_suite, dirty_csv, min_score=99.0)
        with pytest.raises(ValueError) as exc_info:
            op.execute(context={})
        assert "order_id" in str(exc_info.value) or "amount" in str(exc_info.value)

    def test_gate_passes_with_low_min_score(self, dirty_suite, dirty_csv):
        """If we lower the bar, the gate should pass even for dirty data."""
        op = _make_operator(dirty_suite, dirty_csv, min_score=0.0)
        result = op.execute(context={})
        assert isinstance(result, dict)
        assert result["failed_expectations"] > 0   # failures exist, but gate is lenient


# ---------------------------------------------------------------------------
# 5. HTML report generation
# ---------------------------------------------------------------------------

class TestReportGeneration:

    def test_html_report_is_created(self, clean_suite, clean_csv, tmp_path):
        report = str(tmp_path / "report.html")
        op = _make_operator(clean_suite, clean_csv,
                            min_score=0.0, report_path=report)
        op.execute(context={})
        assert os.path.isfile(report)
        assert os.path.getsize(report) > 0

    def test_html_report_not_created_when_path_is_none(self, clean_suite, clean_csv, tmp_path):
        op = _make_operator(clean_suite, clean_csv, min_score=0.0)
        op.execute(context={})
        # No .html report file should have been written
        html_files = list(tmp_path.glob("*.html"))
        assert html_files == [], f"Unexpected HTML files: {html_files}"

    def test_report_path_in_xcom(self, clean_suite, clean_csv, tmp_path):
        report = str(tmp_path / "report.html")
        op = _make_operator(clean_suite, clean_csv,
                            min_score=0.0, report_path=report)
        result = op.execute(context={})
        assert result["report_path"] == report


# ---------------------------------------------------------------------------
# 6. Unsupported data format
# ---------------------------------------------------------------------------

class TestUnsupportedFormat:

    def test_raises_value_error_for_json(self, clean_suite, clean_csv):
        op = _make_operator(clean_suite, clean_csv, data_format="json")
        with pytest.raises(ValueError, match="Unsupported data_format"):
            op.execute(context={})

    def test_raises_value_error_for_excel(self, clean_suite, clean_csv):
        op = _make_operator(clean_suite, clean_csv, data_format="xlsx")
        with pytest.raises(ValueError, match="Unsupported data_format"):
            op.execute(context={})


# ---------------------------------------------------------------------------
# 7. XCom return value structure
# ---------------------------------------------------------------------------

class TestXComOutput:

    def test_xcom_keys_are_present(self, clean_suite, clean_csv):
        op = _make_operator(clean_suite, clean_csv, min_score=0.0)
        result = op.execute(context={})
        assert set(result.keys()) == {
            "validatex_score",
            "passed_expectations",
            "failed_expectations",
            "report_path",
        }

    def test_xcom_score_is_float(self, clean_suite, clean_csv):
        op = _make_operator(clean_suite, clean_csv, min_score=0.0)
        result = op.execute(context={})
        assert isinstance(result["validatex_score"], float)

    def test_xcom_counts_are_ints(self, clean_suite, clean_csv):
        op = _make_operator(clean_suite, clean_csv, min_score=0.0)
        result = op.execute(context={})
        assert isinstance(result["passed_expectations"], int)
        assert isinstance(result["failed_expectations"], int)

    def test_xcom_counts_sum_to_total_expectations(self, clean_suite, clean_csv):
        op = _make_operator(clean_suite, clean_csv, min_score=0.0)
        result = op.execute(context={})
        total = result["passed_expectations"] + result["failed_expectations"]
        assert total == len(clean_suite)
