"""
Integration tests — end-to-end validation pipeline.
"""

import json
import os

import pytest
import pandas as pd

import validatex.expectations  # noqa: F401
from validatex.core.suite import ExpectationSuite
from validatex.core.validator import validate
from validatex.profiler.profiler import DataProfiler
from validatex.datasources.csv_source import CSVDataSource
from validatex.datasources.dataframe_source import DataFrameSource


@pytest.fixture
def sample_csv(tmp_path):
    """Create a sample CSV file for testing."""
    df = pd.DataFrame(
        {
            "user_id": [1, 2, 3, 4, 5],
            "username": ["alice", "bob", "charlie", "diana", "eve"],
            "age": [25, 30, 35, 28, 42],
            "email": [
                "alice@test.com",
                "bob@test.com",
                "charlie@test.com",
                "diana@test.com",
                "eve@test.com",
            ],
            "status": ["active", "active", "inactive", "active", "pending"],
            "balance": [100.50, 200.75, 50.00, 300.25, 150.00],
        }
    )
    path = tmp_path / "users.csv"
    df.to_csv(path, index=False)
    return str(path)


class TestEndToEnd:
    """Full pipeline tests."""

    def test_csv_to_validation_to_html_report(self, sample_csv, tmp_path):
        """Load CSV → Build suite → Validate → HTML report."""
        # Load
        source = CSVDataSource(sample_csv)
        df = source.load_pandas()

        # Build suite
        suite = (
            ExpectationSuite("user_validation")
            .add("expect_column_to_exist", column="user_id")
            .add("expect_column_to_not_be_null", column="user_id")
            .add("expect_column_values_to_be_unique", column="user_id")
            .add(
                "expect_column_values_to_be_between",
                column="age",
                min_value=0,
                max_value=150,
            )
            .add(
                "expect_column_values_to_be_in_set",
                column="status",
                value_set=["active", "inactive", "pending"],
            )
            .add(
                "expect_column_values_to_match_regex",
                column="email",
                regex=r"^[\w.]+@[\w]+\.\w+$",
            )
            .add("expect_table_row_count_to_be_between", min_value=1, max_value=1000)
        )

        # Validate
        result = validate(df, suite, data_source=sample_csv)

        assert result.success is True
        assert result.total_expectations == 7
        assert result.success_percent == 100.0

        # HTML report
        html_path = str(tmp_path / "report.html")
        result.to_html(html_path)
        assert os.path.exists(html_path)
        with open(html_path, encoding="utf-8") as f:
            content = f.read()
        assert "ValidateX" in content
        assert "PASSED" in content

    def test_csv_to_validation_to_json_report(self, sample_csv, tmp_path):
        """Load CSV → Build suite → Validate → JSON report."""
        source = CSVDataSource(sample_csv)
        df = source.load_pandas()

        suite = (
            ExpectationSuite("json_test")
            .add("expect_column_to_exist", column="user_id")
            .add("expect_column_to_not_be_null", column="user_id")
        )

        result = validate(df, suite)
        json_path = str(tmp_path / "report.json")
        result.to_json_file(json_path)

        with open(json_path) as f:
            data = json.load(f)

        assert data["success"] is True
        assert len(data["results"]) == 2

    def test_profile_and_auto_validate(self, sample_csv, tmp_path):
        """Profile → Auto-suggest → Validate."""
        df = pd.read_csv(sample_csv)

        profiler = DataProfiler()
        profile = profiler.profile(df)
        assert profile.row_count == 5
        assert profile.column_count == 6

        # Auto-suggest
        suite = profiler.suggest_expectations(df, suite_name="auto_users")
        assert len(suite) > 0

        # Validate with auto-suggested suite
        result = validate(df, suite)
        assert result.total_expectations > 0

    def test_suite_save_load_validate(self, sample_csv, tmp_path):
        """Build suite → Save YAML → Reload → Validate."""
        suite = (
            ExpectationSuite("save_load_test")
            .add("expect_column_to_exist", column="user_id")
            .add(
                "expect_column_values_to_be_between",
                column="age",
                min_value=0,
                max_value=100,
            )
        )

        # Save
        yaml_path = str(tmp_path / "suite.yaml")
        suite.save(yaml_path)

        # Reload
        loaded = ExpectationSuite.load(yaml_path)
        assert loaded.name == "save_load_test"
        assert len(loaded) == 2

        # Validate
        df = pd.read_csv(sample_csv)
        result = validate(df, loaded)
        assert result.success is True

    def test_dataframe_source(self):
        """Validate using DataFrameSource directly."""
        df = pd.DataFrame(
            {
                "x": [1, 2, 3],
                "y": [10, 20, 30],
            }
        )

        source = DataFrameSource(df, name="in_memory")
        data = source.load_pandas()

        suite = (
            ExpectationSuite("direct_df")
            .add("expect_table_row_count_to_equal", value=3)
            .add(
                "expect_column_values_to_be_between",
                column="x",
                min_value=1,
                max_value=3,
            )
            .add(
                "expect_column_pair_values_a_to_be_greater_than_b",
                column_a="y",
                column_b="x",
                or_equal=False,
            )
        )

        result = validate(data, suite)
        assert result.success is True

    def test_failing_validation_produces_correct_report(self, sample_csv, tmp_path):
        """Intentionally failing validations produce correct failure report."""
        df = pd.read_csv(sample_csv)

        suite = (
            ExpectationSuite("failing_test")
            .add("expect_column_to_exist", column="nonexistent_column")
            .add(
                "expect_column_values_to_be_between",
                column="age",
                min_value=30,
                max_value=35,
            )
            .add("expect_table_row_count_to_equal", value=100)
        )

        result = validate(df, suite)
        assert result.success is False
        assert result.failed_expectations >= 2

        # Report should still generate
        html_path = str(tmp_path / "fail_report.html")
        result.to_html(html_path)
        with open(html_path, encoding="utf-8") as f:
            content = f.read()
        assert "FAILED" in content
