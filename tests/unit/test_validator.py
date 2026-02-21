"""
Unit tests for the Validator and ValidationResult.
"""

import pytest
import pandas as pd

import validatex.expectations  # noqa: F401
from validatex.core.suite import ExpectationSuite
from validatex.core.validator import Validator, validate
from validatex.core.result import ValidationResult


@pytest.fixture
def sample_df():
    return pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
            "age": [25, 30, 35, 28, 42],
            "status": ["active", "active", "inactive", "active", "pending"],
        }
    )


@pytest.fixture
def passing_suite():
    suite = ExpectationSuite("passing_suite")
    suite.add("expect_column_to_exist", column="id")
    suite.add("expect_column_to_not_be_null", column="id")
    suite.add(
        "expect_column_values_to_be_between", column="age", min_value=0, max_value=100
    )
    return suite


@pytest.fixture
def failing_suite():
    suite = ExpectationSuite("failing_suite")
    suite.add("expect_column_to_exist", column="nonexistent")
    suite.add(
        "expect_column_values_to_be_between", column="age", min_value=30, max_value=40
    )
    return suite


class TestValidator:
    def test_run_all_pass(self, sample_df, passing_suite):
        validator = Validator(passing_suite)
        result = validator.run(sample_df, data_source="test_data")
        assert result.success is True
        assert result.total_expectations == 3
        assert result.successful_expectations == 3
        assert result.failed_expectations == 0

    def test_run_with_failures(self, sample_df, failing_suite):
        validator = Validator(failing_suite)
        result = validator.run(sample_df)
        assert result.success is False
        assert result.failed_expectations > 0

    def test_run_records_duration(self, sample_df, passing_suite):
        validator = Validator(passing_suite)
        result = validator.run(sample_df)
        assert result.run_duration_seconds > 0

    def test_run_records_engine(self, sample_df, passing_suite):
        validator = Validator(passing_suite, engine="pandas")
        result = validator.run(sample_df)
        assert result.engine == "pandas"


class TestValidateFunction:
    def test_convenience_function(self, sample_df, passing_suite):
        result = validate(sample_df, passing_suite)
        assert isinstance(result, ValidationResult)
        assert result.success is True


class TestValidationResult:
    def test_statistics(self, sample_df, passing_suite):
        result = validate(sample_df, passing_suite)
        stats = result.compute_statistics()
        assert stats["total"] == 3
        assert stats["passed"] == 3
        assert stats["success_percent"] == 100.0

    def test_summary_string(self, sample_df, passing_suite):
        result = validate(sample_df, passing_suite)
        summary = result.summary()
        assert "ALL PASSED" in summary
        assert "passing_suite" in summary

    def test_to_dict(self, sample_df, passing_suite):
        result = validate(sample_df, passing_suite)
        d = result.to_dict()
        assert "suite_name" in d
        assert "results" in d
        assert "statistics" in d

    def test_to_json(self, sample_df, passing_suite):
        result = validate(sample_df, passing_suite)
        json_str = result.to_json()
        assert "passing_suite" in json_str

    def test_to_json_file(self, sample_df, passing_suite, tmp_path):
        result = validate(sample_df, passing_suite)
        filepath = str(tmp_path / "report.json")
        result.to_json_file(filepath)
        import json

        with open(filepath) as f:
            data = json.load(f)
        assert data["suite_name"] == "passing_suite"

    def test_to_html(self, sample_df, passing_suite, tmp_path):
        result = validate(sample_df, passing_suite)
        filepath = str(tmp_path / "report.html")
        result.to_html(filepath)
        with open(filepath, encoding="utf-8") as f:
            content = f.read()
        assert "ValidateX" in content
        assert "passing_suite" in content


class TestExpectationSuite:
    def test_add_and_len(self):
        suite = ExpectationSuite("test_suite")
        suite.add("expect_column_to_exist", column="col1")
        suite.add("expect_column_to_exist", column="col2")
        assert len(suite) == 2

    def test_fluent_chaining(self):
        suite = (
            ExpectationSuite("chained")
            .add("expect_column_to_exist", column="a")
            .add("expect_column_to_exist", column="b")
        )
        assert len(suite) == 2

    def test_remove(self):
        suite = ExpectationSuite("test")
        suite.add("expect_column_to_exist", column="a")
        suite.add("expect_column_to_exist", column="b")
        suite.remove(0)
        assert len(suite) == 1

    def test_clear(self):
        suite = ExpectationSuite("test")
        suite.add("expect_column_to_exist", column="a")
        suite.clear()
        assert len(suite) == 0

    def test_save_and_load_yaml(self, tmp_path):
        suite = ExpectationSuite("yaml_test")
        suite.add("expect_column_to_exist", column="id")
        suite.add("expect_column_to_not_be_null", column="name")

        filepath = str(tmp_path / "suite.yaml")
        suite.save(filepath)

        loaded = ExpectationSuite.load(filepath)
        assert loaded.name == "yaml_test"
        assert len(loaded) == 2

    def test_save_and_load_json(self, tmp_path):
        suite = ExpectationSuite("json_test")
        suite.add("expect_column_to_exist", column="id")

        filepath = str(tmp_path / "suite.json")
        suite.save(filepath)

        loaded = ExpectationSuite.load(filepath)
        assert loaded.name == "json_test"
        assert len(loaded) == 1

    def test_to_yaml(self):
        suite = ExpectationSuite("test")
        suite.add("expect_column_to_exist", column="id")
        yaml_str = suite.to_yaml()
        assert "suite_name" in yaml_str
        assert "expect_column_to_exist" in yaml_str
