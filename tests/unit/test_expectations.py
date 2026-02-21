"""
Unit tests for ValidateX expectations.
"""

import pytest
import pandas as pd

# Ensure expectations are registered
import validatex.expectations  # noqa: F401
from validatex.core.expectation import get_expectation_class, list_expectations

# ── Fixtures ──────────────────────────────────────────────────────────────


@pytest.fixture
def sample_df():
    """A sample DataFrame for testing."""
    return pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
            "age": [25, 30, 35, 28, 42],
            "email": [
                "alice@test.com",
                "bob@test.com",
                "charlie@test.com",
                "diana@test.com",
                "eve@test.com",
            ],
            "status": ["active", "active", "inactive", "active", "pending"],
            "score": [85.5, 92.3, 78.1, 95.0, 88.7],
        }
    )


@pytest.fixture
def df_with_nulls():
    """DataFrame with null values."""
    return pd.DataFrame(
        {
            "id": [1, 2, 3, None, 5],
            "name": ["Alice", None, "Charlie", "Diana", None],
            "value": [10, 20, None, 40, 50],
        }
    )


@pytest.fixture
def df_with_duplicates():
    """DataFrame with duplicate values."""
    return pd.DataFrame(
        {
            "id": [1, 2, 2, 3, 3],
            "category": ["A", "B", "B", "A", "C"],
        }
    )


# ── Test: Expectation Registration ───────────────────────────────────────


class TestExpectationRegistry:
    def test_list_expectations_not_empty(self):
        exps = list_expectations()
        assert len(exps) > 0

    def test_all_registered_expectations_can_be_retrieved(self):
        for name in list_expectations():
            cls = get_expectation_class(name)
            assert cls is not None

    def test_unknown_expectation_raises(self):
        with pytest.raises(ValueError, match="Unknown expectation"):
            get_expectation_class("nonexistent_expectation")


# ── Test: Column Expectations ────────────────────────────────────────────


class TestExpectColumnToExist:
    def test_pass(self, sample_df):
        exp = get_expectation_class("expect_column_to_exist")(column="id")
        result = exp.validate(sample_df)
        assert result.success is True

    def test_fail(self, sample_df):
        exp = get_expectation_class("expect_column_to_exist")(column="nonexistent")
        result = exp.validate(sample_df)
        assert result.success is False


class TestExpectColumnToNotBeNull:
    def test_pass_no_nulls(self, sample_df):
        exp = get_expectation_class("expect_column_to_not_be_null")(column="id")
        result = exp.validate(sample_df)
        assert result.success is True
        assert result.unexpected_count == 0

    def test_fail_with_nulls(self, df_with_nulls):
        exp = get_expectation_class("expect_column_to_not_be_null")(column="name")
        result = exp.validate(df_with_nulls)
        assert result.success is False
        assert result.unexpected_count == 2


class TestExpectColumnValuesToBeUnique:
    def test_pass_unique(self, sample_df):
        exp = get_expectation_class("expect_column_values_to_be_unique")(column="id")
        result = exp.validate(sample_df)
        assert result.success is True

    def test_fail_duplicates(self, df_with_duplicates):
        exp = get_expectation_class("expect_column_values_to_be_unique")(column="id")
        result = exp.validate(df_with_duplicates)
        assert result.success is False
        assert result.unexpected_count > 0


class TestExpectColumnValuesToBeBetween:
    def test_pass_in_range(self, sample_df):
        exp = get_expectation_class("expect_column_values_to_be_between")(
            column="age", kwargs={"min_value": 0, "max_value": 100}
        )
        result = exp.validate(sample_df)
        assert result.success is True

    def test_fail_out_of_range(self, sample_df):
        exp = get_expectation_class("expect_column_values_to_be_between")(
            column="age", kwargs={"min_value": 30, "max_value": 40}
        )
        result = exp.validate(sample_df)
        assert result.success is False

    def test_strict_bounds(self, sample_df):
        exp = get_expectation_class("expect_column_values_to_be_between")(
            column="age",
            kwargs={
                "min_value": 25,
                "max_value": 42,
                "strict_min": True,
                "strict_max": True,
            },
        )
        result = exp.validate(sample_df)
        # 25 and 42 are in the data; with strict bounds they should fail
        assert result.success is False


class TestExpectColumnValuesToBeInSet:
    def test_pass(self, sample_df):
        exp = get_expectation_class("expect_column_values_to_be_in_set")(
            column="status",
            kwargs={"value_set": ["active", "inactive", "pending"]},
        )
        result = exp.validate(sample_df)
        assert result.success is True

    def test_fail_missing_value(self, sample_df):
        exp = get_expectation_class("expect_column_values_to_be_in_set")(
            column="status",
            kwargs={"value_set": ["active", "inactive"]},
        )
        result = exp.validate(sample_df)
        assert result.success is False
        assert "pending" in result.unexpected_values


class TestExpectColumnValuesToNotBeInSet:
    def test_pass(self, sample_df):
        exp = get_expectation_class("expect_column_values_to_not_be_in_set")(
            column="status",
            kwargs={"value_set": ["deleted", "banned"]},
        )
        result = exp.validate(sample_df)
        assert result.success is True

    def test_fail(self, sample_df):
        exp = get_expectation_class("expect_column_values_to_not_be_in_set")(
            column="status",
            kwargs={"value_set": ["active"]},
        )
        result = exp.validate(sample_df)
        assert result.success is False


class TestExpectColumnValuesToMatchRegex:
    def test_pass_email(self, sample_df):
        exp = get_expectation_class("expect_column_values_to_match_regex")(
            column="email",
            kwargs={"regex": r"^[\w.]+@[\w]+\.\w+$"},
        )
        result = exp.validate(sample_df)
        assert result.success is True

    def test_fail_bad_regex(self, sample_df):
        exp = get_expectation_class("expect_column_values_to_match_regex")(
            column="name",
            kwargs={"regex": r"^\d+$"},
        )
        result = exp.validate(sample_df)
        assert result.success is False


class TestExpectColumnValuesToBeOfType:
    def test_pass(self, sample_df):
        exp = get_expectation_class("expect_column_values_to_be_of_type")(
            column="age",
            kwargs={"expected_type": "int"},
        )
        result = exp.validate(sample_df)
        assert result.success is True

    def test_fail(self, sample_df):
        exp = get_expectation_class("expect_column_values_to_be_of_type")(
            column="name",
            kwargs={"expected_type": "int"},
        )
        result = exp.validate(sample_df)
        assert result.success is False


class TestExpectColumnMeanToBeBetween:
    def test_pass(self, sample_df):
        exp = get_expectation_class("expect_column_mean_to_be_between")(
            column="age",
            kwargs={"min_value": 20, "max_value": 40},
        )
        result = exp.validate(sample_df)
        assert result.success is True

    def test_fail(self, sample_df):
        exp = get_expectation_class("expect_column_mean_to_be_between")(
            column="age",
            kwargs={"min_value": 40, "max_value": 50},
        )
        result = exp.validate(sample_df)
        assert result.success is False


# ── Test: Table Expectations ─────────────────────────────────────────────


class TestExpectTableRowCountToEqual:
    def test_pass(self, sample_df):
        exp = get_expectation_class("expect_table_row_count_to_equal")(
            kwargs={"value": 5}
        )
        result = exp.validate(sample_df)
        assert result.success is True

    def test_fail(self, sample_df):
        exp = get_expectation_class("expect_table_row_count_to_equal")(
            kwargs={"value": 10}
        )
        result = exp.validate(sample_df)
        assert result.success is False


class TestExpectTableRowCountToBeBetween:
    def test_pass(self, sample_df):
        exp = get_expectation_class("expect_table_row_count_to_be_between")(
            kwargs={"min_value": 1, "max_value": 100}
        )
        result = exp.validate(sample_df)
        assert result.success is True


class TestExpectTableColumnsToMatchSet:
    def test_pass(self, sample_df):
        exp = get_expectation_class("expect_table_columns_to_match_set")(
            kwargs={
                "column_set": ["id", "name", "age", "email", "status", "score"],
                "exact_match": True,
            },
        )
        result = exp.validate(sample_df)
        assert result.success is True

    def test_fail_missing(self, sample_df):
        exp = get_expectation_class("expect_table_columns_to_match_set")(
            kwargs={
                "column_set": [
                    "id",
                    "name",
                    "age",
                    "email",
                    "status",
                    "score",
                    "extra",
                ],
                "exact_match": True,
            },
        )
        result = exp.validate(sample_df)
        assert result.success is False


# ── Test: Aggregate Expectations ─────────────────────────────────────────


class TestExpectColumnPairValuesToBeEqual:
    def test_pass(self):
        df = pd.DataFrame({"a": [1, 2, 3], "b": [1, 2, 3]})
        exp = get_expectation_class("expect_column_pair_values_to_be_equal")(
            kwargs={"column_a": "a", "column_b": "b"}
        )
        result = exp.validate(df)
        assert result.success is True

    def test_fail(self):
        df = pd.DataFrame({"a": [1, 2, 3], "b": [1, 2, 4]})
        exp = get_expectation_class("expect_column_pair_values_to_be_equal")(
            kwargs={"column_a": "a", "column_b": "b"}
        )
        result = exp.validate(df)
        assert result.success is False


class TestExpectCompoundColumnsToBeUnique:
    def test_pass(self):
        df = pd.DataFrame(
            {
                "first": ["A", "A", "B"],
                "second": [1, 2, 1],
            }
        )
        exp = get_expectation_class("expect_compound_columns_to_be_unique")(
            kwargs={"column_list": ["first", "second"]}
        )
        result = exp.validate(df)
        assert result.success is True

    def test_fail(self):
        df = pd.DataFrame(
            {
                "first": ["A", "A", "B"],
                "second": [1, 1, 1],
            }
        )
        exp = get_expectation_class("expect_compound_columns_to_be_unique")(
            kwargs={"column_list": ["first", "second"]}
        )
        result = exp.validate(df)
        assert result.success is False
