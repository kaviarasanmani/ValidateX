"""
Unit tests for the DataProfiler.
"""

import pytest
import pandas as pd
import numpy as np

from validatex.profiler.profiler import DataProfiler, DataProfile


@pytest.fixture
def sample_df():
    return pd.DataFrame(
        {
            "id": range(1, 101),
            "name": [f"User_{i}" for i in range(100)],
            "age": np.random.randint(18, 80, 100),
            "score": np.random.uniform(0, 100, 100),
            "category": np.random.choice(["A", "B", "C"], 100),
            "email": [f"user{i}@example.com" for i in range(100)],
        }
    )


@pytest.fixture
def df_with_nulls():
    return pd.DataFrame(
        {
            "a": [1, 2, None, 4, 5],
            "b": ["x", None, "z", None, "w"],
        }
    )


class TestDataProfiler:
    def test_profile_returns_data_profile(self, sample_df):
        profiler = DataProfiler()
        profile = profiler.profile(sample_df)
        assert isinstance(profile, DataProfile)
        assert profile.row_count == 100
        assert profile.column_count == 6

    def test_profile_column_count(self, sample_df):
        profiler = DataProfiler()
        profile = profiler.profile(sample_df)
        assert len(profile.columns) == 6

    def test_profile_numeric_stats(self, sample_df):
        profiler = DataProfiler()
        profile = profiler.profile(sample_df)

        age_profile = next(c for c in profile.columns if c.name == "age")
        assert age_profile.mean_value is not None
        assert age_profile.std_value is not None
        assert age_profile.min_value is not None
        assert age_profile.max_value is not None

    def test_profile_null_detection(self, df_with_nulls):
        profiler = DataProfiler()
        profile = profiler.profile(df_with_nulls)

        a_profile = next(c for c in profile.columns if c.name == "a")
        assert a_profile.null_count == 1

        b_profile = next(c for c in profile.columns if c.name == "b")
        assert b_profile.null_count == 2

    def test_profile_string_lengths(self, sample_df):
        profiler = DataProfiler()
        profile = profiler.profile(sample_df)

        name_profile = next(c for c in profile.columns if c.name == "name")
        assert name_profile.min_length is not None
        assert name_profile.max_length is not None

    def test_profile_top_values(self, sample_df):
        profiler = DataProfiler()
        profile = profiler.profile(sample_df)

        cat_profile = next(c for c in profile.columns if c.name == "category")
        assert len(cat_profile.top_values) > 0

    def test_profile_to_dict(self, sample_df):
        profiler = DataProfiler()
        profile = profiler.profile(sample_df)
        d = profile.to_dict()
        assert "row_count" in d
        assert "columns" in d

    def test_profile_summary_string(self, sample_df):
        profiler = DataProfiler()
        profile = profiler.profile(sample_df)
        summary = profile.summary()
        assert "ValidateX Data Profile" in summary
        assert "Rows" in summary

    def test_suggest_expectations(self, sample_df):
        profiler = DataProfiler()
        suite = profiler.suggest_expectations(sample_df, suite_name="auto")
        assert len(suite) > 0
        assert suite.name == "auto"

    def test_suggest_includes_not_null_for_clean_columns(self, sample_df):
        profiler = DataProfiler()
        suite = profiler.suggest_expectations(sample_df)
        exp_types = [e.expectation_type for e in suite.expectations]
        assert "expect_column_to_not_be_null" in exp_types

    def test_suggest_includes_column_exists(self, sample_df):
        profiler = DataProfiler()
        suite = profiler.suggest_expectations(sample_df)
        exp_types = [e.expectation_type for e in suite.expectations]
        assert "expect_column_to_exist" in exp_types
