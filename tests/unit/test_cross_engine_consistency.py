"""
Cross-Engine Consistency Tests (P0)

Verifies that identical rules evaluated against identical datasets across
Pandas, Polars, Push-Down SQL (SQLite), and PySpark (if available) produce
the exact same semantic results (success, element_count, unexpected_count,
unexpected_percent, and quality_score).
"""

from __future__ import annotations

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

import validatex as vx
from validatex.core.expectation import Expectation, get_expectation_class, list_expectations
from validatex.core.result import _SEVERITY_MAP
from validatex.integrations.bigquery import ValidateXBigQuery
from validatex.integrations.snowflake import ValidateXSnowflake

try:
    import polars as pl

    HAS_POLARS = True
except ImportError:
    HAS_POLARS = False


@pytest.fixture
def canonical_pandas_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "user_id": [1, 2, 2, 4, 5],
            "name": ["Alice", "Bob", None, "Diana", "Eve"],
            "age": [25, 30, 17, 40, 150],
            "status": ["ACTIVE", "ACTIVE", "INVALID_STATUS", "PENDING", "INACTIVE"],
            "email": [
                "alice@example.com",
                "bob@example.com",
                "not-an-email",
                "diana@example.com",
                "eve@example.com",
            ],
            "score": [80.0, 90.0, 70.0, 85.0, 95.0],
        }
    )


@pytest.fixture
def canonical_sql_engine(canonical_pandas_df: pd.DataFrame):
    engine = create_engine("sqlite:///:memory:")
    canonical_pandas_df.to_sql("users", engine, index=False)
    return engine


def test_all_50_registered_expectations_have_explicit_severity_and_all_4_engines():
    """Every registered expectation must have an explicit entry in _SEVERITY_MAP and support all 4 engines."""
    registered = set(list_expectations())
    assert len(registered) == 50, f"Expected 50 expectations, got {len(registered)}"
    mapped = set(_SEVERITY_MAP.keys())
    missing = registered - mapped
    assert not missing, f"Expectations missing from _SEVERITY_MAP: {sorted(missing)}"

    # Verify every expectation implements _validate_pandas, _validate_polars, _validate_spark, and _validate_sql
    for exp_name in sorted(registered):
        cls = get_expectation_class(exp_name)
        assert cls._validate_pandas is not Expectation._validate_pandas, f"{exp_name} missing _validate_pandas"
        assert cls._validate_polars is not Expectation._validate_polars, f"{exp_name} missing _validate_polars"
        assert cls._validate_spark is not Expectation._validate_spark, f"{exp_name} missing _validate_spark"
        assert hasattr(cls, "_validate_sql"), f"{exp_name} missing _validate_sql"


def test_empty_suite_quality_score_is_zero(canonical_pandas_df: pd.DataFrame):
    """An empty suite (0 configured checks) must report 0.0 quality score, not 100.0."""
    empty_suite = vx.ExpectationSuite("empty_suite")
    res = vx.validate(canonical_pandas_df, empty_suite, engine="pandas")
    assert res.compute_quality_score() == 0.0
    assert res.statistics["quality_score_scope"] == "configured_checks_only"
    assert res.checked_columns == 0
    assert res.total_columns == 6
    assert res.column_coverage_percent == 0.0


def test_column_coverage_metrics(canonical_pandas_df: pd.DataFrame):
    """Verify checked_columns and column_coverage_percent reflect rule coverage."""
    suite = (
        vx.ExpectationSuite("partial_coverage_suite", version="2.1.0", owner="data-platform")
        .add("expect_column_to_not_be_null", column="user_id", owner="identity-team")
        .add("expect_column_values_to_be_between", column="age", min_value=0, max_value=200)
        .add("expect_column_values_to_be_between", column="score", min_value=0, max_value=100)
    )
    res = vx.validate(canonical_pandas_df, suite, engine="pandas")
    assert res.compute_quality_score() == 100.0
    assert res.checked_columns == 3
    assert res.total_columns == 6
    assert res.column_coverage_percent == 50.0
    assert res.suite_meta["version"] == "2.1.0"
    assert res.suite_meta["owner"] == "data-platform"


@pytest.mark.skipif(not HAS_POLARS, reason="Polars not installed")
def test_pandas_polars_sql_core_expectation_parity(
    canonical_pandas_df: pd.DataFrame, canonical_sql_engine
):
    """
    Same rule, same semantics: verify Pandas, Polars, and Push-Down SQL
    produce identical results on the exact same dataset across table, column,
    aggregate, statistical, format, and conditional expectations.
    """
    suite = (
        vx.ExpectationSuite("cross_engine_parity")
        .add("expect_table_row_count_to_equal", value=5)
        .add("expect_table_row_count_to_be_between", min_value=3, max_value=10)
        .add(
            "expect_table_columns_to_match_ordered_list",
            column_list=["user_id", "name", "age", "status", "email", "score"],
        )
        .add(
            "expect_table_columns_to_match_set",
            column_set=["score", "email", "status", "age", "name", "user_id"],
        )
        .add("expect_table_column_count_to_equal", value=6)
        .add("expect_column_to_exist", column="user_id")
        .add("expect_column_to_exist", column="missing_col")
        .add("expect_column_to_not_be_null", column="name")
        .add("expect_column_values_to_be_unique", column="user_id")
        .add("expect_column_values_to_be_between", column="age", min_value=18, max_value=120)
        .add("expect_column_values_to_be_in_set", column="status", value_set=["ACTIVE", "PENDING", "INACTIVE"])
        .add("expect_column_values_to_not_be_in_set", column="status", value_set=["INVALID_STATUS"])
        .add("expect_column_value_lengths_to_be_between", column="status", min_value=6, max_value=8)
        .add("expect_column_max_to_be_between", column="score", min_value=90.0, max_value=100.0)
        .add("expect_column_min_to_be_between", column="score", min_value=60.0, max_value=75.0)
        .add("expect_column_mean_to_be_between", column="score", min_value=80.0, max_value=90.0)
        .add("expect_column_sum_to_be_between", column="score", min_value=400.0, max_value=450.0)
        .add("expect_column_null_percentage_to_be_less_than", column="name", max_pct=25.0)
        .add("expect_column_values_to_be_positive", column="age")
        .add("expect_column_values_to_be_all_uppercase", column="status")
        .add("expect_column_pair_values_a_to_be_greater_than_b", column="score", column_b="age")
        .add("expect_compound_columns_to_be_unique", column_list=["user_id", "status"])
        .add(
            "expect_column_values_to_be_null_when",
            column="name",
            condition_column="status",
            condition_value="INVALID_STATUS",
        )
    )

    res_pd = vx.validate(canonical_pandas_df, suite, engine="pandas")
    res_pl = vx.validate(pl.from_pandas(canonical_pandas_df), suite, engine="polars")
    res_sql = vx.validate("users", suite, engine="sql", sql_engine=canonical_sql_engine)

    assert res_pd.compute_quality_score() == res_pl.compute_quality_score() == res_sql.compute_quality_score()

    for idx, exp_name in enumerate([e.expectation_type for e in suite]):
        r_pd = res_pd.results[idx]
        r_pl = res_pl.results[idx]
        r_sq = res_sql.results[idx]

        assert r_pd.success == r_pl.success == r_sq.success, f"Mismatch on success for #{idx} ({exp_name})"
        assert (
            r_pd.unexpected_count == r_pl.unexpected_count == r_sq.unexpected_count
        ), f"Mismatch on unexpected_count for #{idx} ({exp_name}): pd={r_pd.unexpected_count}, pl={r_pl.unexpected_count}, sql={r_sq.unexpected_count}"
        assert (
            round(r_pd.unexpected_percent, 2)
            == round(r_pl.unexpected_percent, 2)
            == round(r_sq.unexpected_percent, 2)
        ), f"Mismatch on unexpected_percent for #{idx} ({exp_name})"


@pytest.mark.skipif(not HAS_POLARS, reason="Polars not installed")
def test_pandas_polars_sql_extended_expectation_parity(
    canonical_pandas_df: pd.DataFrame, canonical_sql_engine
):
    """Verify extended expectations (format, statistical, aggregate, sequential) match across Pandas, Polars, and SQL."""
    suite = (
        vx.ExpectationSuite("extended_parity")
        .add("expect_column_values_to_be_in_set", column="status", value_set=["ACTIVE", "PENDING", "INACTIVE"])
        .add("expect_column_values_to_not_be_in_set", column="status", value_set=["INVALID_STATUS"])
        .add("expect_column_values_to_match_regex", column="email", regex=r"^[\w.+-]+@[\w-]+\.[\w.-]+$")
        .add("expect_column_values_to_be_valid_email", column="email")
        .add("expect_column_max_to_be_between", column="score", min_value=90.0, max_value=100.0)
        .add("expect_column_min_to_be_between", column="score", min_value=60.0, max_value=75.0)
        .add("expect_column_mean_to_be_between", column="score", min_value=80.0, max_value=90.0)
        .add("expect_column_stdev_to_be_between", column="score", min_value=5.0, max_value=15.0)
        .add("expect_column_sum_to_be_between", column="score", min_value=400.0, max_value=450.0)
        .add("expect_column_median_to_be_between", column="score", min_value=80.0, max_value=90.0)
        .add("expect_column_values_to_be_increasing", column="user_id", strictly=True)
        .add("expect_compound_columns_to_be_unique", column_list=["user_id", "status"])
    )

    res_pd = vx.validate(canonical_pandas_df, suite, engine="pandas")
    res_pl = vx.validate(pl.from_pandas(canonical_pandas_df), suite, engine="polars")
    res_sql = vx.validate("users", suite, engine="sql", sql_engine=canonical_sql_engine)

    assert res_pd.compute_quality_score() == res_pl.compute_quality_score() == res_sql.compute_quality_score()
    for r_pd, r_pl, r_sq in zip(res_pd.results, res_pl.results, res_sql.results):
        assert r_pd.expectation_type == r_pl.expectation_type == r_sq.expectation_type
        assert r_pd.success == r_pl.success == r_sq.success, f"Success mismatch on {r_pd.expectation_type}"
        assert r_pd.element_count == r_pl.element_count == r_sq.element_count, f"Element count mismatch on {r_pd.expectation_type}"
        assert r_pd.unexpected_count == r_pl.unexpected_count == r_sq.unexpected_count, f"Unexpected count mismatch on {r_pd.expectation_type}"
        assert round(r_pd.unexpected_percent, 4) == round(r_pl.unexpected_percent, 4) == round(r_sq.unexpected_percent, 4)


@pytest.mark.skipif(not HAS_POLARS, reason="Polars not installed")
def test_all_null_column_aggregate_parity():
    """Verify all-null columns behave identically on Pandas, Polars, and SQL for min/max/mean/stdev."""
    df_pd = pd.DataFrame({"val": [None, None, None]}, dtype="float64")
    df_pl = pl.DataFrame({"val": [None, None, None]}, schema={"val": pl.Float64})
    sql_eng = create_engine("sqlite:///:memory:")
    df_pd.to_sql("null_table", sql_eng, index=False)

    suite = (
        vx.ExpectationSuite("all_null_stats")
        .add("expect_column_max_to_be_between", column="val", min_value=0, max_value=10)
        .add("expect_column_min_to_be_between", column="val", min_value=0, max_value=10)
        .add("expect_column_mean_to_be_between", column="val", min_value=0, max_value=10)
        .add("expect_column_stdev_to_be_between", column="val", min_value=0, max_value=10)
    )

    res_pd = vx.validate(df_pd, suite, engine="pandas")
    res_pl = vx.validate(df_pl, suite, engine="polars")
    res_sql = vx.validate("null_table", suite, engine="sql", sql_engine=sql_eng)

    for r_pd, r_pl, r_sq in zip(res_pd.results, res_pl.results, res_sql.results):
        assert r_pd.success is False
        assert r_pl.success is False
        assert r_sq.success is False
        assert r_pd.observed_value is None
        assert r_pl.observed_value is None
        assert r_sq.observed_value is None


def test_snowflake_and_bigquery_helpers_dispatch_sql(canonical_sql_engine):
    """Verify ValidateXSnowflake and ValidateXBigQuery helpers work with vx.validate(sql_engine=...)."""
    suite = (
        vx.ExpectationSuite("warehouse_suite")
        .add("expect_table_row_count_to_equal", value=5)
        .add("expect_column_to_exist", column="email")
    )
    sf_res = ValidateXSnowflake.validate_table(canonical_sql_engine, "users", suite)
    bq_res = ValidateXBigQuery.validate_table(canonical_sql_engine, "users", suite)

    assert sf_res.success is True
    assert sf_res.data_source == "snowflake://users"
    assert bq_res.success is True
    assert bq_res.data_source == "bigquery://users"
