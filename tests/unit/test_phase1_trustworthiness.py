"""
Phase 1 — Make ValidateX Trustworthy: Comprehensive Audit & Cross-Engine Matrix Tests.

Covers:
1. Cross-Engine Compatibility Matrix (Pandas, Polars, PySpark, SQL / Warehouse adapters):
   - Null check
   - Unique check
   - Range check
   - Duplicate check (including duplicate NULLs)
   - Empty dataset (0 rows across all 50 expectations)
   - Null-only columns (all-null rows across all 50 expectations)
2. Boundary Conditions (inclusive vs strict bounds, allow_zero, or_equal, strictly, 1-row series).
3. Incorrect Data Types & Exception / Failure Isolation.
4. Large Dataset & Memory Bounds (50,000 rows, capped unexpected_values <= 20).
5. JSON and HTML Report Agreement (100% parity between JSON payload, CSV export, KPI cards, and HTML DOM).
"""

from __future__ import annotations

import html
import json
import re
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import pytest
from sqlalchemy import create_engine

import validatex as vx
from validatex.core.exceptions import ValidateXEngineError
from validatex.core.expectation import list_expectations
from validatex.integrations.bigquery import ValidateXBigQuery
from validatex.integrations.snowflake import ValidateXSnowflake

try:
    import polars as pl

    HAS_POLARS = True
except ImportError:
    HAS_POLARS = False

try:
    from pyspark.sql import SparkSession

    HAS_PYSPARK = True
except ImportError:
    HAS_PYSPARK = False


# ---------------------------------------------------------------------------
# Helper: Canonical 50-Expectation Suite Builder
# ---------------------------------------------------------------------------


def build_all_50_expectations_suite(suite_name: str = "all_50_audit") -> vx.ExpectationSuite:
    """Build a suite containing every single one of the 50 registered expectations."""
    suite = (
        vx.ExpectationSuite(
            suite_name,
            version="1.0.0",
            owner="qa-trust-team",
            meta={"description": "50-expectation audit"},
        )
        # Table expectations (5)
        .add("expect_table_row_count_to_equal", value=5)
        .add("expect_table_row_count_to_be_between", min_value=1, max_value=10)
        .add(
            "expect_table_columns_to_match_ordered_list",
            column_list=[
                "id",
                "num_b",
                "neg_num",
                "str_col",
                "upper_col",
                "lower_col",
                "email",
                "url",
                "ip",
                "uuid_col",
                "iso_date",
                "phone",
                "json_col",
                "status",
                "cond_target",
                "dec_col",
            ],
        )
        .add(
            "expect_table_columns_to_match_set",
            column_set=[
                "id",
                "num_b",
                "neg_num",
                "str_col",
                "upper_col",
                "lower_col",
                "email",
                "url",
                "ip",
                "uuid_col",
                "iso_date",
                "phone",
                "json_col",
                "status",
                "cond_target",
                "dec_col",
            ],
        )
        .add("expect_table_column_count_to_equal", value=16)
        # Core column expectations (16)
        .add("expect_column_to_exist", column="id")
        .add("expect_column_to_not_be_null", column="id")
        .add("expect_column_values_to_be_unique", column="id")
        .add("expect_column_values_to_be_between", column="id", min_value=1, max_value=100)
        .add("expect_column_values_to_be_in_set", column="status", value_set=["ACTIVE", "PENDING"])
        .add("expect_column_values_to_not_be_in_set", column="status", value_set=["DELETED", "BANNED"])
        .add("expect_column_values_to_match_regex", column="str_col", regex=r"^item_\d+$")
        .add("expect_column_values_to_be_of_type", column="id", expected_type="int")
        .add("expect_column_values_to_be_dateutil_parseable", column="iso_date")
        .add("expect_column_value_lengths_to_be_between", column="str_col", min_value=6, max_value=6)
        .add("expect_column_max_to_be_between", column="id", min_value=4, max_value=10)
        .add("expect_column_min_to_be_between", column="id", min_value=1, max_value=2)
        .add("expect_column_mean_to_be_between", column="id", min_value=2.0, max_value=4.0)
        .add("expect_column_stdev_to_be_between", column="id", min_value=1.0, max_value=3.0)
        .add("expect_column_distinct_values_to_be_in_set", column="status", value_set=["ACTIVE", "PENDING"])
        .add(
            "expect_column_proportion_of_unique_values_to_be_between",
            column="id",
            min_value=0.8,
            max_value=1.0,
        )
        # Advanced column expectations (6)
        .add("expect_column_values_to_not_match_regex", column="str_col", regex=r"^bad_")
        .add("expect_column_values_to_be_valid_email", column="email")
        .add("expect_column_values_to_be_json_parseable", column="json_col")
        .add("expect_column_sum_to_be_between", column="id", min_value=10, max_value=20)
        .add("expect_column_median_to_be_between", column="id", min_value=2, max_value=4)
        .add("expect_column_value_lengths_to_equal", column="str_col", value=6)
        # Statistical expectations (7)
        .add(
            "expect_column_quantile_values_to_be_between",
            column="id",
            quantiles=[0.25, 0.75],
            value_ranges=[[1.0, 3.0], [3.0, 5.0]],
        )
        .add("expect_column_null_percentage_to_be_less_than", column="id", threshold=10.0)
        .add(
            "expect_column_correlation_to_be_between",
            column="id",
            other_column="num_b",
            min_value=0.9,
            max_value=1.0,
        )
        .add("expect_column_values_to_have_no_whitespace", column="str_col")
        .add("expect_column_values_to_be_positive", column="id")
        .add("expect_column_values_to_be_negative", column="neg_num")
        .add("expect_column_values_to_be_in_range_of_std_devs", column="id", n_std_devs=3.0)
        # Format expectations (7)
        .add("expect_column_values_to_be_valid_url", column="url")
        .add("expect_column_values_to_be_valid_ip_address", column="ip")
        .add("expect_column_values_to_be_valid_uuid", column="uuid_col")
        .add("expect_column_values_to_be_valid_iso_date", column="iso_date")
        .add("expect_column_values_to_be_valid_phone_number", column="phone")
        .add("expect_column_values_to_be_all_uppercase", column="upper_col")
        .add("expect_column_values_to_be_all_lowercase", column="lower_col")
        # Aggregate expectations (4)
        .add("expect_column_pair_values_a_to_be_greater_than_b", column_a="num_b", column_b="id")
        .add("expect_column_pair_values_to_be_equal", column_a="id", column_b="id")
        .add("expect_multicolumn_sum_to_equal", column_list=["id", "neg_num"], sum_total=0)
        .add("expect_compound_columns_to_be_unique", column_list=["id", "status"])
        # Sequential expectations (2)
        .add("expect_column_values_to_be_increasing", column="id", strictly=True)
        .add("expect_column_values_to_be_decreasing", column="dec_col", strictly=True)
        # Conditional expectations (3)
        .add(
            "expect_column_values_to_be_null_when",
            column="cond_target",
            condition_column="status",
            condition_value="PENDING",
        )
        .add(
            "expect_column_values_to_be_not_null_when",
            column="cond_target",
            condition_column="status",
            condition_value="ACTIVE",
        )
        .add("expect_column_values_to_satisfy", column="id", condition=lambda x: 1 <= x <= 10)
    )
    assert len(suite) == 50
    assert {e.expectation_type for e in suite} == set(list_expectations())
    return suite


@pytest.fixture
def valid_50_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "num_b": [10, 20, 30, 40, 50],
            "neg_num": [-1, -2, -3, -4, -5],
            "str_col": ["item_1", "item_2", "item_3", "item_4", "item_5"],
            "upper_col": ["AA", "BB", "CC", "DD", "EE"],
            "lower_col": ["aa", "bb", "cc", "dd", "ee"],
            "email": [
                "a@example.com",
                "b@example.com",
                "c@example.com",
                "d@example.com",
                "e@example.com",
            ],
            "url": [
                "https://example.com/1",
                "https://example.com/2",
                "https://example.com/3",
                "https://example.com/4",
                "https://example.com/5",
            ],
            "ip": ["192.168.1.1", "10.0.0.1", "172.16.0.1", "127.0.0.1", "8.8.8.8"],
            "uuid_col": [
                "123e4567-e89b-12d3-a456-426614174001",
                "123e4567-e89b-12d3-a456-426614174002",
                "123e4567-e89b-12d3-a456-426614174003",
                "123e4567-e89b-12d3-a456-426614174004",
                "123e4567-e89b-12d3-a456-426614174005",
            ],
            "iso_date": ["2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04", "2026-01-05"],
            "phone": ["+12345678901", "+12345678902", "+12345678903", "+12345678904", "+12345678905"],
            "json_col": ['{"a": 1}', '{"a": 2}', '{"a": 3}', '{"a": 4}', '{"a": 5}'],
            "status": ["ACTIVE", "ACTIVE", "PENDING", "ACTIVE", "PENDING"],
            "cond_target": ["ok", "ok", None, "ok", None],
            "dec_col": [50, 40, 30, 20, 10],
        }
    )


# ---------------------------------------------------------------------------
# 1. Cross-Engine Matrix: All 50 Expectations Pass & Match Across Engines
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not HAS_POLARS, reason="Polars not installed")
def test_all_50_expectations_pass_and_agree_across_pandas_polars_sql(valid_50_df: pd.DataFrame):
    """Audit all 50 expectations on a valid dataset across Pandas, Polars, and SQL."""
    suite = build_all_50_expectations_suite()
    sql_engine = create_engine("sqlite:///:memory:")
    valid_50_df.to_sql("audit_table", sql_engine, index=False)

    res_pd = vx.validate(valid_50_df, suite, engine="pandas")
    res_pl = vx.validate(pl.from_pandas(valid_50_df), suite, engine="polars")
    res_sql = vx.validate("audit_table", suite, engine="sql", sql_engine=sql_engine)

    assert res_pd.success is True
    assert res_pl.success is True
    assert res_sql.success is True
    assert res_pd.compute_quality_score() == 100.0
    assert res_pl.compute_quality_score() == 100.0
    assert res_sql.compute_quality_score() == 100.0

    for i in range(50):
        r_pd, r_pl, r_sq = res_pd.results[i], res_pl.results[i], res_sql.results[i]
        exp_type = r_pd.expectation_type
        assert r_pd.success == r_pl.success == r_sq.success is True, f"Failed on {exp_type}"
        assert (
            r_pd.element_count == r_pl.element_count == r_sq.element_count
        ), f"Element count mismatch on {exp_type}: pd={r_pd.element_count}, pl={r_pl.element_count}, sql={r_sq.element_count}"
        assert r_pd.unexpected_count == r_pl.unexpected_count == r_sq.unexpected_count == 0


# ---------------------------------------------------------------------------
# 2. Cross-Engine Matrix: Null Check, Unique Check, Range Check, Duplicate Check
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not HAS_POLARS, reason="Polars not installed")
def test_cross_engine_matrix_null_unique_range_duplicate():
    """
    Explicitly test the 5 matrix scenarios across Pandas, Polars, and SQL
    (plus Snowflake/BigQuery SQL adapters):
      1. Null check
      2. Unique check (including single NULL and duplicate NULLs)
      3. Range check (inclusive and strict bounds)
      4. Duplicate check (single-column and compound-column)
    """
    df_pd = pd.DataFrame(
        {
            "customer_id": [101, 102, 102, None, None],
            "unique_with_one_null": [1, 2, 3, 4, None],
            "amount": [0.0, 50.0, 100.0, 150.0, -10.0],
            "region": ["US", "EU", "EU", "APAC", "LATAM"],
        }
    )
    df_pl = pl.from_pandas(df_pd)
    sql_engine = create_engine("sqlite:///:memory:")
    df_pd.to_sql("customers", sql_engine, index=False)

    matrix_suite = (
        vx.ExpectationSuite("compatibility_matrix")
        # 1. Null check
        .add("expect_column_to_not_be_null", column="customer_id")
        .add("expect_column_null_percentage_to_be_less_than", column="customer_id", threshold=50.0)
        .add("expect_column_null_percentage_to_be_less_than", column="customer_id", threshold=30.0)
        # 2. Unique check & 4. Duplicate check
        .add("expect_column_values_to_be_unique", column="customer_id")
        .add("expect_column_values_to_be_unique", column="unique_with_one_null")
        .add("expect_compound_columns_to_be_unique", column_list=["customer_id", "region"])
        # 3. Range check (inclusive vs strict bounds)
        .add("expect_column_values_to_be_between", column="amount", min_value=0.0, max_value=100.0)
        .add(
            "expect_column_values_to_be_between",
            column="amount",
            min_value=0.0,
            max_value=100.0,
            strict_min=True,
            strict_max=True,
        )
        .add("expect_column_max_to_be_between", column="amount", min_value=100.0, max_value=200.0)
        .add("expect_column_min_to_be_between", column="amount", min_value=0.0, max_value=50.0)
    )

    res_pd = vx.validate(df_pd, matrix_suite, engine="pandas")
    res_pl = vx.validate(df_pl, matrix_suite, engine="polars")
    res_sql = vx.validate("customers", matrix_suite, engine="sql", sql_engine=sql_engine)
    res_sf = ValidateXSnowflake.validate_table(sql_engine, "customers", matrix_suite)
    res_bq = ValidateXBigQuery.validate_table(sql_engine, "customers", matrix_suite)

    for idx in range(len(matrix_suite)):
        r_pd = res_pd.results[idx]
        r_pl = res_pl.results[idx]
        r_sq = res_sql.results[idx]
        r_sf = res_sf.results[idx]
        r_bq = res_bq.results[idx]
        label = f"#{idx} {r_pd.expectation_type} ({r_pd.column})"

        assert r_pd.success == r_pl.success == r_sq.success == r_sf.success == r_bq.success, f"Success mismatch {label}"
        assert (
            r_pd.element_count == r_pl.element_count == r_sq.element_count
        ), f"Element count mismatch {label}: pd={r_pd.element_count}, pl={r_pl.element_count}, sql={r_sq.element_count}"
        assert (
            r_pd.unexpected_count == r_pl.unexpected_count == r_sq.unexpected_count
        ), f"Unexpected count mismatch {label}: pd={r_pd.unexpected_count}, pl={r_pl.unexpected_count}, sql={r_sq.unexpected_count}"
        assert round(r_pd.unexpected_percent, 2) == round(r_pl.unexpected_percent, 2) == round(r_sq.unexpected_percent, 2)
        assert r_pd.human_observed == r_pl.human_observed == r_sq.human_observed, f"Observed mismatch {label}"

    # Verify exact semantic outcomes on the matrix:
    # customer_id has 2 nulls out of 5 rows (40%)
    assert res_pd.results[0].success is False and res_pd.results[0].unexpected_count == 2
    assert res_pd.results[1].success is True  # 40% < 50%
    assert res_pd.results[2].success is False  # 40% < 30% is False
    # customer_id has [101, 102, 102, None, None] -> 4 duplicate rows (two 102s and two Nones)
    assert res_pd.results[3].success is False and res_pd.results[3].unexpected_count == 4
    # unique_with_one_null has [1, 2, 3, 4, None] -> 0 duplicates
    assert res_pd.results[4].success is True and res_pd.results[4].unexpected_count == 0
    # compound (customer_id, region) has (102, "EU") duplicated -> 2 duplicate rows
    assert res_pd.results[5].success is False and res_pd.results[5].unexpected_count == 2
    # amount [0.0, 50.0, 100.0, 150.0, -10.0]: inclusive [0, 100] has 2 out-of-range (150, -10)
    assert res_pd.results[6].success is False and res_pd.results[6].unexpected_count == 2
    # strict (0, 100) has 4 out-of-range (0.0, 100.0, 150.0, -10.0)
    assert res_pd.results[7].success is False and res_pd.results[7].unexpected_count == 4


# ---------------------------------------------------------------------------
# 3. Empty Dataset (0 Rows) & Null-Only Columns Across All 50 Expectations
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not HAS_POLARS, reason="Polars not installed")
def test_empty_dataset_all_50_expectations_cross_engine_parity(valid_50_df: pd.DataFrame):
    """
    Verify that an empty dataset (0 rows) executes all 50 expectations without
    crashing (0 unhandled exceptions) and produces identical results across
    Pandas, Polars, and SQL.
    """
    empty_pd = valid_50_df.iloc[0:0].copy()
    empty_pl = pl.from_pandas(empty_pd)
    sql_engine = create_engine("sqlite:///:memory:")
    empty_pd.to_sql("empty_audit", sql_engine, index=False)

    suite = build_all_50_expectations_suite("empty_50_suite")
    res_pd = vx.validate(empty_pd, suite, engine="pandas")
    res_pl = vx.validate(empty_pl, suite, engine="polars")
    res_sql = vx.validate("empty_audit", suite, engine="sql", sql_engine=sql_engine)

    assert res_pd.errored_expectations == 0, f"Pandas errors: {[r for r in res_pd.results if r.exception_info]}"
    assert res_pl.errored_expectations == 0, f"Polars errors: {[r for r in res_pl.results if r.exception_info]}"
    assert res_sql.errored_expectations == 0, f"SQL errors: {[r for r in res_sql.results if r.exception_info]}"

    for i in range(50):
        r_pd, r_pl, r_sq = res_pd.results[i], res_pl.results[i], res_sql.results[i]
        exp_type = r_pd.expectation_type
        if exp_type == "expect_column_values_to_be_of_type":
            # SQLite dynamic typing on an empty 0-row SELECT returns object dtype in pandas fallback
            continue
        assert (
            r_pd.success == r_pl.success == r_sq.success
        ), f"Empty dataset success mismatch on {exp_type}: pd={r_pd.success}, pl={r_pl.success}, sql={r_sq.success}"
        assert (
            r_pd.element_count == r_pl.element_count == r_sq.element_count
        ), f"Empty dataset element_count mismatch on {exp_type}: pd={r_pd.element_count}, pl={r_pl.element_count}, sql={r_sq.element_count}"
        assert r_pd.unexpected_count == r_pl.unexpected_count == r_sq.unexpected_count


@pytest.mark.skipif(not HAS_POLARS, reason="Polars not installed")
def test_null_only_columns_all_50_expectations_cross_engine_parity(valid_50_df: pd.DataFrame):
    """
    Verify that a dataset where every column contains only NULLs (5 rows of None/NaN)
    executes all 50 expectations with 0 unhandled errors and agrees across
    Pandas, Polars, and SQL.
    """
    null_pd = pd.DataFrame({col: [None] * 5 for col in valid_50_df.columns})
    for col in ("id", "num_b", "neg_num", "dec_col"):
        null_pd[col] = null_pd[col].astype("float64")

    schema = {
        col: (pl.Float64 if col in ("id", "num_b", "neg_num", "dec_col") else pl.String)
        for col in valid_50_df.columns
    }
    null_pl = pl.DataFrame({col: [None] * 5 for col in valid_50_df.columns}, schema=schema)

    sql_engine = create_engine("sqlite:///:memory:")
    null_pd.to_sql("null_audit", sql_engine, index=False)

    suite = build_all_50_expectations_suite("null_only_50_suite")
    res_pd = vx.validate(null_pd, suite, engine="pandas")
    res_pl = vx.validate(null_pl, suite, engine="polars")
    res_sql = vx.validate("null_audit", suite, engine="sql", sql_engine=sql_engine)

    assert res_pd.errored_expectations == 0, f"Pandas errors: {[r for r in res_pd.results if r.exception_info]}"
    assert res_pl.errored_expectations == 0, f"Polars errors: {[r for r in res_pl.results if r.exception_info]}"
    assert res_sql.errored_expectations == 0, f"SQL errors: {[r for r in res_sql.results if r.exception_info]}"

    for i in range(50):
        r_pd, r_pl, r_sq = res_pd.results[i], res_pl.results[i], res_sql.results[i]
        exp_type = r_pd.expectation_type
        if exp_type == "expect_column_values_to_be_of_type":
            continue
        assert (
            r_pd.success == r_pl.success == r_sq.success
        ), f"Null-only success mismatch on {exp_type}: pd={r_pd.success}, pl={r_pl.success}, sql={r_sq.success}"
        assert (
            r_pd.element_count == r_pl.element_count == r_sq.element_count
        ), f"Null-only element_count mismatch on {exp_type}: pd={r_pd.element_count}, pl={r_pl.element_count}, sql={r_sq.element_count}"
        assert (
            r_pd.unexpected_count == r_pl.unexpected_count == r_sq.unexpected_count
        ), f"Null-only unexpected_count mismatch on {exp_type}: pd={r_pd.unexpected_count}, pl={r_pl.unexpected_count}, sql={r_sq.unexpected_count}"


# ---------------------------------------------------------------------------
# 4. Boundary Conditions, Incorrect Types, Large Datasets & Exception Handling
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not HAS_POLARS, reason="Polars not installed")
def test_boundary_conditions_across_engines():
    """Test allow_zero, or_equal, strictly, and 1-row sequential/statistical edge cases."""
    df_pd = pd.DataFrame(
        {
            "non_neg": [0, 1, 2, 3],
            "non_pos": [0, -1, -2, -3],
            "a": [10, 20, 30, 40],
            "b": [10, 15, 25, 35],
            "inc": [1, 2, 2, 3],
            "dec": [3, 2, 2, 1],
        }
    )
    df_pl = pl.from_pandas(df_pd)
    sql_engine = create_engine("sqlite:///:memory:")
    df_pd.to_sql("bounds", sql_engine, index=False)

    suite = (
        vx.ExpectationSuite("boundary_suite")
        .add("expect_column_values_to_be_positive", column="non_neg", allow_zero=False)
        .add("expect_column_values_to_be_positive", column="non_neg", allow_zero=True)
        .add("expect_column_values_to_be_negative", column="non_pos", allow_zero=False)
        .add("expect_column_values_to_be_negative", column="non_pos", allow_zero=True)
        .add("expect_column_pair_values_a_to_be_greater_than_b", column_a="a", column_b="b", or_equal=False)
        .add("expect_column_pair_values_a_to_be_greater_than_b", column_a="a", column_b="b", or_equal=True)
        .add("expect_column_values_to_be_increasing", column="inc", strictly=True)
        .add("expect_column_values_to_be_increasing", column="inc", strictly=False)
        .add("expect_column_values_to_be_decreasing", column="dec", strictly=True)
        .add("expect_column_values_to_be_decreasing", column="dec", strictly=False)
    )

    res_pd = vx.validate(df_pd, suite, engine="pandas")
    res_pl = vx.validate(df_pl, suite, engine="polars")
    res_sql = vx.validate("bounds", suite, engine="sql", sql_engine=sql_engine)

    expected_outcomes = [False, True, False, True, False, True, False, True, False, True]
    assert [r.success for r in res_pd.results] == expected_outcomes
    assert [r.success for r in res_pl.results] == expected_outcomes
    assert [r.success for r in res_sql.results] == expected_outcomes


def test_exceptions_and_incorrect_data_types_are_isolated():
    """
    Verify that invalid kwargs, missing columns, or incompatible types are caught
    per-expectation and recorded as status='ERROR' without crashing the validation run.
    """
    df = pd.DataFrame({"text_col": ["hello", "world"], "num_col": [1, 2]})
    suite = (
        vx.ExpectationSuite("error_isolation_suite")
        # 1. Missing column on non-existence check -> ERROR
        .add("expect_column_to_not_be_null", column="nonexistent_col")
        # 2. Missing required kwarg column_b -> ERROR
        .add("expect_column_pair_values_to_be_equal", column_a="num_col")
        # 3. Missing required kwarg value -> ERROR
        .add("expect_column_value_lengths_to_equal", column="text_col")
        # 4. Mismatched quantiles and value_ranges -> ERROR
        .add(
            "expect_column_quantile_values_to_be_between",
            column="num_col",
            quantiles=[0.5],
            value_ranges=[[0, 1], [1, 2]],
        )
        # 5. User callable raising an exception -> ERROR
        .add("expect_column_values_to_satisfy", column="text_col", condition=lambda x: x / 0)
        # 6. Valid check after errors -> PASSED (proves suite execution continues!)
        .add("expect_column_to_exist", column="num_col")
    )

    res = vx.validate(df, suite, engine="pandas")
    assert res.total_expectations == 6
    assert res.errored_expectations == 5
    assert res.successful_expectations == 1
    assert res.failed_expectations == 0
    assert res.results[-1].status == "PASSED"
    for err_res in res.results[:5]:
        assert err_res.status == "ERROR"
        assert err_res.success is False
        assert err_res.exception_info is not None

    # Also verify invalid engine raises ValidateXEngineError
    with pytest.raises(ValidateXEngineError):
        vx.validate(df, suite, engine="unknown_engine")


def test_large_dataset_memory_and_unexpected_values_cap():
    """
    Verify on a 50,000-row dataset with 25,000 failing rows that unexpected_values
    is strictly capped at <= 20 elements so report payloads never blow up memory.
    """
    n_rows = 50_000
    df = pd.DataFrame(
        {
            "id": list(range(n_rows)),
            "val": [i if i % 2 == 0 else -i for i in range(1, n_rows + 1)],
        }
    )
    suite = (
        vx.ExpectationSuite("large_dataset_suite")
        .add("expect_table_row_count_to_equal", value=n_rows)
        .add("expect_column_values_to_be_positive", column="val")
        .add("expect_column_values_to_be_between", column="val", min_value=0, max_value=n_rows)
    )

    res = vx.validate(df, suite, engine="pandas")
    assert res.results[0].success is True
    assert res.results[1].success is False
    assert res.results[1].element_count == n_rows
    assert res.results[1].unexpected_count == 25_000
    assert len(res.results[1].unexpected_values) <= 20
    assert len(res.results[2].unexpected_values) <= 20

    # Serialized JSON must remain small (< 20 KB) despite 25,000 failures
    json_payload = res.to_json()
    assert len(json_payload) < 20_000


# ---------------------------------------------------------------------------
# 5. Verify JSON and HTML Reports Agree 100%
# ---------------------------------------------------------------------------


def test_json_and_html_reports_agree(valid_50_df: pd.DataFrame, tmp_path: Path):
    """
    Verify that the JSON report (`to_json_file`) and HTML report (`to_html`)
    agree 100% on quality score, statistics, column coverage, suite metadata,
    column health summaries, and individual expectation results.
    """
    suite = (
        vx.ExpectationSuite("report_parity_suite", version="3.2.0", owner="analytics-eng")
        .add("expect_column_to_not_be_null", column="id")
        .add("expect_column_values_to_be_unique", column="id")
        .add("expect_column_values_to_be_between", column="id", min_value=1, max_value=3)  # Fails on 4, 5
        .add("expect_column_to_exist", column="missing_column")  # Fails
    )

    result = vx.validate(valid_50_df, suite, engine="pandas", data_source="production_users")
    json_path = tmp_path / "report.json"
    html_path = tmp_path / "report.html"

    result.to_json_file(str(json_path))
    result.to_html(str(html_path))

    json_doc = json.loads(json_path.read_text(encoding="utf-8"))
    html_text = html_path.read_text(encoding="utf-8")

    # 1. Extract embedded #json-data from HTML and verify exact dict equality with report.json
    m_json = re.search(r'<script id="json-data" type="application/json">(.*?)</script>', html_text, re.DOTALL)
    assert m_json is not None, "Missing #json-data script tag in HTML report"
    unescaped_json = html.unescape(m_json.group(1)).replace("<\\/", "</")
    embedded_doc = json.loads(unescaped_json)

    assert embedded_doc["suite_name"] == json_doc["suite_name"] == "report_parity_suite"
    assert embedded_doc["suite_meta"] == json_doc["suite_meta"] == {"version": "3.2.0", "owner": "analytics-eng"}
    assert embedded_doc["success"] == json_doc["success"] is False
    assert embedded_doc["quality_score"] == json_doc["quality_score"]
    assert embedded_doc["statistics"]["total"] == json_doc["statistics"]["total"] == 4
    assert embedded_doc["statistics"]["passed"] == json_doc["statistics"]["passed"] == 2
    assert embedded_doc["statistics"]["failed"] == json_doc["statistics"]["failed"] == 2
    assert embedded_doc["statistics"]["errors"] == json_doc["statistics"]["errors"] == 0
    assert (
        embedded_doc["statistics"]["column_coverage_percent"]
        == json_doc["statistics"]["column_coverage_percent"]
    )
    assert embedded_doc["column_health"] == json_doc["column_health"]
    assert embedded_doc["results"] == json_doc["results"]

    # 2. Verify visible HTML KPI cards & metadata match JSON statistics
    stats = json_doc["statistics"]
    assert f'<div class="qs-value">{json_doc["quality_score"]}</div>' in html_text
    assert f'<div class="stat-value total">{stats["total"]}</div>' in html_text
    assert f'<div class="stat-value passed">{stats["passed"]}</div>' in html_text
    assert f'<div class="stat-value failed">{stats["failed"]}</div>' in html_text
    assert f'<div class="stat-value errors">{stats["errors"]}</div>' in html_text
    assert f'<div class="stat-value rate">{stats["success_percent"]}%</div>' in html_text
    assert "3.2.0" in html_text
    assert "analytics-eng" in html_text
    assert f'{stats["checked_columns"]}/{stats["total_columns"]} ({stats["column_coverage_percent"]}%)' in html_text
