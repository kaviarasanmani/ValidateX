import pytest
import polars as pl
import validatex as vx

# 1. Core Column Expectations (Existing)
def test_polars_expect_column_to_exist():
    df = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    suite_pass = vx.ExpectationSuite("polars_test").add("expect_column_to_exist", column="a")
    result_pass = vx.validate(df, suite_pass, engine="polars")
    assert result_pass.success
    
    suite_fail = vx.ExpectationSuite("polars_test").add("expect_column_to_exist", column="c")
    result_fail = vx.validate(df, suite_fail, engine="polars")
    assert not result_fail.success

def test_polars_expect_column_to_not_be_null():
    df = pl.DataFrame({"a": [1, 2, None]})
    suite_fail = vx.ExpectationSuite("polars_test").add("expect_column_to_not_be_null", column="a")
    result_fail = vx.validate(df, suite_fail, engine="polars")
    assert not result_fail.success
    
    df_clean = pl.DataFrame({"a": [1, 2, 3]})
    suite_pass = vx.ExpectationSuite("polars_test").add("expect_column_to_not_be_null", column="a")
    result_pass = vx.validate(df_clean, suite_pass, engine="polars")
    assert result_pass.success

def test_polars_expect_column_values_to_be_unique():
    df = pl.DataFrame({"a": [1, 2, 2]})
    suite_fail = vx.ExpectationSuite("polars_test").add("expect_column_values_to_be_unique", column="a")
    result_fail = vx.validate(df, suite_fail, engine="polars")
    assert not result_fail.success
    
    df_unique = pl.DataFrame({"a": [1, 2, 3]})
    suite_pass = vx.ExpectationSuite("polars_test").add("expect_column_values_to_be_unique", column="a")
    result_pass = vx.validate(df_unique, suite_pass, engine="polars")
    assert result_pass.success

def test_polars_expect_column_values_to_be_between():
    df = pl.DataFrame({"a": [10, 20, 30]})
    suite_pass = vx.ExpectationSuite("polars_test").add("expect_column_values_to_be_between", column="a", min_value=5, max_value=35)
    result_pass = vx.validate(df, suite_pass, engine="polars")
    assert result_pass.success
    
    suite_fail = vx.ExpectationSuite("polars_test").add("expect_column_values_to_be_between", column="a", min_value=15, max_value=25)
    result_fail = vx.validate(df, suite_fail, engine="polars")
    assert not result_fail.success

# 2. Table-level Expectations
def test_polars_table_expectations():
    df = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    
    # expect_table_row_count_to_equal
    suite = vx.ExpectationSuite("polars_test").add("expect_table_row_count_to_equal", value=3)
    assert vx.validate(df, suite, engine="polars").success
    
    # expect_table_row_count_to_be_between
    suite = vx.ExpectationSuite("polars_test").add("expect_table_row_count_to_be_between", min_value=2, max_value=4)
    assert vx.validate(df, suite, engine="polars").success
    
    # expect_table_columns_to_match_ordered_list
    suite = vx.ExpectationSuite("polars_test").add("expect_table_columns_to_match_ordered_list", column_list=["a", "b"])
    assert vx.validate(df, suite, engine="polars").success
    
    # expect_table_columns_to_match_set
    suite = vx.ExpectationSuite("polars_test").add("expect_table_columns_to_match_set", column_set=["b", "a"])
    assert vx.validate(df, suite, engine="polars").success
    
    # expect_table_column_count_to_equal
    suite = vx.ExpectationSuite("polars_test").add("expect_table_column_count_to_equal", value=2)
    assert vx.validate(df, suite, engine="polars").success

# 3. Sequential Expectations
def test_polars_sequential_expectations():
    df_inc = pl.DataFrame({"a": [1, 2, 3]})
    suite = vx.ExpectationSuite("polars_test").add("expect_column_values_to_be_increasing", column="a", strictly=True)
    assert vx.validate(df_inc, suite, engine="polars").success
    
    df_dec = pl.DataFrame({"a": [3, 2, 1]})
    suite = vx.ExpectationSuite("polars_test").add("expect_column_values_to_be_decreasing", column="a", strictly=True)
    assert vx.validate(df_dec, suite, engine="polars").success

# 4. Conditional/Custom Expectations
def test_polars_conditional_expectations():
    df = pl.DataFrame({
        "status": ["refunded", "active", "active"],
        "refund_amount": [10.0, None, None],
        "shipping_address": ["123 St", "456 St", None]
    })
    
    # expect_column_values_to_be_null_when
    suite = vx.ExpectationSuite("polars_test").add(
        "expect_column_values_to_be_null_when",
        column="refund_amount",
        condition_column="status",
        condition_value="active"
    )
    assert vx.validate(df, suite, engine="polars").success
    
    # expect_column_values_to_be_not_null_when
    suite = vx.ExpectationSuite("polars_test").add(
        "expect_column_values_to_be_not_null_when",
        column="refund_amount",
        condition_column="status",
        condition_value="refunded"
    )
    assert vx.validate(df, suite, engine="polars").success
    
    # expect_column_values_to_satisfy (lambda)
    df_sat = pl.DataFrame({"a": [10, 20, 30]})
    suite = vx.ExpectationSuite("polars_test").add(
        "expect_column_values_to_satisfy",
        column="a",
        condition=lambda x: x > 5 and x < 50
    )
    assert vx.validate(df_sat, suite, engine="polars").success

# 5. Format Expectations
def test_polars_format_expectations():
    df = pl.DataFrame({
        "url": ["https://google.com", "http://github.com/abc"],
        "ip": ["192.168.1.1", "10.0.0.1"],
        "uuid": ["123e4567-e89b-12d3-a456-426614174000", "987f6543-e21b-32d1-b543-123456789abc"],
        "iso": ["2026-06-15", "1999-12-31"],
        "phone": ["+1234567890", "012-345-6789"],
        "upper": ["HELLO", "WORLD"],
        "lower": ["hello", "world"]
    })
    
    suite = (
        vx.ExpectationSuite("polars_test")
        .add("expect_column_values_to_be_valid_url", column="url")
        .add("expect_column_values_to_be_valid_ip_address", column="ip")
        .add("expect_column_values_to_be_valid_uuid", column="uuid")
        .add("expect_column_values_to_be_valid_iso_date", column="iso")
        .add("expect_column_values_to_be_valid_phone_number", column="phone")
        .add("expect_column_values_to_be_all_uppercase", column="upper")
        .add("expect_column_values_to_be_all_lowercase", column="lower")
    )
    assert vx.validate(df, suite, engine="polars").success

# 6. Advanced Column Expectations
def test_polars_advanced_column_expectations():
    df = pl.DataFrame({
        "regex_col": ["abc-123", "def-456", "ghi-789"],
        "email": ["user@example.com", "admin+test@domain.co.uk", "test@gmail.com"],
        "json": ['{"name": "Alice"}', '[1, 2, 3]', '{}'],
        "nums": [10, 20, 30],
        "strs": ["a", "bb", "ccc"]
    })
    
    suite = (
        vx.ExpectationSuite("polars_test")
        .add("expect_column_values_to_not_match_regex", column="regex_col", regex=r"^\d+$")
        .add("expect_column_values_to_be_valid_email", column="email")
        .add("expect_column_values_to_be_json_parseable", column="json")
        .add("expect_column_sum_to_be_between", column="nums", min_value=50, max_value=70)
        .add("expect_column_median_to_be_between", column="nums", min_value=15, max_value=25)
        .add("expect_column_value_lengths_to_equal", column="strs", value=3)  # Fail since lengths vary
    )
    res = vx.validate(df, suite, engine="polars")
    # Assert specific expectation failures and successes
    assert not res.success
    # expect_column_value_lengths_to_equal is expected to fail
    assert res.results[-1].success is False
    assert res.results[0].success is True
    assert res.results[1].success is True
    assert res.results[2].success is True
    assert res.results[3].success is True
    assert res.results[4].success is True

# 7. Aggregate Expectations
def test_polars_aggregate_expectations():
    df = pl.DataFrame({
        "col_a": [10, 20, 30],
        "col_b": [5, 15, 25],
        "col_c": [5, 5, 5]
    })
    
    suite = (
        vx.ExpectationSuite("polars_test")
        .add("expect_column_pair_values_a_to_be_greater_than_b", column_a="col_a", column_b="col_b")
        .add("expect_column_pair_values_to_be_equal", column_a="col_b", column_b="col_b")
        .add("expect_multicolumn_sum_to_equal", column_list=["col_b", "col_c"], sum_total=20) # 5+5=10 (fail), 15+5=20 (pass) -> Fail overall
        .add("expect_compound_columns_to_be_unique", column_list=["col_a", "col_b"])
    )
    res = vx.validate(df, suite, engine="polars")
    assert not res.success
    assert res.results[0].success is True
    assert res.results[1].success is True
    assert res.results[2].success is False
    assert res.results[3].success is True

# 8. Statistical Expectations
def test_polars_statistical_expectations():
    df = pl.DataFrame({
        "a": [1.0, 2.0, 3.0, 4.0, 5.0, None],
        "b": [2.0, 4.0, 6.0, 8.0, 10.0, None],
        "spaces": ["abc", "def ", "ghi", None, None, None]
    })
    
    suite = (
        vx.ExpectationSuite("polars_test")
        .add("expect_column_quantile_values_to_be_between", column="a", quantiles=[0.25, 0.75], value_ranges=[[1.5, 2.5], [3.5, 4.5]])
        .add("expect_column_null_percentage_to_be_less_than", column="a", threshold=20.0)
        .add("expect_column_correlation_to_be_between", column="a", other_column="b", min_value=0.9, max_value=1.1)
        .add("expect_column_values_to_have_no_whitespace", column="spaces")  # Fail due to "def "
        .add("expect_column_values_to_be_positive", column="a")
        .add("expect_column_values_to_be_negative", column="a")  # Fail
        .add("expect_column_values_to_be_in_range_of_std_devs", column="a", n_std_devs=3.0)
    )
    res = vx.validate(df, suite, engine="polars")
    assert not res.success
    assert res.results[0].success is True
    assert res.results[1].success is True
    assert res.results[2].success is True
    assert res.results[3].success is False
    assert res.results[4].success is True
    assert res.results[5].success is False
    assert res.results[6].success is True

# 9. Remaining Column Expectations
def test_polars_remaining_column_expectations():
    df = pl.DataFrame({
        "in_set": ["yes", "no", "yes"],
        "not_in_set": ["red", "green", "blue"],
        "regex": ["ABC-100", "ABC-200", "ABC-300"],
        "type_col": [1, 2, 3],
        "date_col": ["2026-06-15", "June 16, 2026", "2026/06/17"],
        "lens": ["abc", "abcd", "abcde"],
        "stats": [10.0, 20.0, 30.0],
        "distinct": ["x", "y", "x"]
    })
    
    suite = (
        vx.ExpectationSuite("polars_test")
        .add("expect_column_values_to_be_in_set", column="in_set", value_set=["yes", "no"])
        .add("expect_column_values_to_not_be_in_set", column="not_in_set", value_set=["black", "white"])
        .add("expect_column_values_to_match_regex", column="regex", regex=r"^ABC-\d+$")
        .add("expect_column_values_to_be_of_type", column="type_col", expected_type="Int")
        .add("expect_column_values_to_be_dateutil_parseable", column="date_col")
        .add("expect_column_value_lengths_to_be_between", column="lens", min_value=2, max_value=6)
        .add("expect_column_max_to_be_between", column="stats", min_value=25, max_value=35)
        .add("expect_column_min_to_be_between", column="stats", min_value=5, max_value=15)
        .add("expect_column_mean_to_be_between", column="stats", min_value=15, max_value=25)
        .add("expect_column_stdev_to_be_between", column="stats", min_value=5, max_value=15)
        .add("expect_column_distinct_values_to_be_in_set", column="distinct", value_set=["x", "y", "z"])
        .add("expect_column_proportion_of_unique_values_to_be_between", column="distinct", min_value=0.5, max_value=0.8)
    )
    res = vx.validate(df, suite, engine="polars")
    assert res.success
