import pytest
import polars as pl
import validatex as vx

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
