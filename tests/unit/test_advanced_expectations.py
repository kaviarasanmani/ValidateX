import pandas as pd
import pytest

from validatex.expectations.advanced_column_expectations import (
    ExpectColumnValuesToNotMatchRegex,
    ExpectColumnValuesToBeValidEmail,
    ExpectColumnValuesToBeJsonParseable,
    ExpectColumnSumToBeBetween,
    ExpectColumnMedianToBeBetween,
    ExpectColumnValueLengthsToEqual,
)

def test_not_match_regex():
    df = pd.DataFrame({"codes": ["A123", "B456", "C-FAIL"]})
    
    # We want them to NOT match this failure regex
    exp = ExpectColumnValuesToNotMatchRegex(column="codes", regex=r"FAIL")
    res = exp._validate_pandas(df)
    assert res.success is False
    assert res.unexpected_count == 1
    assert res.unexpected_values == ["C-FAIL"]

def test_valid_email():
    df = pd.DataFrame({"email": ["valid@test.com", "invalid-email", "also_ok@domain.co.uk"]})
    exp = ExpectColumnValuesToBeValidEmail(column="email")
    res = exp._validate_pandas(df)
    assert res.success is False
    assert res.unexpected_count == 1
    assert res.unexpected_values == ["invalid-email"]

def test_json_parseable():
    df = pd.DataFrame({"config": ['{"key": "value"}', '[1, 2, 3]', 'bad json :(']})
    exp = ExpectColumnValuesToBeJsonParseable(column="config")
    res = exp._validate_pandas(df)
    assert res.success is False
    assert res.unexpected_count == 1
    assert res.unexpected_values == ["bad json :("]

def test_sum_to_be_between():
    df = pd.DataFrame({"sales": [10.5, 20.0, 5.0]}) # Sum is 35.5
    
    # Passing
    exp_pass = ExpectColumnSumToBeBetween(column="sales", min_value=30.0, max_value=40.0)
    res_pass = exp_pass._validate_pandas(df)
    assert res_pass.success is True
    assert res_pass.observed_value == 35.5
    
    # Failing
    exp_fail = ExpectColumnSumToBeBetween(column="sales", min_value=40.0)
    res_fail = exp_fail._validate_pandas(df)
    assert res_fail.success is False

def test_median_to_be_between():
    df = pd.DataFrame({"prices": [10, 20, 100]}) # Median is 20
    
    # Passing
    exp_pass = ExpectColumnMedianToBeBetween(column="prices", min_value=15, max_value=25)
    res_pass = exp_pass._validate_pandas(df)
    assert res_pass.success is True
    assert res_pass.observed_value == 20
    
    # Failing
    exp_fail = ExpectColumnMedianToBeBetween(column="prices", max_value=15)
    res_fail = exp_fail._validate_pandas(df)
    assert res_fail.success is False

def test_value_lengths_to_equal():
    df = pd.DataFrame({"zip_code": ["12345", "67890", "1234"]}) # One has length 4
    exp = ExpectColumnValueLengthsToEqual(column="zip_code", value=5)
    res = exp._validate_pandas(df)
    
    assert res.success is False
    assert res.unexpected_count == 1
    assert res.unexpected_values == ["1234"]
