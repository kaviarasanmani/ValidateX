import pandas as pd
from validatex.expectations.sequential_expectations import (
    ExpectColumnValuesToBeIncreasing,
    ExpectColumnValuesToBeDecreasing,
)

def test_values_to_be_increasing():
    df = pd.DataFrame({"times": [1, 2, 3, 5, 8], "bad_times": [1, 5, 4, 6, 7]})
    
    # Passing
    exp = ExpectColumnValuesToBeIncreasing(column="times")
    res = exp._validate_pandas(df)
    assert res.success is True
    
    # Failing
    exp_fail = ExpectColumnValuesToBeIncreasing(column="bad_times")
    res_fail = exp_fail._validate_pandas(df)
    assert res_fail.success is False
    assert res_fail.unexpected_count == 1
    assert res_fail.unexpected_values == [4] # 5 -> 4 is the drop

def test_values_to_be_decreasing():
    df = pd.DataFrame({"countdown": [10, 9, 5, 1], "bad_countdown": [10, 9, 11, 1]})
    
    # Passing
    exp = ExpectColumnValuesToBeDecreasing(column="countdown")
    res = exp._validate_pandas(df)
    assert res.success is True
    
    # Failing
    exp_fail = ExpectColumnValuesToBeDecreasing(column="bad_countdown")
    res_fail = exp_fail._validate_pandas(df)
    assert res_fail.success is False
    assert res_fail.unexpected_count == 1
    assert res_fail.unexpected_values == [11] # 9 -> 11 is the jump
