import pandas as pd
from validatex.core.expectation import get_expectation_class


def test_empty_pandas_dataframe():
    """Test expectations behavior on an empty pandas DataFrame."""
    df = pd.DataFrame(columns=["id", "name", "age"])

    # Not null test should pass for empty dataframe (no nulls found)
    exp = get_expectation_class("expect_column_to_not_be_null")(column="id")
    result = exp.validate(df)
    assert result.success is True
    assert result.element_count == 0
    assert result.unexpected_percent == 0.0

    # Between test should pass
    exp = get_expectation_class("expect_column_values_to_be_between")(
        column="age", kwargs={"min_value": 0, "max_value": 100}
    )
    result = exp.validate(df)
    assert result.success is True
    assert result.element_count == 0


def test_malformed_input_dataframe():
    """Test expectations behavior when the column doesn't exist or is improperly formatted."""
    df = pd.DataFrame({"id": [1, 2, 3]})

    # Missing column when running an expectation that requires it
    exp = get_expectation_class("expect_column_to_not_be_null")(column="missing_col")
    result = exp.validate(df)
    assert result.success is False


def test_all_nulls_dataframe():
    """Test handling of totally null columns."""
    df = pd.DataFrame({"age": [None, None, None]})

    exp = get_expectation_class("expect_column_to_not_be_null")(column="age")
    result = exp.validate(df)
    assert result.success is False
    assert result.unexpected_percent == 100.0

    # Range expectation on all nulls should pass (nulls are typically ignored)
    exp = get_expectation_class("expect_column_values_to_be_between")(
        column="age", kwargs={"min_value": 0, "max_value": 100}
    )
    result = exp.validate(df)
    assert result.success is True
