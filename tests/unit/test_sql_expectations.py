from typing import Any

import pytest
from sqlalchemy import create_engine, text

from validatex.core.result import ExpectationResult
from validatex.expectations.column_expectations import (
    ExpectColumnToExist,
    ExpectColumnToNotBeNull,
    ExpectColumnValuesToBeUnique,
)
from validatex.expectations.table_expectations import (
    ExpectTableRowCountToBeBetween,
    ExpectTableRowCountToEqual,
)


@pytest.fixture
def sqlite_engine():
    engine = create_engine("sqlite:///:memory:")
    with engine.connect() as conn:
        conn.execute(text("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)"))
        conn.execute(
            text(
                """
            INSERT INTO users (id, name, age) VALUES
            (1, 'Alice', 25),
            (2, 'Bob', 30),
            (3, 'Charlie', 22),
            (4, NULL, 40)
        """
            )
        )
        conn.commit()
    return engine, "users"


def test_sql_row_count(sqlite_engine):
    exp = ExpectTableRowCountToEqual(value=4)
    res = exp._validate_sql(sqlite_engine)
    assert res.success is True
    assert res.observed_value == 4

    exp_between = ExpectTableRowCountToBeBetween(min_value=2, max_value=5)
    res_between = exp_between._validate_sql(sqlite_engine)
    assert res_between.success is True


def test_sql_column_to_exist(sqlite_engine):
    exp = ExpectColumnToExist(column="age")
    res = exp._validate_sql(sqlite_engine)
    assert res.success is True

    exp_fail = ExpectColumnToExist(column="invalid_col")
    res_fail = exp_fail._validate_sql(sqlite_engine)
    assert res_fail.success is False


def test_sql_column_not_null(sqlite_engine):
    exp = ExpectColumnToNotBeNull(column="id")
    res = exp._validate_sql(sqlite_engine)
    assert res.success is True

    exp_fail = ExpectColumnToNotBeNull(column="name")
    res_fail = exp_fail._validate_sql(sqlite_engine)
    assert res_fail.success is False
    assert res_fail.unexpected_count == 1


def test_sql_column_unique(sqlite_engine):
    exp = ExpectColumnValuesToBeUnique(column="id")
    res = exp._validate_sql(sqlite_engine)
    assert res.success is True

    # Make duplicate
    engine, table = sqlite_engine
    with engine.connect() as conn:
        conn.execute(text("INSERT INTO users (id, name, age) VALUES (1, 'Eve', 25)"))
        conn.commit()

    res_fail = exp._validate_sql(sqlite_engine)
    assert res_fail.success is False
    assert "4 unique values out of 5" in str(res_fail.observed_value)
