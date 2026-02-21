import pytest

# Attempt to import pyspark. Skip tests if omitted.
try:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, IntegerType, StringType
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False

from validatex.core.expectation import get_expectation_class

@pytest.fixture(scope="module")
def spark():
    if not SPARK_AVAILABLE:
        pytest.skip("PySpark is not installed")
    return SparkSession.builder.master("local[1]").appName("validatex-test").getOrCreate()

@pytest.fixture
def sample_spark_df(spark):
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True)
    ])
    data = [
        (1, "Alice", 25),
        (2, "Bob", 30),
        (3, "Charlie", 35),
        (4, "Diana", 28),
        (5, "Eve", 42),
    ]
    return spark.createDataFrame(data, schema)

@pytest.fixture
def spark_df_with_nulls(spark):
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True)
    ])
    data = [
        (1, "Alice"),
        (2, None),
        (3, "Charlie"),
        (None, "Diana"),
        (5, None),
    ]
    return spark.createDataFrame(data, schema)


def test_spark_expect_column_to_exist(sample_spark_df):
    exp = get_expectation_class("expect_column_to_exist")(column="id")
    result = exp.validate(sample_spark_df)
    assert result.success is True

def test_spark_expect_column_to_not_be_null_pass(sample_spark_df):
    exp = get_expectation_class("expect_column_to_not_be_null")(column="id")
    result = exp.validate(sample_spark_df)
    assert result.success is True
    assert result.unexpected_count == 0

def test_spark_expect_column_to_not_be_null_fail(spark_df_with_nulls):
    exp = get_expectation_class("expect_column_to_not_be_null")(column="name")
    result = exp.validate(spark_df_with_nulls)
    assert result.success is False
    assert result.unexpected_count == 2

def test_spark_expect_column_values_to_be_between(sample_spark_df):
    exp = get_expectation_class("expect_column_values_to_be_between")(
        column="age", kwargs={"min_value": 0, "max_value": 100}
    )
    result = exp.validate(sample_spark_df)
    assert result.success is True

def test_spark_expect_table_row_count_to_equal(sample_spark_df):
    exp = get_expectation_class("expect_table_row_count_to_equal")(kwargs={"value": 5})
    result = exp.validate(sample_spark_df)
    assert result.success is True

def test_spark_expect_table_columns_to_match_set(sample_spark_df):
    exp = get_expectation_class("expect_table_columns_to_match_set")(
        kwargs={"column_set": ["id", "name", "age"], "exact_match": True}
    )
    result = exp.validate(sample_spark_df)
    assert result.success is True
