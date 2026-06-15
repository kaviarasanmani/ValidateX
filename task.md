# Tasks: Polars Engine Support

- [x] Polars Engine Support
  - [x] Install `polars` library in test virtual environment
  - [x] Modify base `Expectation` in `validatex/core/expectation.py`
  - [x] Modify `Validator` in `validatex/core/validator.py`
  - [x] Update `DataSource` in `validatex/datasources/base_source.py`
  - [x] Update `CSVDataSource` in `validatex/datasources/csv_source.py`
  - [x] Update `ParquetDataSource` in `validatex/datasources/parquet_source.py`
  - [x] Update CLI in `validatex/cli/main.py`
  - [x] Implement Polars validation for core expectations in `validatex/expectations/column_expectations.py`
  - [x] Add unit tests in `tests/unit/test_polars_expectations.py`
  - [x] Run and verify tests
