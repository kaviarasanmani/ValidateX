"""Direct DataFrame data source (pass an already-loaded DataFrame)."""

from __future__ import annotations

from typing import Any, Optional

from validatex.datasources.base_source import DataSource


class DataFrameSource(DataSource):
    """
    Wraps an existing Pandas or PySpark DataFrame as a DataSource.

    Parameters
    ----------
    dataframe : pd.DataFrame | pyspark.sql.DataFrame
        The DataFrame to validate.
    name : str, optional
    """

    def __init__(self, dataframe: Any, name: Optional[str] = None):
        super().__init__(name=name or "in_memory_dataframe")
        self._df = dataframe

    def load_pandas(self) -> Any:
        return self._df

    def load_spark(self, spark_session: Any = None) -> Any:
        return self._df
