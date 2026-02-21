"""CSV data source."""

from __future__ import annotations

from typing import Any, Dict, Optional

import pandas as pd

from validatex.datasources.base_source import DataSource


class CSVDataSource(DataSource):
    """
    Load data from a CSV file.

    Parameters
    ----------
    filepath : str
        Path to the CSV file.
    read_options : dict, optional
        Extra keyword arguments forwarded to ``pd.read_csv`` / Spark reader.
    name : str, optional
        A label for this source.
    """

    def __init__(
        self,
        filepath: str,
        read_options: Optional[Dict[str, Any]] = None,
        name: Optional[str] = None,
    ):
        super().__init__(name=name or filepath)
        self.filepath = filepath
        self.read_options = read_options or {}

    def load_pandas(self) -> pd.DataFrame:
        return pd.read_csv(self.filepath, **self.read_options)

    def load_spark(self, spark_session: Any = None) -> Any:
        if spark_session is None:
            raise ValueError(
                "A SparkSession is required to load CSV as Spark DataFrame."
            )
        reader = spark_session.read.option("header", "true").option(
            "inferSchema", "true"
        )
        for k, v in self.read_options.items():
            reader = reader.option(k, v)
        return reader.csv(self.filepath)
