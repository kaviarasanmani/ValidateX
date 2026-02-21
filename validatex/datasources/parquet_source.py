"""Parquet data source."""

from __future__ import annotations

from typing import Any, Dict, Optional

import pandas as pd

from validatex.datasources.base_source import DataSource


class ParquetDataSource(DataSource):
    """
    Load data from a Parquet file.

    Parameters
    ----------
    filepath : str
        Path to the Parquet file or directory.
    read_options : dict, optional
        Extra kwargs forwarded to ``pd.read_parquet`` / Spark reader.
    name : str, optional
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
        return pd.read_parquet(self.filepath, **self.read_options)

    def load_spark(self, spark_session: Any = None) -> Any:
        if spark_session is None:
            raise ValueError("A SparkSession is required.")
        return spark_session.read.parquet(self.filepath)
