"""Database (SQL) data source using SQLAlchemy."""

from __future__ import annotations

from typing import Any, Dict, Optional

import pandas as pd

from validatex.datasources.base_source import DataSource


class DatabaseDataSource(DataSource):
    """
    Load data from a SQL database.

    Parameters
    ----------
    connection_string : str
        SQLAlchemy connection string, e.g.
        ``"postgresql://user:pass@host/db"`` or ``"sqlite:///data.db"``.
    query : str
        SQL query to execute.
    name : str, optional
    """

    def __init__(
        self,
        connection_string: str,
        query: str,
        name: Optional[str] = None,
    ):
        super().__init__(name=name or "database")
        self.connection_string = connection_string
        self.query = query

    def load_pandas(self) -> pd.DataFrame:
        from sqlalchemy import create_engine

        engine = create_engine(self.connection_string)
        return pd.read_sql(self.query, engine)

    def load_spark(self, spark_session: Any = None) -> Any:
        """Load via Spark JDBC. Requires a JDBC driver on the classpath."""
        if spark_session is None:
            raise ValueError("A SparkSession is required.")
        raise NotImplementedError(
            "Spark JDBC loading requires additional JDBC driver configuration. "
            "Please load the DataFrame manually and use DataFrameSource instead."
        )
