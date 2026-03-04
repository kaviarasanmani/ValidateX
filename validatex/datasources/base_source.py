"""Abstract base class for data sources."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Optional


class DataSource(ABC):
    """
    Base class for all data sources.

    A DataSource knows how to load data into either a Pandas or PySpark
    DataFrame depending on the requested engine.
    """

    def __init__(self, name: Optional[str] = None):
        self.name = name or self.__class__.__name__

    @abstractmethod
    def load_pandas(self) -> Any:
        """Load data as a Pandas DataFrame."""
        ...

    def load_spark(self, spark_session: Any = None) -> Any:
        """Load data as a PySpark DataFrame. Override in subclass."""
        raise NotImplementedError(f"{self.__class__.__name__} does not support PySpark loading.")

    def load_sql(self) -> Any:
        """Load data engine as a SQLAlchemy engine. Override in subclass."""
        raise NotImplementedError(f"{self.__class__.__name__} does not support SQL loading.")

    def load(self, engine: str = "pandas", spark_session: Any = None) -> Any:
        """Load data using the specified engine."""
        if engine == "pandas":
            return self.load_pandas()
        elif engine == "spark":
            return self.load_spark(spark_session)
        elif engine == "sql":
            return self.load_sql()
        raise ValueError(f"Unsupported engine: {engine}")

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name!r})"
