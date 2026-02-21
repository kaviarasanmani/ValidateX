"""
Config loader — read YAML / JSON checkpoint files.

A *checkpoint* file ties together a data source and an expectation suite
so that validations can be run declaratively from the CLI.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

from validatex.core.suite import ExpectationSuite


@dataclass
class CheckpointConfig:
    """
    Represents a checkpoint configuration.

    Attributes
    ----------
    name : str
        Checkpoint name.
    suite_path : str
        Path to the expectation suite YAML/JSON file.
    data_source : dict
        Data source configuration (type, path, query, etc.).
    engine : str
        Engine to use: ``"pandas"`` or ``"spark"``.
    report : dict
        Report configuration (format, output_path).
    """

    name: str = "default_checkpoint"
    suite_path: str = ""
    data_source: Dict[str, Any] = field(default_factory=dict)
    engine: str = "pandas"
    report: Dict[str, Any] = field(default_factory=dict)

    def load_suite(self) -> ExpectationSuite:
        """Load the expectation suite from the configured path."""
        # Ensure expectations are registered
        import validatex.expectations  # noqa: F401
        return ExpectationSuite.load(self.suite_path)

    def load_data(self, spark_session: Any = None) -> Any:
        """Load data based on the data source configuration."""
        ds_type = self.data_source.get("type", "csv")
        path = self.data_source.get("path", "")
        query = self.data_source.get("query", "")
        connection = self.data_source.get("connection_string", "")

        if ds_type == "csv":
            from validatex.datasources.csv_source import CSVDataSource
            source = CSVDataSource(filepath=path)
        elif ds_type == "parquet":
            from validatex.datasources.parquet_source import ParquetDataSource
            source = ParquetDataSource(filepath=path)
        elif ds_type == "database":
            from validatex.datasources.database_source import DatabaseDataSource
            source = DatabaseDataSource(connection_string=connection, query=query)
        else:
            raise ValueError(f"Unsupported data source type: {ds_type}")

        return source.load(engine=self.engine, spark_session=spark_session)


def load_checkpoint(filepath: str) -> CheckpointConfig:
    """
    Load a checkpoint configuration from a YAML or JSON file.

    Parameters
    ----------
    filepath : str
        Path to the checkpoint config file.

    Returns
    -------
    CheckpointConfig
    """
    with open(filepath, "r", encoding="utf-8") as f:
        if filepath.endswith((".yaml", ".yml")):
            data = yaml.safe_load(f)
        else:
            import json
            data = json.load(f)

    return CheckpointConfig(
        name=data.get("name", "default_checkpoint"),
        suite_path=data.get("suite_path", ""),
        data_source=data.get("data_source", {}),
        engine=data.get("engine", "pandas"),
        report=data.get("report", {}),
    )
