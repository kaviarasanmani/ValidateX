"""
Validator — orchestrates expectation suite execution against a dataset.

The :func:`validate` convenience function is the primary public entry point.
"""

from __future__ import annotations

import time
from typing import Any, Optional

from validatex.core.exceptions import ValidateXEngineError
from validatex.core.result import ValidationResult
from validatex.core.suite import ExpectationSuite

SUPPORTED_ENGINES = {"pandas", "spark", "polars", "sql"}


def _detect_total_columns(data: Any, engine: str) -> Optional[int]:
    """Best-effort detection of total dataset columns for coverage reporting."""
    try:
        if engine in ("pandas", "polars", "spark") and hasattr(data, "columns"):
            return len(data.columns)
        if engine == "sql" and isinstance(data, tuple) and len(data) == 2:
            from sqlalchemy import text
            from validatex.core.utils import format_sql_from_clause

            sql_eng, query_or_table = data
            from_clause = format_sql_from_clause(query_or_table)
            with sql_eng.connect() as conn:
                res = conn.execute(text(f"SELECT * FROM {from_clause} LIMIT 1"))
                return len(list(res.keys()))
    except Exception:
        return None
    return None


class Validator:
    """
    Runs an :class:`ExpectationSuite` against a dataset.

    Parameters
    ----------
    suite : ExpectationSuite
        The suite of expectations to evaluate.
    engine : str
        ``"pandas"``, ``"spark"``, ``"polars"``, or ``"sql"``.
    sql_engine : Any, optional
        SQLAlchemy engine instance when ``engine="sql"`` and *data* is a table or query string.
    """

    def __init__(self, suite: ExpectationSuite, engine: str = "pandas", sql_engine: Optional[Any] = None):
        self.suite = suite
        norm_engine = engine.lower().strip()
        if norm_engine == "pyspark":
            norm_engine = "spark"
        if norm_engine not in SUPPORTED_ENGINES:
            raise ValidateXEngineError(
                f"Unsupported engine '{engine}'. Supported engines: {', '.join(sorted(SUPPORTED_ENGINES))}."
            )
        self.engine = norm_engine
        self.sql_engine = sql_engine

    def run(
        self,
        data: Any,
        data_source: Optional[str] = None,
        sql_engine: Optional[Any] = None,
    ) -> ValidationResult:
        """
        Execute every expectation in the suite against *data*.

        Parameters
        ----------
        data : pd.DataFrame | pl.DataFrame | pyspark.sql.DataFrame | tuple | str
            The dataset to validate.
        data_source : str, optional
            A label describing where the data came from.
        sql_engine : Any, optional
            SQLAlchemy engine instance when ``engine="sql"``.

        Returns
        -------
        ValidationResult
        """
        effective_sql_engine = sql_engine or self.sql_engine
        target_data = data
        if self.engine == "sql":
            if isinstance(data, tuple) and len(data) == 2:
                target_data = data
            elif effective_sql_engine is not None:
                target_data = (effective_sql_engine, data)
            else:
                raise ValidateXEngineError(
                    "SQL validation requires either a (sql_engine, table_or_query) tuple as 'data' "
                    "or passing 'sql_engine=<SQLAlchemy Engine>'."
                )

        total_cols = _detect_total_columns(target_data, self.engine)

        result = ValidationResult(
            suite_name=self.suite.name,
            data_source=data_source,
            engine=self.engine,
            total_columns=total_cols,
            suite_meta={
                "version": getattr(self.suite, "version", "1.0.0"),
                "owner": getattr(self.suite, "owner", None),
                **(self.suite.meta or {}),
            },
        )

        start = time.perf_counter()
        for expectation in self.suite:
            exp_result = expectation.validate(target_data, engine=self.engine)
            result.results.append(exp_result)
        result.run_duration_seconds = time.perf_counter() - start
        result.compute_statistics()
        return result


def validate(
    data: Any,
    suite: ExpectationSuite,
    engine: str = "pandas",
    data_source: Optional[str] = None,
    sql_engine: Optional[Any] = None,
) -> ValidationResult:
    """
    Convenience function to validate *data* against a *suite*.

    Parameters
    ----------
    data : pd.DataFrame | pl.DataFrame | pyspark.sql.DataFrame | tuple | str
    suite : ExpectationSuite
    engine : str
        ``"pandas"``, ``"spark"``, ``"polars"``, or ``"sql"``.
    data_source : str, optional
    sql_engine : Any, optional
        SQLAlchemy engine connection when ``engine="sql"``.

    Returns
    -------
    ValidationResult
    """
    return Validator(suite, engine=engine, sql_engine=sql_engine).run(
        data, data_source=data_source, sql_engine=sql_engine
    )
