"""
Data freshness checks.

``check_freshness`` inspects the newest timestamp *inside the data*; ``check_run_freshness`` checks that a
dataset is still being validated at all (a stalled pipeline stops producing runs).

Naive timestamps are interpreted as UTC. Timezone-aware timestamps are converted to UTC.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Union

from validatex.core.exceptions import ValidateXError

_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}
_AGE_PATTERN = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*([smhdw])\s*$", re.IGNORECASE)


def parse_age(value: Union[str, int, float, timedelta]) -> timedelta:
    """Parse ``'90s'``, ``'30m'``, ``'24h'``, ``'7d'``, ``'2w'``, a number of seconds, or a ``timedelta``."""
    if isinstance(value, timedelta):
        return value
    if isinstance(value, (int, float)):
        return timedelta(seconds=float(value))
    match = _AGE_PATTERN.match(str(value))
    if not match:
        raise ValidateXError(f"Invalid age {value!r}: use a number plus s/m/h/d/w, e.g. '24h'.")
    return timedelta(seconds=float(match.group(1)) * _UNITS[match.group(2).lower()])


def _to_utc(ts: Any) -> Optional[datetime]:
    """Convert a pandas / polars / python / ISO-string timestamp into an aware UTC datetime."""
    if ts is None:
        return None
    if isinstance(ts, str):
        import dateutil.parser

        ts = dateutil.parser.parse(ts)
    if hasattr(ts, "to_pydatetime"):
        ts = ts.to_pydatetime()
    if not isinstance(ts, datetime):
        raise ValidateXError(f"Column does not hold timestamps (got {type(ts).__name__}).")
    if ts != ts:  # NaT
        return None
    return ts.replace(tzinfo=timezone.utc) if ts.tzinfo is None else ts.astimezone(timezone.utc)


@dataclass
class FreshnessResult:
    """Outcome of a freshness check."""

    fresh: bool
    column: Optional[str]
    latest_timestamp: Optional[datetime]
    age_seconds: Optional[float]
    max_age_seconds: float
    message: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "fresh": self.fresh,
            "column": self.column,
            "latest_timestamp": self.latest_timestamp.isoformat() if self.latest_timestamp else None,
            "age_seconds": self.age_seconds,
            "max_age_seconds": self.max_age_seconds,
            "message": self.message,
        }

    def __bool__(self) -> bool:
        return self.fresh


def _latest_timestamp(data: Any, column: str, engine: str) -> Any:
    if engine == "pandas":
        return data[column].max()
    if engine == "polars":
        return data[column].max()
    if engine == "spark":
        from pyspark.sql import functions as F

        return data.agg(F.max(column)).first()[0]
    if engine == "sql":
        from sqlalchemy import text

        from validatex.core.utils import format_sql_from_clause, quote_sql_identifier

        sql_engine, query_or_table = data if isinstance(data, tuple) else (None, None)
        if sql_engine is None:
            raise ValidateXError("SQL freshness checks need a (sql_engine, table_or_query) tuple.")
        query = (
            f"SELECT MAX({quote_sql_identifier(column)}) "  # nosec B608
            f"FROM {format_sql_from_clause(query_or_table)}"
        )
        with sql_engine.connect() as conn:
            return conn.execute(text(query)).scalar()
    raise ValidateXError(f"Unsupported engine for freshness check: {engine!r}")


def check_freshness(
    data: Any,
    column: str,
    max_age: Union[str, int, float, timedelta],
    engine: str = "pandas",
    now: Optional[datetime] = None,
) -> FreshnessResult:
    """
    Check that the newest value of timestamp *column* is no older than *max_age*.

    An empty dataset or an all-null column is reported as **not fresh**. Future timestamps count as fresh.
    """
    limit = parse_age(max_age)
    current = _to_utc(now) if now is not None else datetime.now(timezone.utc)
    latest = _to_utc(_latest_timestamp(data, column, engine))
    if latest is None:
        return FreshnessResult(
            False, column, None, None, limit.total_seconds(), f"No timestamps found in '{column}' (empty or all null)"
        )
    assert current is not None
    age = (current - latest).total_seconds()
    fresh = age <= limit.total_seconds()
    msg = (
        f"'{column}' latest value {latest.isoformat()} is {_humanize(age)} old "
        f"({'within' if fresh else 'exceeds'} limit of {_humanize(limit.total_seconds())})"
    )
    return FreshnessResult(fresh, column, latest, age, limit.total_seconds(), msg)


def check_run_freshness(
    store: Any,
    dataset_name: str,
    max_age: Union[str, int, float, timedelta],
    now: Optional[datetime] = None,
) -> FreshnessResult:
    """Check that *dataset_name* was validated within *max_age* (detects stalled pipelines)."""
    limit = parse_age(max_age)
    run = store.latest_run(dataset_name=dataset_name)
    if run is None:
        return FreshnessResult(
            False, None, None, None, limit.total_seconds(), f"No validation runs recorded for '{dataset_name}'"
        )
    current = _to_utc(now) if now is not None else datetime.now(timezone.utc)
    # Run timestamps are stored as local naive ISO strings; astimezone() interprets naive values as local time.
    ran_utc = datetime.fromisoformat(run["timestamp"]).astimezone(timezone.utc)
    assert current is not None
    age = (current - ran_utc).total_seconds()
    fresh = age <= limit.total_seconds()
    msg = f"Last validation of '{dataset_name}' was {_humanize(age)} ago ({'within' if fresh else 'exceeds'} limit)"
    return FreshnessResult(fresh, None, ran_utc, age, limit.total_seconds(), msg)


def _humanize(seconds: float) -> str:
    s = abs(seconds)
    for unit, size in (("d", 86400), ("h", 3600), ("m", 60)):
        if s >= size:
            return f"{s / size:.1f}{unit}"
    return f"{s:.0f}s"
