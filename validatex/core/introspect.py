"""
Engine-neutral dataset introspection.

Provides the schema (column -> raw dtype string) and row count of a dataset for
every supported engine, plus a normaliser that maps raw dtype strings onto a small
set of *logical* types shared by data contracts and schema-change monitoring.
"""

from __future__ import annotations

import re
from typing import Any, Dict, Optional

LOGICAL_TYPES = ("integer", "float", "string", "boolean", "date", "datetime", "any")

# Aliases accepted in contract files, mapped to a logical type.
TYPE_ALIASES: Dict[str, str] = {
    "integer": "integer",
    "int": "integer",
    "long": "integer",
    "bigint": "integer",
    "smallint": "integer",
    "float": "float",
    "double": "float",
    "decimal": "float",
    "number": "float",
    "numeric": "float",
    "real": "float",
    "string": "string",
    "str": "string",
    "text": "string",
    "varchar": "string",
    "boolean": "boolean",
    "bool": "boolean",
    "date": "date",
    "datetime": "datetime",
    "timestamp": "datetime",
    "any": "any",
}

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$")


def canonical_type(name: Any) -> Optional[str]:
    """Map a user-supplied contract type name onto a logical type (``None`` if unknown)."""
    return TYPE_ALIASES.get(str(name).strip().lower())


def normalize_type(raw: Any) -> str:
    """
    Map a raw engine dtype string (``int64``, ``Utf8``, ``LongType()``, ``VARCHAR(20)`` ...)
    onto a logical type: integer, float, string, boolean, date, datetime, or ``unknown``.
    """
    if raw is None:
        return "unknown"
    s = str(raw).strip().lower()
    if not s or s == "unknown":
        return "unknown"
    if "interval" in s or "duration" in s:
        return "unknown"
    if "datetime" in s or "timestamp" in s:
        return "datetime"
    if "date" in s:
        return "date"
    if "bool" in s:
        return "boolean"
    if "decimal" in s or "float" in s or "double" in s or "real" in s or "numeric" in s:
        return "float"
    if "int" in s:
        return "integer"
    if any(tok in s for tok in ("str", "char", "text", "object", "category", "utf8", "clob")):
        return "string"
    return "unknown"


def is_type_compatible(expected: str, actual_logical: str) -> bool:
    """
    Whether an actual logical type satisfies a contract type.

    Widening is accepted: ``integer`` satisfies ``float``; ``date`` / ``datetime`` satisfy each other
    (Pandas has no pure date dtype). ``any`` accepts everything.
    """
    if expected == "any" or actual_logical == "unknown":
        return True
    if expected == actual_logical:
        return True
    if expected == "float" and actual_logical == "integer":
        return True
    if expected in ("date", "datetime") and actual_logical in ("date", "datetime"):
        return True
    return False


def get_schema(data: Any, engine: str) -> Dict[str, str]:
    """Return ``{column: raw dtype string}`` for *data* (best effort; ``{}`` when unavailable)."""
    try:
        if engine == "pandas":
            return {str(c): str(t) for c, t in data.dtypes.items()}
        if engine == "polars":
            return {str(c): str(t) for c, t in data.schema.items()}
        if engine == "spark":
            return {f.name: f.dataType.simpleString() for f in data.schema.fields}
        if engine == "sql" and isinstance(data, tuple) and len(data) == 2:
            return _sql_schema(data[0], data[1])
    except Exception:
        return {}
    return {}


def _sql_schema(sql_engine: Any, query_or_table: Any) -> Dict[str, str]:
    from sqlalchemy import inspect, text

    from validatex.core.utils import format_sql_from_clause

    name = str(query_or_table).strip()
    if _IDENTIFIER.match(name):
        schema_name: Optional[str] = None
        table = name
        if "." in name:
            schema_name, table = name.split(".", 1)
        try:
            cols = inspect(sql_engine).get_columns(table, schema=schema_name)
            if cols:
                return {str(c["name"]): str(c["type"]) for c in cols}
        except Exception:
            pass
    from_clause = format_sql_from_clause(query_or_table)
    with sql_engine.connect() as conn:
        res = conn.execute(text(f"SELECT * FROM {from_clause} WHERE 1=0"))  # nosec B608
        return {str(k): "unknown" for k in res.keys()}


def get_row_count(data: Any, engine: str) -> Optional[int]:
    """Return the number of rows in *data* (``None`` if it cannot be determined)."""
    try:
        if engine in ("pandas", "polars"):
            return int(len(data))
        if engine == "spark":
            return int(data.count())
        if engine == "sql" and isinstance(data, tuple) and len(data) == 2:
            from sqlalchemy import text

            from validatex.core.utils import format_sql_from_clause

            sql_engine, query_or_table = data
            from_clause = format_sql_from_clause(query_or_table)
            with sql_engine.connect() as conn:
                value = conn.execute(text(f"SELECT COUNT(*) FROM {from_clause}")).scalar()  # nosec B608
            return int(value or 0)
    except Exception:
        return None
    return None
