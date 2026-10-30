"""
ValidateX Core Utilities — Centralized Math & Value Operations.

Eliminates code duplication across expectation implementations by consolidating
boundary checks, percentage calculations, and unexpected value slicing.
"""

from __future__ import annotations

import math
import re
from typing import Any, List, Sequence
from urllib.parse import urlparse

_SAFE_SQL_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*(\.[A-Za-z_][A-Za-z0-9_$]*)*$")
_FORBIDDEN_SQL_TOKENS_RE = re.compile(
    r"(;|--|/\*|\*/|\b(DROP|DELETE|UPDATE|INSERT|ALTER|TRUNCATE|GRANT|REVOKE|EXEC|EXECUTE|ATTACH|DETACH|PRAGMA)\b)",
    re.IGNORECASE,
)


def calculate_percentage(count: int, total: int) -> float:
    """Safely calculate percentage (0.0 to 100.0) avoiding ZeroDivisionError."""
    if total <= 0:
        return 0.0
    return round((count / total) * 100.0, 4)


def is_between(value: Any, min_value: Any = None, max_value: Any = None, strict: bool = False) -> bool:
    """Check if a scalar value is between min_value and max_value."""
    if value is None:
        return False
    if isinstance(value, float) and math.isnan(value):
        return False
    if min_value is not None:
        if strict and value <= min_value:
            return False
        if not strict and value < min_value:
            return False
    if max_value is not None:
        if strict and value >= max_value:
            return False
        if not strict and value > max_value:
            return False
    return True


def slice_unexpected(values: Sequence[Any], max_items: int = 20) -> List[Any]:
    """Slice unexpected values to max_items to prevent memory bottlenecks."""
    if not values:
        return []
    return list(values[:max_items])


def quote_sql_identifier(identifier: Any) -> str:
    """
    Validate and double-quote a SQL identifier (column or table name) to prevent SQL injection.

    Raises ValueError if the identifier contains unsafe characters or SQL metacharacters.
    """
    ident_str = str(identifier or "").strip()
    if not ident_str or not _SAFE_SQL_IDENTIFIER_RE.match(ident_str) or _FORBIDDEN_SQL_TOKENS_RE.search(ident_str):
        raise ValueError(f"Unsafe or invalid SQL identifier: {identifier!r}")
    parts = ident_str.split(".")
    return ".".join(f'"{p}"' for p in parts)


def format_sql_from_clause(query_or_table: Any) -> str:
    """
    Validate a table name or read-only SELECT query and return a safe SQL subquery expression.

    Raises ValueError if SQL injection patterns, multi-statement separators, or DDL/DML keywords are detected.
    """
    raw = str(query_or_table or "").strip()
    if not raw or "\x00" in raw or _FORBIDDEN_SQL_TOKENS_RE.search(raw):
        raise ValueError(f"Unsafe SQL table or query expression rejected: {query_or_table!r}")

    if _SAFE_SQL_IDENTIFIER_RE.match(raw):
        quoted_table = quote_sql_identifier(raw)
        return f"(SELECT * FROM {quoted_table}) AS subquery"

    normalized = raw.lstrip("(").strip()
    if not normalized.upper().startswith(("SELECT ", "WITH ")):
        raise ValueError(
            f"Unsafe SQL target {query_or_table!r}: must be a valid table identifier or read-only SELECT query."
        )
    return f"({raw}) AS subquery"


def validate_webhook_url(url: str) -> str:
    """Validate that a webhook URL uses HTTP/HTTPS and has a valid network host."""
    parsed = urlparse(str(url or "").strip())
    if parsed.scheme not in ("https", "http") or not parsed.netloc:
        raise ValueError(f"Invalid or unsafe webhook URL scheme (must be https:// or http://): {url!r}")
    return url.strip()
