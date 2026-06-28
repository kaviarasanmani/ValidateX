"""
ValidateX Core Utilities — Centralized Math & Value Operations.

Eliminates code duplication across expectation implementations by consolidating
boundary checks, percentage calculations, and unexpected value slicing.
"""

from __future__ import annotations

from typing import Any, List, Sequence


def calculate_percentage(count: int, total: int) -> float:
    """Safely calculate percentage (0.0 to 100.0) avoiding ZeroDivisionError."""
    if total <= 0:
        return 0.0
    return round((count / total) * 100.0, 4)


def is_between(value: Any, min_value: Any = None, max_value: Any = None, strict: bool = False) -> bool:
    """Check if a scalar value is between min_value and max_value."""
    if value is None:
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
