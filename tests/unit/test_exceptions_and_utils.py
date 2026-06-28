"""
Unit tests for validatex.core.exceptions, protocols, and utils
"""

import pytest
from validatex.core.exceptions import ValidateXEngineError, SuiteConfigurationError
from validatex.core.utils import calculate_percentage, is_between, slice_unexpected

def test_calculate_percentage():
    assert calculate_percentage(0, 0) == 0.0
    assert calculate_percentage(5, 10) == 50.0
    assert calculate_percentage(1, 3) == 33.3333

def test_is_between():
    assert is_between(10, min_value=5, max_value=15) is True
    assert is_between(5, min_value=5, max_value=15) is True
    assert is_between(5, min_value=5, max_value=15, strict=True) is False
    assert is_between(None, min_value=5) is False

def test_slice_unexpected():
    data = list(range(100))
    sliced = slice_unexpected(data, max_items=10)
    assert len(sliced) == 10
    assert sliced == list(range(10))

def test_domain_exceptions():
    orig = ValueError("Raw database error")
    err = ValidateXEngineError("Engine failed to execute SQL", original_exception=orig)
    assert str(err) == "Engine failed to execute SQL"
    assert err.original_exception is orig
