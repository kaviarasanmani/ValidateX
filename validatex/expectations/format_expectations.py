"""
ValidateX — String & Format Expectations.

URL, IP address, phone number,
credit card masking, UUID and ISO date validation.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from validatex.core.expectation import Expectation, register_expectation
from validatex.core.result import ExpectationResult

# Pre-compiled patterns
_URL_PATTERN = re.compile(
    r"^(https?|ftp)://[^\s/$.?#].[^\s]*$", re.IGNORECASE
)
_IP4_PATTERN = re.compile(
    r"^((25[0-5]|2[0-4]\d|[01]?\d\d?)\.){3}(25[0-5]|2[0-4]\d|[01]?\d\d?)$"
)
_IP6_PATTERN = re.compile(
    r"^([0-9a-f]{1,4}:){7}[0-9a-f]{1,4}$", re.IGNORECASE
)
_UUID_PATTERN = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)
_ISO_DATE_PATTERN = re.compile(r"^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$")
_PHONE_PATTERN = re.compile(r"^\+?[\d\s\-\(\)]{7,15}$")


def _vectorized_match(series: pd.Series, pattern: re.Pattern) -> pd.Series:
    return series.apply(lambda x: bool(pattern.match(str(x))))


def _build_unexpected(series, unexpected_mask):
    unexpected_count = int(unexpected_mask.sum())
    total = len(series)
    pct = (unexpected_count / total * 100) if total > 0 else 0.0
    unexpected_values = series[unexpected_mask].tolist()[:20]
    return total, unexpected_count, pct, unexpected_values


# ---------------------------------------------------------------------------
# 1. expect_column_values_to_be_valid_url
# ---------------------------------------------------------------------------

@register_expectation
@dataclass
class ExpectColumnValuesToBeValidUrl(Expectation):
    """Expect column values to be valid HTTP/HTTPS/FTP URLs."""
    expectation_type: str = field(
        init=False, default="expect_column_values_to_be_valid_url"
    )

    def _validate_pandas(self, df: pd.DataFrame) -> ExpectationResult:
        series = df[self.column].dropna().astype(str)
        unexpected_mask = ~_vectorized_match(series, _URL_PATTERN)
        total, cnt, pct, vals = _build_unexpected(series, unexpected_mask)
        return self._build_result(success=(cnt == 0), element_count=total,
                                  unexpected_count=cnt, unexpected_percent=pct,
                                  unexpected_values=vals)

    def _validate_spark(self, df: Any) -> ExpectationResult:
        from pyspark.sql import functions as F
        col = F.col(str(self.column))
        filtered = df.filter(col.isNotNull())
        total = filtered.count()
        pattern = r"^(https?|ftp)://[^\s/$.?#].[^\s]*$"
        unexpected_df = filtered.filter(~col.rlike(pattern))
        cnt = unexpected_df.count()
        pct = (cnt / total * 100) if total > 0 else 0.0
        vals = [r[0] for r in unexpected_df.select(self.column).limit(20).collect()]
        return self._build_result(success=(cnt == 0), element_count=total,
                                  unexpected_count=cnt, unexpected_percent=pct,
                                  unexpected_values=vals)


# ---------------------------------------------------------------------------
# 2. expect_column_values_to_be_valid_ip_address
# ---------------------------------------------------------------------------

@register_expectation
@dataclass
class ExpectColumnValuesToBeValidIpAddress(Expectation):
    """Expect column values to be valid IPv4 or IPv6 addresses."""
    expectation_type: str = field(
        init=False, default="expect_column_values_to_be_valid_ip_address"
    )

    def _validate_pandas(self, df: pd.DataFrame) -> ExpectationResult:
        series = df[self.column].dropna().astype(str)
        matches = series.apply(
            lambda x: bool(_IP4_PATTERN.match(x)) or bool(_IP6_PATTERN.match(x))
        )
        unexpected_mask = ~matches
        total, cnt, pct, vals = _build_unexpected(series, unexpected_mask)
        return self._build_result(success=(cnt == 0), element_count=total,
                                  unexpected_count=cnt, unexpected_percent=pct,
                                  unexpected_values=vals)

    def _validate_spark(self, df: Any) -> ExpectationResult:
        from pyspark.sql import functions as F
        col = F.col(str(self.column))
        filtered = df.filter(col.isNotNull())
        total = filtered.count()
        # Basic IPv4
        ipv4 = r"^((25[0-5]|2[0-4]\d|[01]?\d\d?)\.){3}(25[0-5]|2[0-4]\d|[01]?\d\d?)$"
        unexpected_df = filtered.filter(~col.rlike(ipv4))
        cnt = unexpected_df.count()
        pct = (cnt / total * 100) if total > 0 else 0.0
        vals = [r[0] for r in unexpected_df.select(self.column).limit(20).collect()]
        return self._build_result(success=(cnt == 0), element_count=total,
                                  unexpected_count=cnt, unexpected_percent=pct,
                                  unexpected_values=vals)


# ---------------------------------------------------------------------------
# 3. expect_column_values_to_be_valid_uuid
# ---------------------------------------------------------------------------

@register_expectation
@dataclass
class ExpectColumnValuesToBeValidUuid(Expectation):
    """Expect column values to be valid UUIDs (any version)."""
    expectation_type: str = field(
        init=False, default="expect_column_values_to_be_valid_uuid"
    )

    def _validate_pandas(self, df: pd.DataFrame) -> ExpectationResult:
        series = df[self.column].dropna().astype(str)
        unexpected_mask = ~_vectorized_match(series, _UUID_PATTERN)
        total, cnt, pct, vals = _build_unexpected(series, unexpected_mask)
        return self._build_result(success=(cnt == 0), element_count=total,
                                  unexpected_count=cnt, unexpected_percent=pct,
                                  unexpected_values=vals)

    def _validate_spark(self, df: Any) -> ExpectationResult:
        from pyspark.sql import functions as F
        col = F.col(str(self.column))
        filtered = df.filter(col.isNotNull())
        total = filtered.count()
        pattern = r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
        unexpected_df = filtered.filter(~col.rlike(pattern))
        cnt = unexpected_df.count()
        pct = (cnt / total * 100) if total > 0 else 0.0
        vals = [r[0] for r in unexpected_df.select(self.column).limit(20).collect()]
        return self._build_result(success=(cnt == 0), element_count=total,
                                  unexpected_count=cnt, unexpected_percent=pct,
                                  unexpected_values=vals)


# ---------------------------------------------------------------------------
# 4. expect_column_values_to_be_valid_iso_date
# ---------------------------------------------------------------------------

@register_expectation
@dataclass
class ExpectColumnValuesToBeValidIsoDate(Expectation):
    """Expect column values to be valid ISO 8601 date strings (YYYY-MM-DD)."""
    expectation_type: str = field(
        init=False, default="expect_column_values_to_be_valid_iso_date"
    )

    def _validate_pandas(self, df: pd.DataFrame) -> ExpectationResult:
        series = df[self.column].dropna().astype(str)
        unexpected_mask = ~_vectorized_match(series, _ISO_DATE_PATTERN)
        total, cnt, pct, vals = _build_unexpected(series, unexpected_mask)
        return self._build_result(success=(cnt == 0), element_count=total,
                                  unexpected_count=cnt, unexpected_percent=pct,
                                  unexpected_values=vals)

    def _validate_spark(self, df: Any) -> ExpectationResult:
        from pyspark.sql import functions as F
        col = F.col(str(self.column))
        filtered = df.filter(col.isNotNull())
        total = filtered.count()
        pattern = r"^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$"
        unexpected_df = filtered.filter(~col.rlike(pattern))
        cnt = unexpected_df.count()
        pct = (cnt / total * 100) if total > 0 else 0.0
        vals = [r[0] for r in unexpected_df.select(self.column).limit(20).collect()]
        return self._build_result(success=(cnt == 0), element_count=total,
                                  unexpected_count=cnt, unexpected_percent=pct,
                                  unexpected_values=vals)


# ---------------------------------------------------------------------------
# 5. expect_column_values_to_be_valid_phone_number
# ---------------------------------------------------------------------------

@register_expectation
@dataclass
class ExpectColumnValuesToBeValidPhoneNumber(Expectation):
    """Expect column values to be valid international phone numbers."""
    expectation_type: str = field(
        init=False, default="expect_column_values_to_be_valid_phone_number"
    )

    def _validate_pandas(self, df: pd.DataFrame) -> ExpectationResult:
        series = df[self.column].dropna().astype(str)
        unexpected_mask = ~_vectorized_match(series, _PHONE_PATTERN)
        total, cnt, pct, vals = _build_unexpected(series, unexpected_mask)
        return self._build_result(success=(cnt == 0), element_count=total,
                                  unexpected_count=cnt, unexpected_percent=pct,
                                  unexpected_values=vals)

    def _validate_spark(self, df: Any) -> ExpectationResult:
        from pyspark.sql import functions as F
        col = F.col(str(self.column))
        filtered = df.filter(col.isNotNull())
        total = filtered.count()
        pattern = r"^\+?[\d\s\-\(\)]{7,15}$"
        unexpected_df = filtered.filter(~col.rlike(pattern))
        cnt = unexpected_df.count()
        pct = (cnt / total * 100) if total > 0 else 0.0
        vals = [r[0] for r in unexpected_df.select(self.column).limit(20).collect()]
        return self._build_result(success=(cnt == 0), element_count=total,
                                  unexpected_count=cnt, unexpected_percent=pct,
                                  unexpected_values=vals)


# ---------------------------------------------------------------------------
# 6. expect_column_values_to_be_all_uppercase
# ---------------------------------------------------------------------------

@register_expectation
@dataclass
class ExpectColumnValuesToBeAllUppercase(Expectation):
    """Expect all string values in a column to be fully uppercased."""
    expectation_type: str = field(
        init=False, default="expect_column_values_to_be_all_uppercase"
    )

    def _validate_pandas(self, df: pd.DataFrame) -> ExpectationResult:
        series = df[self.column].dropna().astype(str)
        total = len(series)
        unexpected_mask = series != series.str.upper()
        unexpected_count = int(unexpected_mask.sum())
        pct = (unexpected_count / total * 100) if total > 0 else 0.0
        return self._build_result(success=(unexpected_count == 0), element_count=total,
                                  unexpected_count=unexpected_count, unexpected_percent=pct,
                                  unexpected_values=series[unexpected_mask].tolist()[:20])

    def _validate_spark(self, df: Any) -> ExpectationResult:
        from pyspark.sql import functions as F
        col = F.col(str(self.column))
        filtered = df.filter(col.isNotNull())
        total = filtered.count()
        unexpected_df = filtered.filter(col != F.upper(col))
        cnt = unexpected_df.count()
        pct = (cnt / total * 100) if total > 0 else 0.0
        vals = [r[0] for r in unexpected_df.select(self.column).limit(20).collect()]
        return self._build_result(success=(cnt == 0), element_count=total,
                                  unexpected_count=cnt, unexpected_percent=pct,
                                  unexpected_values=vals)


# ---------------------------------------------------------------------------
# 7. expect_column_values_to_be_all_lowercase
# ---------------------------------------------------------------------------

@register_expectation
@dataclass
class ExpectColumnValuesToBeAllLowercase(Expectation):
    """Expect all string values in a column to be fully lowercased."""
    expectation_type: str = field(
        init=False, default="expect_column_values_to_be_all_lowercase"
    )

    def _validate_pandas(self, df: pd.DataFrame) -> ExpectationResult:
        series = df[self.column].dropna().astype(str)
        total = len(series)
        unexpected_mask = series != series.str.lower()
        unexpected_count = int(unexpected_mask.sum())
        pct = (unexpected_count / total * 100) if total > 0 else 0.0
        return self._build_result(success=(unexpected_count == 0), element_count=total,
                                  unexpected_count=unexpected_count, unexpected_percent=pct,
                                  unexpected_values=series[unexpected_mask].tolist()[:20])

    def _validate_spark(self, df: Any) -> ExpectationResult:
        from pyspark.sql import functions as F
        col = F.col(str(self.column))
        filtered = df.filter(col.isNotNull())
        total = filtered.count()
        unexpected_df = filtered.filter(col != F.lower(col))
        cnt = unexpected_df.count()
        pct = (cnt / total * 100) if total > 0 else 0.0
        vals = [r[0] for r in unexpected_df.select(self.column).limit(20).collect()]
        return self._build_result(success=(cnt == 0), element_count=total,
                                  unexpected_count=cnt, unexpected_percent=pct,
                                  unexpected_values=vals)
