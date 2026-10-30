"""
Security Unit Tests for ValidateX (P0)

Tests defense-in-depth protections against:
1. SQL Injection (column identifiers, table names, custom queries)
2. Unsafe YAML deserialization (arbitrary code execution payloads)
3. HTML Report XSS & <script> tag breakout
4. Webhook SSRF / unsafe URL schemes (file://, ftp://, javascript:)
5. Presence of repository SECURITY.md policy
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

import validatex as vx
from validatex.config.loader import load_checkpoint
from validatex.core.exceptions import ValidateXEngineError
from validatex.core.suite import ExpectationSuite
from validatex.core.utils import (
    format_sql_from_clause,
    quote_sql_identifier,
    validate_webhook_url,
)
from validatex.reporting.alerts import send_webhook_request


@pytest.fixture
def sql_engine_with_table():
    engine = create_engine("sqlite:///:memory:")
    with engine.connect() as conn:
        conn.execute(text("CREATE TABLE users (id INTEGER, name TEXT)"))
        conn.execute(text("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')"))
        conn.commit()
    return engine


class TestSQLInjectionPrevention:
    @pytest.mark.parametrize(
        "malicious_identifier",
        [
            "id; DROP TABLE users--",
            "id) FROM users; DROP TABLE users; --",
            'id" OR 1=1 --',
            "users /* comment */",
            "1=1; DELETE FROM users",
            "",
        ],
    )
    def test_quote_sql_identifier_rejects_injection(self, malicious_identifier: str):
        with pytest.raises(ValueError, match="Unsafe or invalid SQL identifier"):
            quote_sql_identifier(malicious_identifier)

    def test_quote_sql_identifier_allows_valid_names(self):
        assert quote_sql_identifier("user_id") == '"user_id"'
        assert quote_sql_identifier("public.users") == '"public"."users"'

    @pytest.mark.parametrize(
        "malicious_query",
        [
            "users; DROP TABLE users",
            "SELECT * FROM users; DELETE FROM users",
            "DROP TABLE users",
            "DELETE FROM users",
            "SELECT * FROM users -- comment",
            "SELECT * FROM users /* block comment */",
            "PRAGMA database_list",
        ],
    )
    def test_format_sql_from_clause_rejects_injection(self, malicious_query: str):
        with pytest.raises(ValueError, match="Unsafe"):
            format_sql_from_clause(malicious_query)

    def test_sql_expectation_blocks_column_injection_and_preserves_table(self, sql_engine_with_table):
        suite = vx.ExpectationSuite("sqli_suite").add(
            "expect_column_to_not_be_null",
            column="id; DROP TABLE users--",
        )
        res = vx.validate("users", suite, engine="sql", sql_engine=sql_engine_with_table)
        assert res.success is False
        assert res.results[0].status == "ERROR"
        assert "Unsafe or invalid SQL identifier" in (res.results[0].exception_info or "")

        # Verify table was NOT dropped
        with sql_engine_with_table.connect() as conn:
            count = conn.execute(text("SELECT COUNT(*) FROM users")).scalar()
            assert count == 2


class TestYAMLDeserializationSafety:
    def test_suite_load_blocks_arbitrary_python_object(self, tmp_path: Path):
        malicious_yaml = tmp_path / "malicious_suite.yaml"
        malicious_yaml.write_text(
            "!!python/object/apply:os.system ['echo pwned']\n",
            encoding="utf-8",
        )
        with pytest.raises(Exception):
            ExpectationSuite.load(str(malicious_yaml))

    def test_checkpoint_load_blocks_arbitrary_python_object(self, tmp_path: Path):
        malicious_cp = tmp_path / "malicious_cp.yaml"
        malicious_cp.write_text(
            "!!python/object/apply:os.system ['echo pwned']\n",
            encoding="utf-8",
        )
        with pytest.raises(Exception):
            load_checkpoint(str(malicious_cp))


class TestHTMLReportXSSPrevention:
    def test_html_report_escapes_xss_in_suite_columns_and_values(self, tmp_path: Path):
        df = pd.DataFrame(
            {
                "<img src=x onerror=alert(1)>": [
                    "<script>alert('xss')</script>",
                    "</script><script>alert('breakout')</script>",
                ]
            }
        )
        suite = vx.ExpectationSuite("<script>alert('suite')</script>").add(
            "expect_column_values_to_be_in_set",
            column="<img src=x onerror=alert(1)>",
            value_set=["safe_value"],
        )
        result = vx.validate(df, suite)
        report_file = tmp_path / "xss_report.html"
        result.to_html(str(report_file))

        html_content = report_file.read_text(encoding="utf-8")
        assert "<script>alert('suite')</script>" not in html_content
        assert "<script>alert('xss')</script>" not in html_content
        assert "</script><script>alert('breakout')</script>" not in html_content
        assert "<img src=x onerror=alert(1)>" not in html_content


class TestWebhookSSRFPrevention:
    @pytest.mark.parametrize(
        "unsafe_url",
        [
            "file:///etc/passwd",
            "file://C:/Windows/win.ini",
            "ftp://internal.server/data",
            "javascript:alert(1)",
            "",
            "https://",
        ],
    )
    def test_validate_webhook_url_blocks_unsafe_schemes(self, unsafe_url: str):
        with pytest.raises(ValueError, match="Invalid or unsafe webhook URL"):
            validate_webhook_url(unsafe_url)

    def test_send_webhook_request_returns_false_on_file_scheme(self):
        assert send_webhook_request("file:///etc/passwd", {"text": "test"}) is False


class TestEngineValidationAndSecurityPolicy:
    def test_invalid_engine_raises_validatex_engine_error(self):
        suite = vx.ExpectationSuite("test")
        with pytest.raises(ValidateXEngineError, match="Unsupported engine"):
            vx.Validator(suite, engine="invalid_engine")

    def test_security_md_policy_exists(self):
        repo_root = Path(__file__).resolve().parents[2]
        security_md = repo_root / "SECURITY.md"
        assert security_md.exists(), "SECURITY.md must exist at repository root"
        content = security_md.read_text(encoding="utf-8")
        assert "Reporting a Vulnerability" in content
        assert "Supported Versions" in content
