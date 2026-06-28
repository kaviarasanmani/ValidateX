import os
import json
import urllib.request
import urllib.error
from unittest.mock import patch, MagicMock
import pytest

import validatex as vx
from validatex.core.result import ValidationResult, ExpectationResult


@pytest.fixture
def sample_validation_result():
    """Create a sample ValidationResult for testing."""
    # Create expectation results
    res_pass = ExpectationResult(
        expectation_type="expect_column_to_exist",
        success=True,
        column="user_id",
    )
    res_fail = ExpectationResult(
        expectation_type="expect_column_values_to_be_between",
        success=False,
        column="age",
        observed_value=150,
        element_count=10,
        unexpected_count=1,
        unexpected_percent=10.0,
        unexpected_values=[150],
    )

    val_res = ValidationResult(
        suite_name="test_alert_suite",
        results=[res_pass, res_fail],
        data_source="mock_data.csv",
        engine="pandas",
    )
    val_res.compute_statistics()
    return val_res


@pytest.fixture
def passing_validation_result():
    """Create a passing ValidationResult for testing."""
    res_pass = ExpectationResult(
        expectation_type="expect_column_to_exist",
        success=True,
        column="user_id",
    )
    val_res = ValidationResult(
        suite_name="test_pass_suite",
        results=[res_pass],
        data_source="mock_data.csv",
        engine="pandas",
    )
    val_res.compute_statistics()
    return val_res


@patch("urllib.request.urlopen")
def test_slack_notifier_format_and_send(mock_urlopen, sample_validation_result):
    """Test SlackNotifier constructs correct payload and successfully sends it."""
    # Mock response
    mock_response = MagicMock()
    mock_response.getcode.return_value = 200
    mock_urlopen.return_value.__enter__.return_value = mock_response

    webhook_url = "https://hooks.slack.com/services/test/webhook"

    # Send Slack alert
    success = sample_validation_result.send_slack(webhook_url=webhook_url)

    assert success is True
    assert mock_urlopen.call_count == 1

    # Inspect arguments passed to Request
    args, kwargs = mock_urlopen.call_args
    req = args[0]
    assert isinstance(req, urllib.request.Request)
    assert req.full_url == webhook_url
    assert req.headers["Content-type"] == "application/json"

    # Parse payload
    payload = json.loads(req.data.decode("utf-8"))
    assert "blocks" in payload
    assert len(payload["blocks"]) == 3  # Header, metrics, and failures block

    # Header block check
    assert payload["blocks"][0]["type"] == "header"
    assert "test_alert_suite" in payload["blocks"][0]["text"]["text"]

    # Metrics fields check
    fields = payload["blocks"][1]["fields"]
    assert any("SOME FAILED" in f["text"] for f in fields)
    assert any("Quality Score:" in f["text"] for f in fields)
    assert any("pandas" in f["text"] for f in fields)
    assert any("mock_data.csv" in f["text"] for f in fields)

    # Failures list check
    failures_text = payload["blocks"][2]["text"]["text"]
    assert "expect_column_values_to_be_between" in failures_text
    assert "column: age" in failures_text


@patch("urllib.request.urlopen")
def test_teams_notifier_format_and_send(mock_urlopen, sample_validation_result):
    """Test TeamsNotifier constructs correct MessageCard payload and successfully sends it."""
    mock_response = MagicMock()
    mock_response.getcode.return_value = 200
    mock_urlopen.return_value.__enter__.return_value = mock_response

    webhook_url = "https://outlook.office.com/webhook/test"

    success = sample_validation_result.send_teams(webhook_url=webhook_url)

    assert success is True
    assert mock_urlopen.call_count == 1

    args, kwargs = mock_urlopen.call_args
    req = args[0]
    payload = json.loads(req.data.decode("utf-8"))

    assert payload["@type"] == "MessageCard"
    assert payload["themeColor"] == "a30200"  # Fail color
    assert "test_alert_suite" in payload["title"]

    section = payload["sections"][0]
    assert len(section["facts"]) > 0
    assert any(f["name"] == "Status" and f["value"] == "SOME FAILED" for f in section["facts"])
    assert any(f["name"] == "Engine" and f["value"] == "pandas" for f in section["facts"])

    assert "expect_column_values_to_be_between" in section["text"]
    assert "column: age" in section["text"]


@patch("urllib.request.urlopen")
def test_notify_on_filtering(mock_urlopen, sample_validation_result, passing_validation_result):
    """Test that notify_on='failure' triggers alerts only on failure, while 'always' triggers on both."""
    mock_response = MagicMock()
    mock_response.getcode.return_value = 200
    mock_urlopen.return_value.__enter__.return_value = mock_response

    webhook_url = "https://hooks.slack.com/services/test"

    # 1. Passing suite with notify_on="failure" -> should skip and return True without calling webhook
    res1 = passing_validation_result.send_slack(webhook_url=webhook_url, notify_on="failure")
    assert res1 is True
    assert mock_urlopen.call_count == 0

    # 2. Passing suite with notify_on="always" -> should call webhook
    res2 = passing_validation_result.send_slack(webhook_url=webhook_url, notify_on="always")
    assert res2 is True
    assert mock_urlopen.call_count == 1

    mock_urlopen.reset_mock()

    # 3. Failing suite with notify_on="failure" -> should call webhook
    res3 = sample_validation_result.send_slack(webhook_url=webhook_url, notify_on="failure")
    assert res3 is True
    assert mock_urlopen.call_count == 1


@patch("urllib.request.urlopen")
def test_environment_variable_fallback(mock_urlopen, sample_validation_result):
    """Test that notifications fall back to environment variables when webhook_url is not specified."""
    mock_response = MagicMock()
    mock_response.getcode.return_value = 200
    mock_urlopen.return_value.__enter__.return_value = mock_response

    env_slack = "https://hooks.slack.com/services/env_slack"
    env_teams = "https://outlook.office.com/webhook/env_teams"

    with patch.dict(os.environ, {
        "VALIDATEX_SLACK_WEBHOOK_URL": env_slack,
        "VALIDATEX_TEAMS_WEBHOOK_URL": env_teams
    }):
        # Send without specifying URL
        res_slack = sample_validation_result.send_slack()
        res_teams = sample_validation_result.send_teams()

        assert res_slack is True
        assert res_teams is True

        # Verify the env URLs were used
        assert mock_urlopen.call_count == 2
        calls = mock_urlopen.call_args_list
        assert calls[0][0][0].full_url == env_slack
        assert calls[1][0][0].full_url == env_teams


@patch("urllib.request.urlopen")
def test_webhook_error_handling(mock_urlopen, sample_validation_result):
    """Test that webhook errors (HTTPError or general Exception) are handled gracefully and return False."""
    # 1. HTTP Error
    mock_urlopen.side_effect = urllib.error.HTTPError(
        url="https://hooks.slack.com",
        code=500,
        msg="Internal Server Error",
        hdrs=None,
        fp=None
    )

    # Should not raise exception, but return False
    with patch("sys.stderr.write") as mock_stderr:
        success = sample_validation_result.send_slack(webhook_url="https://hooks.slack.com/services/test")
        assert success is False
        assert mock_stderr.call_count == 1
        assert "HTTP Error" in mock_stderr.call_args[0][0]

    # 2. General Connection Exception
    mock_urlopen.side_effect = Exception("Connection timed out")
    with patch("sys.stderr.write") as mock_stderr:
        success = sample_validation_result.send_teams(webhook_url="https://outlook.office.com/webhook/test")
        assert success is False
        assert mock_stderr.call_count == 1
        assert "Connection Error" in mock_stderr.call_args[0][0]
