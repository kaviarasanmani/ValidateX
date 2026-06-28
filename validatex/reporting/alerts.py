"""
Alerts — Webhook notification senders for Slack and Microsoft Teams.
"""

from __future__ import annotations

import json
import urllib.request
import urllib.error
import sys
from typing import Any, Dict, List, Optional


def send_webhook_request(url: str, payload: Dict[str, Any]) -> bool:
    """
    Send a POST request with a JSON payload to the specified webhook URL.

    Uses standard library urllib to avoid third-party dependencies.
    """
    try:
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as response:
            status = response.getcode()
            return bool(200 <= status < 300)
    except urllib.error.HTTPError as e:
        sys.stderr.write(f"ValidateX Webhook HTTP Error: {e.code} - {e.reason}\n")
        return False
    except Exception as e:
        sys.stderr.write(f"ValidateX Webhook Connection Error: {str(e)}\n")
        return False


class SlackNotifier:
    """Formatter and sender for Slack incoming webhooks."""

    @staticmethod
    def send(result: Any, webhook_url: str) -> bool:
        """Format the ValidationResult and post it to Slack."""
        status_emoji = "✅" if result.success else "❌"
        status_text = "ALL PASSED" if result.success else "SOME FAILED"
        score = result.compute_quality_score()

        header_block = {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"🚀 ValidateX Report — {result.suite_name}",
                "emoji": True,
            },
        }

        fields = [
            {"type": "mrkdwn", "text": f"*Status:* {status_emoji} {status_text}"},
            {"type": "mrkdwn", "text": f"*Quality Score:* {score} / 100"},
            {"type": "mrkdwn", "text": f"*Total Expectations:* {result.total_expectations}"},
            {"type": "mrkdwn", "text": f"*Passed:* {result.successful_expectations}"},
            {"type": "mrkdwn", "text": f"*Failed:* {result.failed_expectations}"},
            {"type": "mrkdwn", "text": f"*Errors:* {result.errored_expectations}"},
            {"type": "mrkdwn", "text": f"*Engine:* {result.engine}"},
        ]
        if result.data_source:
            fields.append({"type": "mrkdwn", "text": f"*Data Source:* {result.data_source}"})

        section_block = {"type": "section", "fields": fields}

        blocks = [header_block, section_block]

        # Gather up to 5 failures
        failed_list = []
        for r in result.results:
            if not r.success:
                col_str = f" (column: {r.column})" if r.column else ""
                failed_list.append(f"• {r.status_icon} {r.severity_icon} *{r.expectation_type}*{col_str}")
                if len(failed_list) >= 5:
                    break

        if failed_list:
            failures_block = {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*Failed / Errored Expectations (up to 5):*\n" + "\n".join(failed_list),
                },
            }
            blocks.append(failures_block)

        payload = {
            "text": f"ValidateX Alert: {result.suite_name} {status_text.lower()}",
            "blocks": blocks,
        }

        return send_webhook_request(webhook_url, payload)


class TeamsNotifier:
    """Formatter and sender for Microsoft Teams incoming webhooks."""

    @staticmethod
    def send(result: Any, webhook_url: str) -> bool:
        """Format the ValidationResult and post it to Teams (MessageCard format)."""
        status_text = "ALL PASSED" if result.success else "SOME FAILED"
        theme_color = "2eb886" if result.success else "a30200"
        score = result.compute_quality_score()

        facts = [
            {"name": "Status", "value": status_text},
            {"name": "Quality Score", "value": f"{score} / 100"},
            {"name": "Total Expectations", "value": str(result.total_expectations)},
            {"name": "Passed", "value": str(result.successful_expectations)},
            {"name": "Failed", "value": str(result.failed_expectations)},
            {"name": "Errors", "value": str(result.errored_expectations)},
            {"name": "Engine", "value": result.engine},
        ]
        if result.data_source:
            facts.append({"name": "Data Source", "value": result.data_source})

        failed_list = []
        for r in result.results:
            if not r.success:
                col_str = f" (column: {r.column})" if r.column else ""
                failed_list.append(f"* {r.status_icon} {r.severity_icon} **{r.expectation_type}**{col_str}")
                if len(failed_list) >= 5:
                    break

        text = ""
        if failed_list:
            text = "### Failed / Errored Expectations (up to 5)\n" + "\n".join(failed_list)

        payload = {
            "@type": "MessageCard",
            "@context": "http://schema.org/extensions",
            "themeColor": theme_color,
            "summary": f"ValidateX Report — {result.suite_name}",
            "title": f"ValidateX Report — {result.suite_name}",
            "sections": [
                {
                    "facts": facts,
                    "text": text,
                    "markdown": True,
                }
            ],
        }

        return send_webhook_request(webhook_url, payload)
