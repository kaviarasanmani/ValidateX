"""
Threshold-based alerting over recorded validation history.

An :class:`AlertPolicy` declares *when* a run deserves attention (quality below a floor, a sudden quality drop,
repeated failures, a critical expectation failing, a volume anomaly, a schema change, stale data). Evaluating it
returns :class:`Alert` objects, which can optionally be pushed to Slack / Microsoft Teams webhooks.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from validatex.observability.volume import detect_volume_anomaly


@dataclass
class Alert:
    """A single alert raised by an :class:`AlertPolicy`."""

    rule: str
    severity: str  # critical | warning | info
    message: str
    dataset_name: Optional[str] = None
    run_id: Optional[int] = None
    details: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "rule": self.rule,
            "severity": self.severity,
            "message": self.message,
            "dataset_name": self.dataset_name,
            "run_id": self.run_id,
            "details": self.details,
        }


@dataclass
class AlertPolicy:
    """
    Declarative alert thresholds. Every check is opt-in; unset checks are skipped.

    Parameters
    ----------
    min_quality_score : float, optional
        Alert when the run's quality score falls below this value.
    max_quality_drop : float, optional
        Alert when the score is more than this many points below the mean of the previous ``baseline_runs`` runs.
    max_consecutive_failures : int, optional
        Alert when this many runs in a row (including the current one) did not fully pass.
    alert_on_critical_failure : bool
        Alert whenever any critical-severity check fails.
    volume_anomaly : bool
        Alert when the row count deviates unusually from history.
    alert_on_schema_change : bool
        Alert when the schema differs from the previous run (critical if a column was removed or retyped).
    slack_webhook, teams_webhook : str, optional
        If set, alerts are posted to these webhooks automatically.
    """

    min_quality_score: Optional[float] = None
    max_quality_drop: Optional[float] = None
    baseline_runs: int = 5
    max_consecutive_failures: Optional[int] = None
    alert_on_critical_failure: bool = False
    volume_anomaly: bool = False
    volume_z_threshold: float = 3.5
    volume_min_pct_change: float = 5.0
    volume_min_history: int = 5
    alert_on_schema_change: bool = False
    slack_webhook: Optional[str] = None
    teams_webhook: Optional[str] = None

    def evaluate(
        self,
        result: Any,
        store: Any = None,
        freshness: Any = None,
    ) -> List[Alert]:
        """Return the alerts triggered by *result* (the run just recorded in *store*, if any)."""
        alerts: List[Alert] = []
        dataset = getattr(result, "dataset_name", None) or result.suite_name
        run_id = getattr(result, "run_id", None)
        score = result.compute_quality_score()

        def raise_alert(rule: str, severity: str, message: str, /, **details: Any) -> None:
            alerts.append(Alert(rule, severity, message, dataset, run_id, details))

        if self.min_quality_score is not None and score < self.min_quality_score:
            raise_alert(
                "min_quality_score",
                "critical",
                f"Quality score {score} is below the minimum {self.min_quality_score}",
                score=score,
                minimum=self.min_quality_score,
            )

        if self.alert_on_critical_failure:
            critical = [r for r in result.results if not r.success and r.severity == "critical"]
            if critical:
                names = ", ".join(
                    f"{r.expectation_type}({r.column})" if r.column else r.expectation_type for r in critical[:5]
                )
                raise_alert(
                    "critical_failure",
                    "critical",
                    f"{len(critical)} critical check(s) failed: {names}",
                    count=len(critical),
                )

        if store is not None:
            if self.max_quality_drop is not None:
                prior = [
                    r["quality_score"]
                    for r in store.get_runs(dataset_name=dataset, limit=self.baseline_runs + 1)
                    if r["run_id"] != run_id
                ][: self.baseline_runs]
                if prior:
                    baseline = sum(prior) / len(prior)
                    drop = baseline - score
                    if drop > self.max_quality_drop:
                        raise_alert(
                            "quality_drop",
                            "warning",
                            f"Quality score fell {drop:.1f} points to {score} (baseline {baseline:.1f})",
                            baseline=round(baseline, 2),
                            drop=round(drop, 2),
                        )

            if self.max_consecutive_failures is not None:
                streak = 0
                for r in store.get_runs(dataset_name=dataset, limit=self.max_consecutive_failures + 1):
                    if r.get("success") == 0 or (r.get("success") is None and r["failed_expectations"] > 0):
                        streak += 1
                    else:
                        break
                if streak >= self.max_consecutive_failures:
                    raise_alert(
                        "consecutive_failures",
                        "critical",
                        f"{streak} consecutive validation run(s) failed for '{dataset}'",
                        streak=streak,
                    )

            if self.volume_anomaly and getattr(result, "row_count", None) is not None:
                vol = detect_volume_anomaly(
                    store,
                    dataset,
                    result.row_count,
                    min_history=self.volume_min_history,
                    z_threshold=self.volume_z_threshold,
                    min_pct_change=self.volume_min_pct_change,
                    exclude_run_id=run_id,
                )
                if vol.anomaly:
                    raise_alert("volume_anomaly", "warning", vol.message, **vol.to_dict())

            if self.alert_on_schema_change and run_id is not None:
                changes = store.schema_changes(dataset)
                latest = changes[-1] if changes else None
                if latest is not None and not latest.initial and latest.run_id == run_id:
                    parts = []
                    if latest.added:
                        parts.append(f"added {latest.added}")
                    if latest.removed:
                        parts.append(f"removed {latest.removed}")
                    if latest.type_changes:
                        parts.append(f"retyped {sorted(latest.type_changes)}")
                    raise_alert(
                        "schema_change",
                        "critical" if latest.is_breaking else "warning",
                        f"Schema of '{dataset}' changed: " + "; ".join(parts),
                        **latest.to_dict(),
                    )

        if freshness is not None and not getattr(freshness, "fresh", True):
            raise_alert("stale_data", "critical", freshness.message, **freshness.to_dict())

        return alerts


def dispatch_alerts(
    alerts: List[Alert],
    slack_webhook: Optional[str] = None,
    teams_webhook: Optional[str] = None,
) -> Dict[str, bool]:
    """Post *alerts* to Slack and/or Teams. Returns ``{"slack": ok, "teams": ok}`` for each webhook used."""
    from validatex.reporting.alerts import send_webhook_request

    sent: Dict[str, bool] = {}
    if not alerts:
        return sent
    icons = {"critical": "🔴", "warning": "🟡", "info": "🔵"}
    lines = [f"{icons.get(a.severity, '🟡')} [{a.severity}] {a.message}" for a in alerts]
    dataset = alerts[0].dataset_name or "dataset"
    title = f"ValidateX alerts for {dataset}"

    if slack_webhook:
        sent["slack"] = send_webhook_request(slack_webhook, {"text": f"*{title}*\n" + "\n".join(lines)})
    if teams_webhook:
        sent["teams"] = send_webhook_request(
            teams_webhook,
            {
                "@type": "MessageCard",
                "@context": "http://schema.org/extensions",
                "themeColor": "a30200" if any(a.severity == "critical" for a in alerts) else "ffa500",
                "summary": title,
                "title": title,
                "text": "\n\n".join(lines),
            },
        )
    return sent
