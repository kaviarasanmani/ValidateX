"""
ValidateX Observability - turn individual validation runs into historical data quality monitoring.

Capabilities: run history (:class:`~validatex.storage.run_store.RunStore`), quality-score trends, failing
expectation trends, schema change history, data freshness checks, volume anomaly detection and threshold-based
alerting.
"""

from validatex.observability.alerting import Alert, AlertPolicy, dispatch_alerts
from validatex.observability.freshness import FreshnessResult, check_freshness, check_run_freshness, parse_age
from validatex.observability.trends import (
    FailureTrend,
    QualityTrend,
    SchemaChange,
    diff_schemas,
    failed_expectation_trends,
    quality_trend,
    schema_changes,
)
from validatex.observability.volume import VolumeAnomaly, detect_volume_anomaly, detect_volume_anomaly_from_counts

__all__ = [
    "Alert",
    "AlertPolicy",
    "FailureTrend",
    "FreshnessResult",
    "QualityTrend",
    "SchemaChange",
    "VolumeAnomaly",
    "check_freshness",
    "check_run_freshness",
    "detect_volume_anomaly",
    "detect_volume_anomaly_from_counts",
    "diff_schemas",
    "dispatch_alerts",
    "failed_expectation_trends",
    "parse_age",
    "quality_trend",
    "schema_changes",
]
