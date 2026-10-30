"""
Volume anomaly detection.

Compares a dataset's current row count with its recorded history using a robust (median / MAD) modified
z-score, so a single earlier outlier does not mask or trigger detections. A change is only flagged when it is
both statistically unusual **and** at least ``min_pct_change`` percent away from the typical volume, which avoids
alerting on tiny wobbles in very stable datasets.
"""

from __future__ import annotations

import math
import statistics
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class VolumeAnomaly:
    """Result of comparing the current row count against history."""

    anomaly: bool
    current: int
    expected: Optional[float]
    deviation: Optional[float]
    pct_change: Optional[float]
    history_size: int
    direction: Optional[str]  # spike | drop
    insufficient_history: bool
    message: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "anomaly": self.anomaly,
            "current": self.current,
            "expected": self.expected,
            "deviation": None if self.deviation is None or math.isinf(self.deviation) else round(self.deviation, 2),
            "pct_change": self.pct_change,
            "history_size": self.history_size,
            "direction": self.direction,
            "insufficient_history": self.insufficient_history,
            "message": self.message,
        }

    def __bool__(self) -> bool:
        return self.anomaly


def detect_volume_anomaly_from_counts(
    history: List[int],
    current: int,
    min_history: int = 5,
    z_threshold: float = 3.5,
    min_pct_change: float = 5.0,
) -> VolumeAnomaly:
    """Core detection on a plain list of historical row counts (oldest first)."""
    if len(history) < min_history:
        return VolumeAnomaly(
            False,
            current,
            None,
            None,
            None,
            len(history),
            None,
            True,
            f"Not enough history to judge volume ({len(history)} of {min_history} runs needed)",
        )

    median = float(statistics.median(history))
    mad = float(statistics.median([abs(x - median) for x in history]))
    if mad > 0:
        z = 0.6745 * (current - median) / mad
    else:
        std = statistics.pstdev(history)
        mean = statistics.fmean(history)
        if std > 0:
            z = (current - mean) / std
        else:
            z = 0.0 if current == median else math.copysign(math.inf, current - median)

    pct = ((current - median) / median * 100.0) if median else (0.0 if current == 0 else math.inf)
    anomalous = abs(z) >= z_threshold and abs(pct) >= min_pct_change
    direction = ("spike" if current > median else "drop") if anomalous else None
    pct_out: Optional[float] = None if math.isinf(pct) else round(pct, 2)
    if anomalous:
        msg = f"Row count {direction}: {current:,} vs typical {median:,.0f} ({pct_out}% change, score {z:+.1f})"
    else:
        msg = f"Row count {current:,} is within the normal range (typical {median:,.0f})"
    return VolumeAnomaly(anomalous, current, median, z, pct_out, len(history), direction, False, msg)


def detect_volume_anomaly(
    store: Any,
    dataset_name: str,
    current_row_count: int,
    min_history: int = 5,
    z_threshold: float = 3.5,
    min_pct_change: float = 5.0,
    limit: int = 30,
    exclude_run_id: Optional[int] = None,
) -> VolumeAnomaly:
    """Compare *current_row_count* with the recorded row counts of *dataset_name*."""
    counts = [c for _, c in store.get_row_counts(dataset_name, limit=limit, exclude_run_id=exclude_run_id)]
    return detect_volume_anomaly_from_counts(
        counts, int(current_row_count), min_history=min_history, z_threshold=z_threshold, min_pct_change=min_pct_change
    )
