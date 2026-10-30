"""
Historical analytics over recorded runs: quality-score trends, failing-expectation trends and schema changes.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

# A quality-score slope smaller than this many points per run is reported as "stable".
STABLE_SLOPE = 0.25
# A change in failure rate between the older and newer half of the window that counts as a real trend.
TREND_DELTA = 0.2


# ---------------------------------------------------------------------------
# Quality score trend
# ---------------------------------------------------------------------------


@dataclass
class QualityTrend:
    """Quality scores over time (oldest first) with summary statistics."""

    points: List[Dict[str, Any]] = field(default_factory=list)

    @property
    def scores(self) -> List[float]:
        return [float(p["quality_score"]) for p in self.points]

    @property
    def runs(self) -> int:
        return len(self.points)

    @property
    def latest(self) -> Optional[float]:
        return self.scores[-1] if self.points else None

    @property
    def previous(self) -> Optional[float]:
        return self.scores[-2] if len(self.points) >= 2 else None

    @property
    def delta(self) -> Optional[float]:
        """Latest score minus the previous run's score."""
        if self.latest is None or self.previous is None:
            return None
        return round(self.latest - self.previous, 2)

    @property
    def mean(self) -> Optional[float]:
        return round(sum(self.scores) / len(self.scores), 2) if self.points else None

    @property
    def delta_from_mean(self) -> Optional[float]:
        if self.latest is None or self.mean is None:
            return None
        return round(self.latest - self.mean, 2)

    @property
    def minimum(self) -> Optional[float]:
        return min(self.scores) if self.points else None

    @property
    def maximum(self) -> Optional[float]:
        return max(self.scores) if self.points else None

    @property
    def pass_rate(self) -> Optional[float]:
        """Share of runs where every expectation passed (0-100)."""
        if not self.points:
            return None
        ok = sum(1 for p in self.points if p.get("success"))
        return round(ok / len(self.points) * 100, 1)

    @property
    def slope(self) -> float:
        """Least-squares change in quality score per run."""
        n = len(self.points)
        if n < 2:
            return 0.0
        xs = list(range(n))
        mx, my = sum(xs) / n, sum(self.scores) / n
        den = sum((x - mx) ** 2 for x in xs)
        return sum((x - mx) * (y - my) for x, y in zip(xs, self.scores)) / den if den else 0.0

    @property
    def direction(self) -> str:
        """``improving``, ``degrading`` or ``stable``."""
        if self.slope > STABLE_SLOPE:
            return "improving"
        if self.slope < -STABLE_SLOPE:
            return "degrading"
        return "stable"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "runs": self.runs,
            "latest": self.latest,
            "previous": self.previous,
            "delta": self.delta,
            "mean": self.mean,
            "min": self.minimum,
            "max": self.maximum,
            "pass_rate": self.pass_rate,
            "slope_per_run": round(self.slope, 3),
            "direction": self.direction,
            "points": self.points,
        }


def quality_trend(
    store: Any, dataset_name: Optional[str] = None, suite_name: Optional[str] = None, limit: int = 30
) -> QualityTrend:
    """Build the quality-score trend for the most recent *limit* runs of a dataset and/or suite."""
    runs = store.get_runs(dataset_name=dataset_name, suite_name=suite_name, limit=limit)
    points = [
        {
            "run_id": r["run_id"],
            "timestamp": r["timestamp"],
            "quality_score": r["quality_score"],
            "success": bool(r.get("success")) if r.get("success") is not None else r["failed_expectations"] == 0,
            "failed_expectations": r["failed_expectations"],
            "run_name": r.get("run_name"),
        }
        for r in reversed(runs)
    ]
    return QualityTrend(points=points)


# ---------------------------------------------------------------------------
# Failed expectation trends
# ---------------------------------------------------------------------------


@dataclass
class FailureTrend:
    """How often one expectation failed across the window, and whether that is getting worse."""

    expectation_type: str
    column: Optional[str]
    rule_id: Optional[str]
    severity: Optional[str]
    evaluations: int
    failures: int
    current_streak: int
    last_failed_run_id: Optional[int]
    trend: str  # worsening | improving | stable

    @property
    def failure_rate(self) -> float:
        return round(self.failures / self.evaluations * 100, 1) if self.evaluations else 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "expectation_type": self.expectation_type,
            "column": self.column,
            "rule_id": self.rule_id,
            "severity": self.severity,
            "evaluations": self.evaluations,
            "failures": self.failures,
            "failure_rate": self.failure_rate,
            "current_streak": self.current_streak,
            "last_failed_run_id": self.last_failed_run_id,
            "trend": self.trend,
        }


def _failure_rate(flags: List[bool]) -> float:
    return sum(1 for f in flags if f) / len(flags) if flags else 0.0


def failed_expectation_trends(
    store: Any,
    dataset_name: Optional[str] = None,
    suite_name: Optional[str] = None,
    limit: int = 30,
    top: Optional[int] = None,
) -> List[FailureTrend]:
    """Rank expectations that failed at least once in the last *limit* runs, worst first."""
    runs = store.get_runs(dataset_name=dataset_name, suite_name=suite_name, limit=limit)
    run_ids = [int(r["run_id"]) for r in reversed(runs)]  # oldest first
    history: Dict[Any, Dict[int, bool]] = {}
    meta: Dict[Any, Dict[str, Any]] = {}

    for run_id in run_ids:
        seen: Dict[Any, int] = {}
        for row in store.get_expectation_results(run_id):
            base = (row["expectation_type"], row["column_name"], row["rule_id"])
            seen[base] = seen.get(base, 0) + 1
            key = base if seen[base] == 1 else base + (seen[base],)
            history.setdefault(key, {})[run_id] = not bool(row["success"])
            meta[key] = row

    trends: List[FailureTrend] = []
    for key, by_run in history.items():
        flags = [by_run[rid] for rid in run_ids if rid in by_run]
        failures = sum(1 for f in flags if f)
        if failures == 0:
            continue
        streak = 0
        for f in reversed(flags):
            if not f:
                break
            streak += 1
        failed_runs = [rid for rid in run_ids if by_run.get(rid)]
        mid = len(flags) // 2
        trend = "stable"
        if len(flags) >= 4:
            delta = _failure_rate(flags[mid:]) - _failure_rate(flags[:mid])
            trend = "worsening" if delta >= TREND_DELTA else "improving" if delta <= -TREND_DELTA else "stable"
        row = meta[key]
        trends.append(
            FailureTrend(
                expectation_type=row["expectation_type"],
                column=row["column_name"],
                rule_id=row["rule_id"],
                severity=row["severity"],
                evaluations=len(flags),
                failures=failures,
                current_streak=streak,
                last_failed_run_id=failed_runs[-1] if failed_runs else None,
                trend=trend,
            )
        )
    trends.sort(key=lambda t: (-t.failure_rate, -t.failures, t.expectation_type, t.column or ""))
    return trends[:top] if top else trends


# ---------------------------------------------------------------------------
# Schema change history
# ---------------------------------------------------------------------------


@dataclass
class SchemaChange:
    """One recorded change between consecutive schema snapshots of a dataset."""

    run_id: int
    timestamp: str
    added: List[str] = field(default_factory=list)
    removed: List[str] = field(default_factory=list)
    type_changes: Dict[str, Dict[str, str]] = field(default_factory=dict)
    initial: bool = False

    @property
    def is_breaking(self) -> bool:
        """Removed columns and changed types can break downstream consumers."""
        return bool(self.removed or self.type_changes)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "timestamp": self.timestamp,
            "initial": self.initial,
            "added": self.added,
            "removed": self.removed,
            "type_changes": self.type_changes,
            "breaking": self.is_breaking,
        }


def diff_schemas(old: Dict[str, str], new: Dict[str, str]) -> Dict[str, Any]:
    """Compare two ``{column: dtype}`` mappings."""
    return {
        "added": sorted(set(new) - set(old)),
        "removed": sorted(set(old) - set(new)),
        "type_changes": {c: {"from": old[c], "to": new[c]} for c in sorted(set(old) & set(new)) if old[c] != new[c]},
    }


def schema_changes(store: Any, dataset_name: str) -> List[SchemaChange]:
    """The schema evolution of a dataset: the initial schema plus every later change."""
    snaps = store.get_schema_snapshots(dataset_name)
    changes: List[SchemaChange] = []
    previous: Optional[Dict[str, str]] = None
    for snap in snaps:
        cols = snap["columns"]
        if previous is None:
            changes.append(
                SchemaChange(run_id=snap["run_id"], timestamp=snap["timestamp"], added=sorted(cols), initial=True)
            )
        else:
            d = diff_schemas(previous, cols)
            changes.append(
                SchemaChange(
                    run_id=snap["run_id"],
                    timestamp=snap["timestamp"],
                    added=d["added"],
                    removed=d["removed"],
                    type_changes=d["type_changes"],
                )
            )
        previous = cols
    return changes
