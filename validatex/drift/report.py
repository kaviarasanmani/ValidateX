"""
Data Drift Reporting.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional


@dataclass
class ColumnDriftResult:
    """Stores drift results for a single column."""

    column: str
    feature_type: str  # 'numerical' or 'categorical'
    psi_score: float
    is_drifted: bool
    details: Dict[str, Any]


@dataclass
class DriftReport:
    """Represents a full data drift comparison report."""

    schema_added_columns: List[str]
    schema_removed_columns: List[str]
    schema_type_changes: Dict[str, Dict[str, str]]
    column_drifts: Dict[str, ColumnDriftResult]

    def to_dict(self) -> Dict[str, Any]:
        """Convert the drift report to a dictionary."""
        return {
            "schema_changes": {
                "added_columns": self.schema_added_columns,
                "removed_columns": self.schema_removed_columns,
                "type_changes": self.schema_type_changes,
            },
            "columns": {
                col: asdict(result) for col, result in self.column_drifts.items()
            },
        }

    def to_json(self, indent: int = 2) -> str:
        """Convert the report to a JSON string."""
        return json.dumps(self.to_dict(), indent=indent)

    def summary(self) -> str:
        """Return a human-readable summary of the drift report."""
        lines = []
        lines.append("=" * 60)
        lines.append("  ValidateX Data Drift Report")
        lines.append("=" * 60)

        lines.append("\n[1] Schema Changes:")
        if not self.schema_added_columns and not self.schema_removed_columns and not self.schema_type_changes:
            lines.append("  No schema changes detected.")
        else:
            if self.schema_added_columns:
                lines.append(f"  + Added: {', '.join(self.schema_added_columns)}")
            if self.schema_removed_columns:
                lines.append(f"  - Removed: {', '.join(self.schema_removed_columns)}")
            for col, types in self.schema_type_changes.items():
                lines.append(f"  ~ Type Changed ({col}): {types['baseline']} -> {types['current']}")

        lines.append("\n[2] Feature Drift (PSI):")
        drifted_count = sum(1 for res in self.column_drifts.values() if res.is_drifted)
        lines.append(f"  {drifted_count} out of {len(self.column_drifts)} shared features drifted.")
        
        for col, res in self.column_drifts.items():
            status = "🔴 DRIFTED" if res.is_drifted else "🟢 STABLE"
            lines.append(f"  {status} | {col.ljust(20)} | PSI: {res.psi_score:.4f} ({res.feature_type})")

        return "\n".join(lines)
