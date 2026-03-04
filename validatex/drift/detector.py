"""
Core Drift Detector engine for computing Population Stability Index (PSI).
"""

from __future__ import annotations

from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from validatex.drift.report import ColumnDriftResult, DriftReport


class DriftDetector:
    """
    Detects data drift between a baseline and a current Pandas DataFrame.
    Calculates Population Stability Index (PSI) to detect statistical
    shifts in distributions.
    """

    def __init__(self, psi_threshold: float = 0.2, bins: int = 10):
        """
        Parameters
        ----------
        psi_threshold : float
            The threshold above which a feature is considered drifted.
            Common rules of thumb:
            - PSI < 0.1: No significant drift
            - 0.1 <= PSI < 0.2: Moderate drift
            - PSI >= 0.2: Significant drift
        bins : int
            Number of buckets to use for numerical PSI calculation.
        """
        self.psi_threshold = psi_threshold
        self.bins = bins

    def compare(self, df_base: pd.DataFrame, df_current: pd.DataFrame) -> DriftReport:
        """
        Run schema and statistical drift comparison between two DataFrames.
        """
        cols_base = set(df_base.columns)
        cols_current = set(df_current.columns)

        added = list(cols_current - cols_base)
        removed = list(cols_base - cols_current)
        shared = list(cols_base.intersection(cols_current))

        type_changes: Dict[str, Dict[str, str]] = {}
        column_drifts: Dict[str, ColumnDriftResult] = {}

        for col in shared:
            type_base = str(df_base[col].dtype)
            type_curr = str(df_current[col].dtype)
            
            if type_base != type_curr:
                # Optionally, we might just flag differences that cross numeric/object bounds
                type_changes[col] = {"baseline": type_base, "current": type_curr}

            # Drop completely null subsets
            s_base = df_base[col].dropna()
            s_curr = df_current[col].dropna()

            if len(s_base) == 0 or len(s_curr) == 0:
                continue

            # Determine feature type based on pandas generic types
            from pandas.api.types import is_numeric_dtype

            if is_numeric_dtype(s_base) and is_numeric_dtype(s_curr):
                psi, details = self._calculate_numeric_psi(s_base, s_curr)
                feat_type = "numerical"
            else:
                psi, details = self._calculate_categorical_psi(s_base, s_curr)
                feat_type = "categorical"

            column_drifts[col] = ColumnDriftResult(
                column=col,
                feature_type=feat_type,
                psi_score=psi,
                is_drifted=(psi >= self.psi_threshold),
                details=details,
            )

        return DriftReport(
            schema_added_columns=added,
            schema_removed_columns=removed,
            schema_type_changes=type_changes,
            column_drifts=column_drifts,
        )

    def _calculate_psi_array(self, expected_pct: np.ndarray, actual_pct: np.ndarray) -> float:
        """Core mathematical PSI calculation on probability arrays."""
        # Replace completely empty buckets with a tiny epsilon to prevent div/0 or log(0)
        eps = 1e-6
        expected_pct = np.clip(expected_pct, eps, 1.0)
        actual_pct = np.clip(actual_pct, eps, 1.0)
        
        # PSI = sum((Actual % - Expected %) * ln(Actual % / Expected %))
        psi = np.sum((actual_pct - expected_pct) * np.log(actual_pct / expected_pct))
        return float(psi)

    def _calculate_numeric_psi(
        self, base: pd.Series, current: pd.Series
    ) -> tuple[float, Dict]:
        """Calculate PSI for continuous numeric variables."""
        # Create bins based on the BASELINE distribution quantiles
        bins = pd.qcut(base, q=self.bins, retbins=True, duplicates="drop")[1]
        
        # Extend lowest and highest bin edges to capture outliers in current data
        bins[0] = -np.inf
        bins[-1] = np.inf

        # Bucket both series
        base_counts = pd.cut(base, bins=bins).value_counts(sort=False).values
        curr_counts = pd.cut(current, bins=bins).value_counts(sort=False).values

        base_pct = base_counts / max(len(base), 1)
        curr_pct = curr_counts / max(len(current), 1)
        
        psi = self._calculate_psi_array(base_pct, curr_pct)
        return psi, {"baseline_bins": len(bins) - 1}

    def _calculate_categorical_psi(
        self, base: pd.Series, current: pd.Series
    ) -> tuple[float, Dict]:
        """Calculate PSI for categorical variables by treating distinct values as buckets."""
        # Get counts
        base_counts = base.value_counts(normalize=True)
        curr_counts = current.value_counts(normalize=True)
        
        # Align indexes to ensure buckets match exactly
        all_categories = list(set(base_counts.index).union(set(curr_counts.index)))
        
        base_pct = np.array([base_counts.get(c, 0.0) for c in all_categories])
        curr_pct = np.array([curr_counts.get(c, 0.0) for c in all_categories])
        
        psi = self._calculate_psi_array(base_pct, curr_pct)
        return psi, {"cardinality_baseline": len(base.unique()), "cardinality_current": len(current.unique())}
