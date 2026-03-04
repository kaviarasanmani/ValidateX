import pandas as pd
import numpy as np

from validatex.drift.detector import DriftDetector

def test_drift_categorical():
    # Baseline: 50% A, 50% B
    base = pd.DataFrame({"category": ["A", "B"] * 50})
    # Current: 90% A, 10% B (drifted!)
    curr = pd.DataFrame({"category": ["A"] * 90 + ["B"] * 10})
    
    detector = DriftDetector(psi_threshold=0.2)
    report = detector.compare(base, curr)
    
    result = report.column_drifts["category"]
    assert result.feature_type == "categorical"
    assert result.is_drifted is True
    assert result.psi_score > 0.2
    
def test_drift_numerical():
    np.random.seed(42)
    # Baseline: Normal distribution mean 0, std 1
    base = pd.DataFrame({"value": np.random.normal(0, 1, 1000)})
    # Current: Shifted Normal distribution mean 2, std 1
    curr = pd.DataFrame({"value": np.random.normal(2, 1, 1000)})
    
    detector = DriftDetector(psi_threshold=0.2, bins=10)
    report = detector.compare(base, curr)
    
    result = report.column_drifts["value"]
    assert result.feature_type == "numerical"
    assert result.is_drifted is True
    assert result.psi_score > 0.2
    
def test_no_drift_numerical():
    np.random.seed(42)
    base = pd.DataFrame({"value": np.random.normal(0, 1, 1000)})
    curr = pd.DataFrame({"value": np.random.normal(0, 1, 1000)})
    
    detector = DriftDetector(psi_threshold=0.2, bins=10)
    report = detector.compare(base, curr)
    
    result = report.column_drifts["value"]
    assert result.is_drifted is False
    assert result.psi_score < 0.1

def test_schema_changes():
    base = pd.DataFrame({"id": [1, 2], "age": [30, 40], "email": ["a", "b"]})
    curr = pd.DataFrame({"id": [1, 2], "age": [30.5, 40.2], "new_col": [1, 2]})
    
    detector = DriftDetector()
    report = detector.compare(base, curr)
    
    assert "new_col" in report.schema_added_columns
    assert "email" in report.schema_removed_columns
    assert "age" in report.schema_type_changes
    
if __name__ == "__main__":
    test_drift_categorical()
    test_drift_numerical()
    test_no_drift_numerical()
    test_schema_changes()
    print("All drift tests passed!")
