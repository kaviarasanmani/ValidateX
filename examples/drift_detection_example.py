import pandas as pd
import numpy as np
import validatex as vx

def main():
    print("--- Generating Synthetic Data ---")
    np.random.seed(42)
    
    # Yesterday's data
    baseline_df = pd.DataFrame({
        "user_id": range(1, 1001),
        "age": np.random.normal(35, 10, 1000),         # Stable distribution
        "income": np.random.normal(60000, 5000, 1000), # Stable distribution
        "country": ["US"] * 600 + ["UK"] * 300 + ["CA"] * 100
    })
    
    # Today's data (simulating drift)
    current_df = pd.DataFrame({
        "user_id": range(1001, 2001),
        "age": np.random.normal(35, 10, 1000),         # NO DRIFT: still 35
        "income": np.random.normal(85000, 5000, 1000), # DRIFT: mean jumped to 85k!
        "country": ["US"] * 100 + ["UK"] * 100 + ["CA"] * 800 # DRIFT: audience shifted to CA
    })

    print("--- Running ValidateX Drift Detector ---")
    
    # Initialize detector (default threshold is PSI > 0.2)
    detector = vx.DriftDetector()
    
    # Compare
    report = detector.compare(baseline_df, current_df)
    
    print("\n")
    # Print the beautiful built-in summary report
    print(report.summary())

if __name__ == "__main__":
    main()
