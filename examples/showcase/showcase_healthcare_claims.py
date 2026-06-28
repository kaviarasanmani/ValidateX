"""
ValidateX Real-World Showcase: Healthcare Claims & Clinical Data Quality

Demonstrates validating HIPAA-sensitive healthcare encounter and insurance claims data.
Checks include:
  - Patient & Provider ID integrity
  - Diagnosis code (ICD-10) regex pattern formatting
  - Admission vs Discharge date ordering
  - Non-null mandatory clinical fields
"""

import sys
import pandas as pd
import validatex as vx

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

def run_healthcare_showcase():
    print("🚀 Running ValidateX Showcase: Healthcare Claims Quality Gate")

    # 1. Representative Healthcare Claims dataset
    data = {
        "claim_id": ["CLM-8001", "CLM-8002", "CLM-8003", "CLM-8004", "CLM-8005"],
        "patient_id": ["PT-1009", "PT-1010", "PT-1011", "PT-1012", "PT-1013"],
        "icd10_code": ["E11.9", "I10", "J45.909", "M54.5", "K21.9"],
        "admission_date": ["2026-05-10", "2026-05-12", "2026-05-15", "2026-05-18", "2026-05-20"],
        "discharge_date": ["2026-05-14", "2026-05-12", "2026-05-19", "2026-05-18", "2026-05-22"],
        "billed_amount": [4500.00, 1200.00, 8900.00, 650.00, 2300.00],
    }
    df = pd.DataFrame(data)

    # 2. Define Healthcare Quality Suite
    suite = (
        vx.ExpectationSuite("healthcare_claims_checks")
        .add("expect_column_to_not_be_null", column="claim_id")
        .add("expect_column_values_to_be_unique", column="claim_id")
        .add("expect_column_to_not_be_null", column="patient_id")
        .add("expect_column_values_to_match_regex", column="icd10_code", regex=r"^[A-Z][0-9][0-9A-Z](\.[0-9A-Z]{1,4})?$")
        .add("expect_column_values_to_be_valid_iso_date", column="admission_date")
        .add("expect_column_values_to_be_valid_iso_date", column="discharge_date")
        .add("expect_column_pair_values_a_to_be_greater_than_b", column="discharge_date", column_b="admission_date", allow_equal=True)
        .add("expect_column_values_to_be_positive", column="billed_amount")
    )

    # 3. Validate
    result = vx.validate(df, suite)
    print(result.summary())

    # 4. Save reports
    result.to_html("reports/showcase_healthcare.html")
    result.to_json_file("reports/showcase_healthcare.json")
    print("\n✅ Saved reports to reports/showcase_healthcare.html & .json")

if __name__ == "__main__":
    run_healthcare_showcase()
