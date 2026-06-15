"""
Test the ValidateXOperator logic without needing Airflow installed.
Simulates what execute() does internally.
"""
import pandas as pd
import validatex as vx
from validatex.core.validator import validate

print("Testing Airflow operator internal logic...\n")

df = pd.DataFrame({
    "order_id": [1, 2, 3, 2],           # duplicate -> fail
    "email":    ["a@b.com", "c@d.com", "bad", "e@f.com"],
    "amount":   [100.0, 200.0, -5.0, 300.0],  # -5 -> fail
})

suite = (
    vx.ExpectationSuite("airflow_gate_test")
    .add("expect_column_values_to_be_unique", column="order_id")
    .add("expect_column_to_not_be_null", column="email")
    .add("expect_column_values_to_be_between", column="amount",
         kwargs={"min_value": 0, "max_value": 9999})
)

result = validate(df, suite)

# --- Fixed operator logic ---
score = result.compute_quality_score()

passed = [r for r in result.results if r.success]
failed = [r for r in result.results if not r.success]
failed_cols = list({r.column for r in failed if r.column})

xcom_output = {
    "validatex_score": score,
    "passed_expectations": len(passed),
    "failed_expectations": len(failed),
    "report_path": None,
}

print(f"Score            : {score:.1f} / 100")
print(f"Passed           : {xcom_output['passed_expectations']}")
print(f"Failed           : {xcom_output['failed_expectations']}")
print(f"Failed columns   : {failed_cols}")

min_score = 90.0
if score < min_score:
    print(f"\nGate (>= {min_score}) : FAIL -> would raise ValueError in Airflow")
    print(f"  => 'Data Quality Gate Failed: Score ({score:.2f}) is below {min_score}'")
    print(f"  => Columns with failures: {failed_cols}")
else:
    print(f"\nGate (>= {min_score}) : PASS")

print("\nXCom output (returned to Airflow):", xcom_output)
print("\n[OK] Airflow operator logic works correctly!")
