"""
Example Airflow DAG demonstrating ValidateX pipeline gating.
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

try:
    from validatex.integrations.airflow import ValidateXOperator
    from validatex.core.suite import ExpectationSuite
except ImportError:
    pass

# 1. Define expectations for your pipeline
suite = (
    ExpectationSuite("production_pipeline_checks")
    .add("expect_column_to_exist", column="user_id", severity="critical")
    .add("expect_column_values_to_be_unique", column="user_id")
    .add("expect_column_to_not_be_null", column="email")
    .add("expect_column_values_to_match_regex", column="email", regex=r"^[\w.]+@[\w]+\.\w+$", severity="warning")
)

# 2. Define the Airflow DAG
with DAG(
    dag_id="validatex_etl_pipeline",
    start_date=datetime(2026, 3, 4),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    # Task 1: Extract and load data (mocked)
    extract_data = BashOperator(
        task_id="extract_data",
        bash_command="echo 'id,user_id,email\n1,100,a@test.com\n2,101,b@test.com' > /tmp/daily_users.csv"
    )

    # Task 2: Validate Data Quality!
    # This task will FAIL if the data doesn't score 100/100, preventing the next task from running.
    validate_data = ValidateXOperator(
        task_id="ensure_data_quality",
        suite=suite,
        data_path="/tmp/daily_users.csv",
        data_format="csv",
        min_score=100.0,
        report_path="/tmp/validatex_daily_report.html"
    )

    # Task 3: Downstream processing (will only run if ValidateX gates it)
    publish_dashboard = BashOperator(
        task_id="publish_dashboard",
        bash_command="echo 'Data is perfect! Updating executive dashboard...'"
    )

    # Pipeline logic
    extract_data >> validate_data >> publish_dashboard
