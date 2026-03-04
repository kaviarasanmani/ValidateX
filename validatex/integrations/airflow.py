"""
Apache Airflow operator for ValidateX.
"""

from typing import Any, Dict, Optional

try:
    from airflow.models import BaseOperator
    from airflow.utils.context import Context
except ImportError:
    raise ImportError(
        "To use the ValidateXOperator, you must install apache-airflow:\n"
        "    pip install apache-airflow"
    )

import pandas as pd

from validatex.core.suite import ExpectationSuite
from validatex.core.validator import validate


class ValidateXOperator(BaseOperator):
    """
    Airflow Operator to run ValidateX data quality checks in a DAG.
    
    If the data quality score falls below the specified `min_score`,
    the Airflow task will fail, preventing downstream tasks from running
    and alerting the data engineering team.

    Parameters
    ----------
    suite : ExpectationSuite
        The ValidateX ExpectationSuite containing your validation rules.
    data_path : str
        The URI/path to the dataset to validate.
    data_format : str, optional
        The format of the data (e.g., "csv", "parquet"). Defaults to "csv".
    min_score : float, optional
        Minimum acceptable quality score (0-100). Default is 100.0.
    report_path : str, optional
        Path to save the generated HTML Data Quality report.
    """

    def __init__(
        self,
        *,
        suite: ExpectationSuite,
        data_path: str,
        data_format: str = "csv",
        min_score: float = 100.0,
        report_path: Optional[str] = None,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self.suite = suite
        self.data_path = data_path
        self.data_format = data_format
        self.min_score = min_score
        self.report_path = report_path

    def execute(self, context: Context) -> Dict[str, Any]:
        self.log.info(f"Running ValidateX Quality Checks. Expecting Min Score: {self.min_score}")
        
        self.log.info(f"Loading data from {self.data_path} ({self.data_format})")
        
        # Load the data for validation
        if self.data_format.lower() == "csv":
            df = pd.read_csv(self.data_path)
        elif self.data_format.lower() == "parquet":
            df = pd.read_parquet(self.data_path)
        else:
            raise ValueError(f"Unsupported data_format: {self.data_format}. Use 'csv' or 'parquet'.")

        self.log.info(f"Validating {len(df)} rows...")
        
        # Run ValidateX
        result = validate(df, self.suite)
        score = result.score
        
        self.log.info(f"\n{result.summary()}")
        self.log.info(f"ValidateX Final Score: {score:.2f} / 100.00")

        # Save Report
        if self.report_path:
            result.to_html(self.report_path)
            self.log.info(f"Saved HTML Data Quality report to {self.report_path}")

        # Gate the Airflow pipeline
        if score < self.min_score:
            failed_cols = [col for col, meta in result.columns.items() if not meta.success]
            raise ValueError(
                f"Data Quality Gate Failed: Score ({score:.2f}) is below "
                f"the threshold ({self.min_score}).\n"
                f"Columns with failures: {failed_cols}"
            )

        self.log.info("Data Quality Gate Passed. Task successful.")
        
        # Return a serializable dict into XComs so downstream tasks can access the metadata
        return {
            "validatex_score": score,
            "passed_expectations": result.passed_count,
            "failed_expectations": result.failed_count,
            "report_path": self.report_path
        }
