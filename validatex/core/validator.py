"""
Validator — orchestrates expectation suite execution against a dataset.

The :func:`validate` convenience function is the primary public entry point.
"""

from __future__ import annotations

import time
from typing import Any, Optional

from validatex.core.result import ValidationResult
from validatex.core.suite import ExpectationSuite


class Validator:
    """
    Runs an :class:`ExpectationSuite` against a dataset.

    Parameters
    ----------
    suite : ExpectationSuite
        The suite of expectations to evaluate.
    engine : str
        ``"pandas"`` or ``"spark"``.
    """

    def __init__(self, suite: ExpectationSuite, engine: str = "pandas"):
        self.suite = suite
        self.engine = engine.lower()

    def run(
        self,
        data: Any,
        data_source: Optional[str] = None,
    ) -> ValidationResult:
        """
        Execute every expectation in the suite against *data*.

        Parameters
        ----------
        data : pd.DataFrame | pyspark.sql.DataFrame
            The dataset to validate.
        data_source : str, optional
            A label describing where the data came from.

        Returns
        -------
        ValidationResult
        """
        result = ValidationResult(
            suite_name=self.suite.name,
            data_source=data_source,
            engine=self.engine,
        )

        start = time.perf_counter()
        for expectation in self.suite:
            exp_result = expectation.validate(data, engine=self.engine)
            result.results.append(exp_result)
        result.run_duration_seconds = time.perf_counter() - start
        result.compute_statistics()
        return result


def validate(
    data: Any,
    suite: ExpectationSuite,
    engine: str = "pandas",
    data_source: Optional[str] = None,
) -> ValidationResult:
    """
    Convenience function to validate *data* against a *suite*.

    Parameters
    ----------
    data : pd.DataFrame | pyspark.sql.DataFrame
    suite : ExpectationSuite
    engine : str
        ``"pandas"`` or ``"spark"``.
    data_source : str, optional

    Returns
    -------
    ValidationResult
    """
    return Validator(suite, engine=engine).run(data, data_source=data_source)
