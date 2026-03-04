"""
ValidateX — Cross-Row & Custom Python Expectations.

Row-level conditional checks: If column A has value X,
then column B must satisfy condition Y. Plus user-defined
Python lambda expectations.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Optional

import pandas as pd

from validatex.core.expectation import Expectation, register_expectation
from validatex.core.result import ExpectationResult

# ---------------------------------------------------------------------------
# 1. expect_column_values_to_be_null_when (conditional null)
# ---------------------------------------------------------------------------


@register_expectation
@dataclass
class ExpectColumnValuesToBeNullWhen(Expectation):
    """
    Expect a column to be null when a condition column equals a specific value.

    Example: "refund_amount should be null when status != 'refunded'"

    kwargs:
        condition_column (str): The column to check the condition against.
        condition_value (any): The value of condition_column that triggers the rule.
        when (str): 'equal' (default) or 'not_equal'
    """

    expectation_type: str = field(init=False, default="expect_column_values_to_be_null_when")

    def _validate_pandas(self, df: pd.DataFrame) -> ExpectationResult:
        cond_col = self.kwargs.get("condition_column")
        cond_val = self.kwargs.get("condition_value")
        when = self.kwargs.get("when", "equal")

        if when == "equal":
            condition_mask = df[cond_col] == cond_val
        else:
            condition_mask = df[cond_col] != cond_val

        # Rows where condition is TRUE but target column is NOT null
        scoped = df[condition_mask]
        total = len(scoped)
        unexpected_mask = scoped[self.column].notnull()
        unexpected_count = int(unexpected_mask.sum())
        pct = (unexpected_count / total * 100) if total > 0 else 0.0

        return self._build_result(
            success=(unexpected_count == 0),
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            unexpected_values=scoped.loc[unexpected_mask, self.column].tolist()[:20],
            details={"condition": f"{cond_col} {when} {cond_val!r}"},
        )


# ---------------------------------------------------------------------------
# 2. expect_column_values_to_be_not_null_when (conditional not-null)
# ---------------------------------------------------------------------------


@register_expectation
@dataclass
class ExpectColumnValuesToBeNotNullWhen(Expectation):
    """
    Expect a column to NOT be null when a condition is met.

    Example: "shipping_address must not be null when order_type == 'physical'"

    kwargs:
        condition_column (str): The column whose value triggers this rule.
        condition_value (any): The trigger value.
        when (str): 'equal' (default) or 'not_equal'
    """

    expectation_type: str = field(init=False, default="expect_column_values_to_be_not_null_when")

    def _validate_pandas(self, df: pd.DataFrame) -> ExpectationResult:
        cond_col = self.kwargs.get("condition_column")
        cond_val = self.kwargs.get("condition_value")
        when = self.kwargs.get("when", "equal")

        if when == "equal":
            condition_mask = df[cond_col] == cond_val
        else:
            condition_mask = df[cond_col] != cond_val

        scoped = df[condition_mask]
        total = len(scoped)
        unexpected_mask = scoped[self.column].isnull()
        unexpected_count = int(unexpected_mask.sum())
        pct = (unexpected_count / total * 100) if total > 0 else 0.0

        return self._build_result(
            success=(unexpected_count == 0),
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            details={"condition": f"{cond_col} {when} {cond_val!r}"},
        )


# ---------------------------------------------------------------------------
# 3. expect_column_values_to_satisfy (user-defined lambda)
# ---------------------------------------------------------------------------


@register_expectation
@dataclass
class ExpectColumnValuesToSatisfy(Expectation):
    """
    Expect each non-null column value to satisfy a custom Python callable.

    IMPORTANT: This expectation only works with the Pandas engine and cannot
    be persisted/loaded from YAML because lambdas are not serializable.

    Usage (Python API only):
        from validatex.expectations.conditional_expectations import ExpectColumnValuesToSatisfy

        exp = ExpectColumnValuesToSatisfy(
            column="price",
            condition=lambda x: x > 0 and x < 1_000_000
        )
    """

    expectation_type: str = field(init=False, default="expect_column_values_to_satisfy")

    def _validate_pandas(self, df: pd.DataFrame) -> ExpectationResult:
        condition: Optional[Callable] = self.kwargs.get("condition")
        if condition is None:
            raise ValueError("You must pass a 'condition' callable to " "ExpectColumnValuesToSatisfy.")

        series = df[self.column].dropna()
        total = len(series)
        try:
            result_mask = series.apply(condition)
        except Exception as e:
            raise ValueError(f"Your condition raised an error: {e}")

        unexpected_mask = ~result_mask
        unexpected_count = int(unexpected_mask.sum())
        pct = (unexpected_count / total * 100) if total > 0 else 0.0

        return self._build_result(
            success=(unexpected_count == 0),
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            unexpected_values=series[unexpected_mask].tolist()[:20],
            details={"condition": str(condition)},
        )
