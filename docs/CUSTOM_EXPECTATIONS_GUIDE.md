# Guide to Writing Custom Expectations in ValidateX 🛠️

ValidateX is built for clean extensibility. You can easily add custom domain validation rules without modifying the core package source code.

---

## Step-by-Step Custom Expectation Example

To create a custom expectation, inherit from `Expectation` and decorate your class with `@register_expectation`.

```python
from dataclasses import dataclass, field
import pandas as pd
from validatex.core.expectation import Expectation, register_expectation
from validatex.core.result import ExpectationResult

@register_expectation
@dataclass
class ExpectColumnValuesToBeEven(Expectation):
    """Expect all numbers in a numeric column to be even integers."""

    expectation_type: str = field(
        init=False, default="expect_column_values_to_be_even"
    )

    def _validate_pandas(self, df: pd.DataFrame) -> ExpectationResult:
        series = df[self.column].dropna()
        total = len(series)
        odd_mask = (series % 2) != 0
        unexpected_count = int(odd_mask.sum())
        pct = (unexpected_count / total * 100) if total > 0 else 0.0

        return self._build_result(
            success=(unexpected_count == 0),
            element_count=total,
            unexpected_count=unexpected_count,
            unexpected_percent=pct,
            unexpected_values=series[odd_mask].tolist()[:20],
        )
```

## Using Your Custom Expectation

Once defined, your expectation is automatically registered and ready to use in any `ExpectationSuite`:

```python
import validatex as vx

suite = vx.ExpectationSuite("custom_suite").add("expect_column_values_to_be_even", column="quantity")
result = vx.validate(df, suite)
```
