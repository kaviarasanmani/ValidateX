# ValidateX Data Contracts Guide 📜

ValidateX Data Contracts turn expectation suites into reusable, version-controlled agreements between data producers and data consumers—complete with schema validation, rule ownership, semantic versioning, and automated breaking-change detection.

---

## 1. Defining a Data Contract (YAML / JSON)

```yaml
contract:
  name: customer_master
  version: 1.0.0
  description: Canonical customer master agreement

  owner:
    team: customer_data
    steward: customer_operations
    contact: customer-data@company.com

  schema:
    - name: customer_id
      type: integer
      nullable: false

    - name: email
      type: string
      nullable: false

    - name: status
      type: string
      nullable: true

  quality_rules:
    - expectation: expect_column_values_to_be_unique
      column: customer_id
      severity: critical

    - expectation: expect_column_values_to_match_regex
      column: email
      regex: "^[^@]+@[^@]+\\.[^@]+$"
      severity: warning
      owner: crm_team

  thresholds:
    minimum_quality_score: 95
    max_critical_failures: 0
```

---

## 2. Creating Contracts in Python

```python
import validatex as vx

contract = (
    vx.DataContract.create(
        name="customer_master",
        version="1.0.0",
        team="customer_data",
        steward="customer_operations",
        minimum_quality_score=95,
    )
    .add_field("customer_id", "integer", nullable=False)
    .add_field("email", "string", nullable=False)
    .add_rule("expect_column_values_to_be_unique", column="customer_id", severity="critical")
    .add_rule(
        "expect_column_values_to_match_regex",
        column="email",
        regex=r"^[^@]+@[^@]+\.[^@]+$",
        severity="warning",
        owner="crm_team",
    )
)

contract.save("contracts/customer_master.yaml")
```

---

## 3. Validating Data Against a Contract

`vx.validate_contract()` validates:
1. **Schema existence & logical types** (`integer`, `float`, `string`, `boolean`, `date`, `timestamp`) across Pandas, Polars, PySpark, and Push-Down SQL.
2. **Non-null constraints** (`nullable: false`).
3. **Business quality rules** (`quality_rules`).
4. **Thresholds** (`minimum_quality_score`, `max_critical_failures`, `max_warning_failures`).

```python
result = vx.validate_contract(df, "contracts/customer_master.yaml", strict_schema=False)

print(result.summary())
print("Quality Score:", result.quality_score)
print("Failures by Owner:", result.failures_by_owner())
```

---

## 4. Comparing Contracts & Detecting Breaking Changes

```python
old = vx.DataContract.load("contracts/customer_master_v1.yaml")
new = vx.DataContract.load("contracts/customer_master_v2.yaml")

diff = vx.compare_contracts(old, new)
print(diff.summary())
print("Breaking changes?", diff.has_breaking_changes)
print("Required bump:", diff.required_bump, "| Actual bump:", diff.actual_bump)
print("Version bump valid?", diff.version_ok)
```

### Change Classification Matrix

| Change | Classification | Required Semver Bump |
| :--- | :--- | :--- |
| Column removed | `breaking` | `major` |
| Incompatible type change (`integer` -> `string`, `float` -> `integer`) | `breaking` | `major` |
| Widening type change (`integer` -> `float`, `date` -> `timestamp`) | `non_breaking` | `minor` |
| Nullability relaxed (`nullable: false` -> `true`) | `breaking` | `major` |
| Nullability tightened (`nullable: true` -> `false`) | `non_breaking` | `minor` |
| Quality rule removed or severity downgraded (`critical` -> `warning`) | `breaking` | `major` |
| Range bound loosened or allowed `value_set` expanded | `breaking` | `major` |
| Quality rule added or bound tightened | `non_breaking` | `minor` |
| `minimum_quality_score` lowered | `breaking` | `major` |
| Owner or description updated | `info` | `patch` |

---

## 5. Immutable Version Registry & CLI

```python
registry = vx.ContractRegistry("contracts_registry")
registry.register(contract)  # Enforces immutability and semver bump rules
```

### CLI Commands

```bash
# Lint a contract file
validatex contract lint contracts/customer_master.yaml

# Validate data against a contract
validatex contract validate -c contracts/customer_master.yaml -d data/customers.csv --store .validatex/runs.db

# Compare two contract versions in CI
validatex contract diff v1.yaml v2.yaml --fail-on-breaking --check-version --format markdown

# Register a contract version
validatex contract register contracts/customer_master.yaml -r contracts_registry/
```
