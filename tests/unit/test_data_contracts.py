"""Phase 2 - Data Contract Engine tests."""

from __future__ import annotations

import pandas as pd
import pytest
from sqlalchemy import create_engine

import validatex as vx
from validatex.contracts import (
    BREAKING,
    INFO,
    NON_BREAKING,
    ContractError,
    ContractRegistry,
    DataContract,
    compare_contracts,
    normalize_version,
    parse_version,
    validate_contract,
    version_bump,
)

try:
    import polars as pl

    HAS_POLARS = True
except ImportError:  # pragma: no cover
    HAS_POLARS = False

CONTRACT_YAML = r"""
contract:
  name: customer_master
  version: 1.0

  owner:
    team: customer_data
    steward: customer_operations

  schema:
    - name: customer_id
      type: integer
      nullable: false

    - name: email
      type: string
      nullable: false

  quality_rules:
    - expectation: expect_column_values_to_be_unique
      column: customer_id
      severity: critical

    - expectation: expect_column_values_to_match_regex
      column: email
      severity: warning
      regex: '^[^@\s]+@[^@\s]+\.[^@\s]+$'
      owner: crm_team

  thresholds:
    minimum_quality_score: 95
"""


def good_df() -> pd.DataFrame:
    return pd.DataFrame({"customer_id": [1, 2, 3, 4], "email": ["a@x.com", "b@x.com", "c@x.com", "d@x.com"]})


def base_contract(version: str = "1.0.0") -> DataContract:
    return (
        DataContract.create("orders", version, owner={"team": "sales_data"})
        .add_field("order_id", "integer", nullable=False)
        .add_field("amount", "float")
        .add_field("status", "string")
        .add_rule("expect_column_values_to_be_unique", column="order_id", severity="critical")
        .add_rule(
            "expect_column_values_to_be_between",
            column="amount",
            severity="warning",
            min_value=0,
            max_value=1000,
        )
    )


# ---------------------------------------------------------------------------
# Creation, versions, serialisation
# ---------------------------------------------------------------------------


def test_contract_from_yaml_matches_spec_example():
    c = DataContract.from_yaml(CONTRACT_YAML)
    assert c.name == "customer_master"
    assert c.version == "1.0.0"  # YAML float 1.0 is normalised to semver
    assert c.owner.team == "customer_data" and c.owner.steward == "customer_operations"
    assert [f.name for f in c.schema] == ["customer_id", "email"]
    assert c.get_field("customer_id").nullable is False
    assert [r.severity for r in c.quality_rules] == ["critical", "warning"]
    assert c.quality_rules[1].kwargs == {"regex": r"^[^@\s]+@[^@\s]+\.[^@\s]+$"}
    assert c.thresholds.minimum_quality_score == 95
    assert c.validate_definition() == []


def test_contract_creation_api_and_roundtrip(tmp_path):
    c = base_contract()
    path = c.save(tmp_path / "orders.yaml")
    loaded = DataContract.load(path)
    assert loaded.to_dict() == c.to_dict()
    assert loaded.content_hash == c.content_hash

    json_path = c.save(tmp_path / "orders.json")
    assert DataContract.load(json_path).content_hash == c.content_hash


def test_content_hash_changes_on_edit():
    a, b = base_contract(), base_contract()
    assert a.content_hash == b.content_hash
    b.add_field("extra", "string")
    assert a.content_hash != b.content_hash


@pytest.mark.parametrize(
    "raw,expected",
    [("1", "1.0.0"), (1.0, "1.0.0"), ("1.2", "1.2.0"), ("2.3.4", "2.3.4"), ("v3.1", "3.1.0")],
)
def test_version_normalisation(raw, expected):
    assert normalize_version(raw) == expected


@pytest.mark.parametrize("bad", ["abc", "1.2.3.4", "", "1.x"])
def test_invalid_versions_rejected(bad):
    with pytest.raises(ContractError):
        parse_version(bad)


def test_version_bump_classification():
    assert version_bump("1.0.0", "2.0.0") == "major"
    assert version_bump("1.0.0", "1.1.0") == "minor"
    assert version_bump("1.0.0", "1.0.1") == "patch"
    assert version_bump("1.1.0", "1.0.0") == "none"
    assert version_bump("1.0.0", "1.0.0") == "none"


def test_malformed_contracts_raise():
    with pytest.raises(ContractError):
        DataContract.from_dict({"contract": {"version": "1.0"}})  # no name
    with pytest.raises(ContractError):
        DataContract.from_dict({"name": "x", "bogus_field": 1})
    with pytest.raises(ContractError):
        DataContract.from_yaml("name: [unclosed")
    with pytest.raises(ContractError):
        DataContract.load("does/not/exist.yaml")


def test_definition_validation_reports_all_problems():
    c = DataContract.create("bad name!", "1.0")
    c.add_field("a", "integer").add_field("a", "string").add_field("b", "wibble")
    c.add_rule("expect_nothing_real", column="a")
    c.add_rule("expect_column_to_not_be_null", column="a", severity="urgent")
    c.add_rule("expect_column_to_not_be_null", column="zzz")
    c.thresholds.minimum_quality_score = 150
    problems = "\n".join(c.validate_definition())
    assert "Invalid contract name" in problems
    assert "Duplicate schema field 'a'" in problems
    assert "unknown type 'wibble'" in problems
    assert "unknown expectation 'expect_nothing_real'" in problems
    assert "invalid severity 'urgent'" in problems
    assert "column 'zzz' that is not in the schema" in problems
    assert "between 0 and 100" in problems
    with pytest.raises(ContractError):
        c.ensure_valid()


def test_to_suite_carries_severity_owner_and_rule_ids():
    c = DataContract.from_yaml(CONTRACT_YAML)
    suite = c.to_suite()
    types = [e.expectation_type for e in suite]
    assert types.count("expect_column_to_not_be_null") == 2
    assert "expect_column_values_to_be_unique" in types
    unique = next(e for e in suite if e.expectation_type == "expect_column_values_to_be_unique")
    assert unique.meta["severity"] == "critical"
    assert unique.meta["owner"] == "customer_data"  # defaults to the contract team
    regex = next(e for e in suite if e.expectation_type == "expect_column_values_to_match_regex")
    assert regex.meta["owner"] == "crm_team"  # rule-level ownership override
    assert suite.meta["contract"] == "customer_master"
    assert suite.version == "1.0.0"


def test_rule_ids_are_unique_even_for_repeated_rules():
    c = DataContract.create("t").add_field("x", "integer")
    c.add_rule("expect_column_values_to_be_between", column="x", min_value=0)
    c.add_rule("expect_column_values_to_be_between", column="x", max_value=9)
    ids = c.rule_ids()
    assert len(set(ids)) == 2


# ---------------------------------------------------------------------------
# Validation: schema, business rules, thresholds
# ---------------------------------------------------------------------------


def test_clean_data_passes_contract():
    result = validate_contract(good_df(), DataContract.from_yaml(CONTRACT_YAML))
    assert result.success is True
    assert result.quality_score == 100.0
    assert result.schema_violations == [] and result.rule_violations == []
    assert result.validation.suite_meta["contract"] == "customer_master"
    assert any(r.expectation_type == "expect_column_values_to_be_of_type" for r in result.schema_results)


def test_accepts_path_and_dict_inputs(tmp_path):
    path = tmp_path / "c.yaml"
    path.write_text(CONTRACT_YAML, encoding="utf-8")
    assert validate_contract(good_df(), str(path)).success
    assert validate_contract(good_df(), DataContract.from_yaml(CONTRACT_YAML).to_dict()).success
    with pytest.raises(ContractError):
        validate_contract(good_df(), 12345)


def test_missing_column_is_critical_schema_violation():
    df = good_df().drop(columns=["email"])
    result = validate_contract(df, DataContract.from_yaml(CONTRACT_YAML))
    assert result.success is False
    missing = [r for r in result.schema_violations if r.expectation_type == "expect_column_to_exist"]
    assert [r.column for r in missing] == ["email"]
    assert missing[0].severity == "critical"
    assert result.failures_by_severity()["critical"] >= 1
    # The type check is skipped for a missing column, so only the existence check explains the schema failure.
    assert not any(
        r.expectation_type == "expect_column_values_to_be_of_type" and r.column == "email"
        for r in result.schema_results
    )


def test_type_mismatch_detected():
    df = good_df().assign(customer_id=["a", "b", "c", "d"])
    result = validate_contract(df, DataContract.from_yaml(CONTRACT_YAML))
    bad = [r for r in result.schema_violations if r.expectation_type == "expect_column_values_to_be_of_type"]
    assert [r.column for r in bad] == ["customer_id"]
    assert bad[0].details["expected_type"] == "integer"
    assert bad[0].details["actual_logical_type"] == "string"
    assert result.success is False


def test_integer_column_with_nulls_is_accepted_as_integer():
    c = DataContract.create("t").add_field("n", "integer")
    df = pd.DataFrame({"n": [1.0, 2.0, None]})  # pandas widens ints to float when nulls are present
    assert validate_contract(df, c).success is True
    fractional = pd.DataFrame({"n": [1.5, 2.0, None]})
    assert validate_contract(fractional, c).success is False


def test_integer_widens_to_float_but_not_the_reverse():
    c_float = DataContract.create("t").add_field("v", "float")
    assert validate_contract(pd.DataFrame({"v": [1, 2]}), c_float).success is True
    c_int = DataContract.create("t").add_field("v", "integer")
    assert validate_contract(pd.DataFrame({"v": [1.5, 2.5]}), c_int).success is False


def test_strict_schema_flags_undeclared_columns():
    c = DataContract.from_yaml(CONTRACT_YAML)
    c.strict_schema = True
    df = good_df().assign(surprise=1)
    result = validate_contract(df, c)
    strict = [r for r in result.schema_violations if r.expectation_type == "expect_table_columns_to_match_set"]
    assert strict and strict[0].details["unexpected_columns"] == ["surprise"]
    assert validate_contract(good_df(), c).success is True


def test_nullable_false_is_enforced():
    df = good_df()
    df.loc[1, "email"] = None
    result = validate_contract(df, DataContract.from_yaml(CONTRACT_YAML))
    failed = [r for r in result.violations if r.expectation_type == "expect_column_to_not_be_null"]
    assert [r.column for r in failed] == ["email"]
    assert result.success is False  # a critical failure exceeds max_critical_failures=0


def test_critical_rule_failure_blocks_even_with_high_score():
    c = DataContract.from_yaml(CONTRACT_YAML)
    c.thresholds.minimum_quality_score = 10  # score alone would pass
    df = good_df().assign(customer_id=[1, 1, 2, 3])
    result = validate_contract(df, c)
    assert result.quality_score > 10
    assert result.success is False
    crit = next(t for t in result.threshold_checks if t.name == "max_critical_failures")
    assert crit.passed is False and crit.actual == 1


def test_warning_failure_allowed_when_score_meets_threshold():
    df = good_df().assign(email=["a@x.com", "b@x.com", "c@x.com", "not-an-email"])
    c = DataContract.from_yaml(CONTRACT_YAML)
    c.thresholds.minimum_quality_score = 90
    ok = validate_contract(df, c)
    assert len(ok.rule_violations) == 1 and ok.rule_violations[0].severity == "warning"
    assert 90 <= ok.quality_score < 100
    assert ok.success is True

    c.thresholds.minimum_quality_score = 95  # the spec's example threshold
    strict = validate_contract(df, c)
    assert strict.success is False
    assert next(t for t in strict.threshold_checks if t.name == "minimum_quality_score").passed is False


def test_max_warning_failures_threshold():
    df = good_df().assign(email=["x", "y", "c@x.com", "d@x.com"])
    c = DataContract.from_yaml(CONTRACT_YAML)
    c.thresholds.minimum_quality_score = None
    c.thresholds.max_warning_failures = 0
    assert validate_contract(df, c).success is False
    c.thresholds.max_warning_failures = 1
    assert validate_contract(df, c).success is True


def test_rule_ownership_routes_failures():
    df = good_df().assign(customer_id=[1, 1, 2, 3], email=["a@x.com", "b@x.com", "c@x.com", "bad"])
    result = validate_contract(df, DataContract.from_yaml(CONTRACT_YAML))
    by_owner = result.failures_by_owner()
    assert {r.expectation_type for r in by_owner["customer_data"]} == {"expect_column_values_to_be_unique"}
    assert {r.expectation_type for r in by_owner["crm_team"]} == {"expect_column_values_to_match_regex"}


def test_contract_result_serialisation_and_summary(tmp_path):
    result = validate_contract(good_df().assign(customer_id=[1, 1, 2, 3]), DataContract.from_yaml(CONTRACT_YAML))
    d = result.to_dict()
    assert d["contract"]["name"] == "customer_master" and d["contract"]["version"] == "1.0.0"
    assert d["success"] is False
    assert d["failures_by_owner"] == {"customer_data": 1}
    assert "customer_master" in result.summary() and "FAILED" in result.summary()
    result.to_json_file(str(tmp_path / "r.json"))
    result.to_html(str(tmp_path / "r.html"))
    assert (tmp_path / "r.json").stat().st_size > 0 and (tmp_path / "r.html").stat().st_size > 0


@pytest.mark.skipif(not HAS_POLARS, reason="Polars not installed")
def test_contract_validation_on_polars():
    c = DataContract.from_yaml(CONTRACT_YAML)
    assert validate_contract(pl.from_pandas(good_df()), c, engine="polars").success is True
    bad = pl.from_pandas(good_df().assign(customer_id=[1, 1, 2, 3]))
    assert validate_contract(bad, c, engine="polars").success is False
    missing = pl.from_pandas(good_df().drop(columns=["email"]))
    assert validate_contract(missing, c, engine="polars").schema_violations


def test_contract_validation_on_sql_engine():
    engine = create_engine("sqlite:///:memory:")
    good_df().to_sql("customers", engine, index=False)
    c = DataContract.from_yaml(CONTRACT_YAML)
    ok = validate_contract("customers", c, engine="sql", sql_engine=engine)
    assert ok.success is True
    types = [r for r in ok.schema_results if r.expectation_type == "expect_column_values_to_be_of_type"]
    assert all(r.details["verified"] for r in types)

    good_df().assign(customer_id=[1, 1, 2, 3]).to_sql("dupes", engine, index=False)
    assert validate_contract("dupes", c, engine="sql", sql_engine=engine).success is False

    # Schema-level checks also work for ad-hoc queries (types cannot be introspected there).
    q = validate_contract("SELECT customer_id, email FROM customers", c, engine="sql", sql_engine=engine)
    assert q.success is True


# ---------------------------------------------------------------------------
# Contract comparison & breaking-change detection
# ---------------------------------------------------------------------------


def impacts(diff, kind):
    return [c.impact for c in diff.changes if c.kind == kind]


def test_identical_contracts_have_no_changes():
    diff = compare_contracts(base_contract(), base_contract())
    assert diff.is_identical and not diff.has_breaking_changes
    assert diff.required_bump == "none" and diff.version_ok


def test_removed_column_is_breaking():
    new = base_contract("2.0.0")
    new.schema = [f for f in new.schema if f.name != "status"]
    diff = compare_contracts(base_contract(), new)
    assert impacts(diff, "field_removed") == [BREAKING]
    assert diff.required_bump == "major" and diff.version_ok


def test_added_column_is_non_breaking():
    new = base_contract("1.1.0").add_field("channel", "string")
    diff = compare_contracts(base_contract(), new)
    assert impacts(diff, "field_added") == [NON_BREAKING]
    assert not diff.has_breaking_changes and diff.required_bump == "minor" and diff.version_ok


def test_type_changes_widening_vs_incompatible():
    new = base_contract("1.1.0")
    new.get_field("order_id").type = "float"  # integer -> float is a widening
    assert impacts(compare_contracts(base_contract(), new), "type_changed") == [NON_BREAKING]

    narrowed = base_contract("2.0.0")
    narrowed.get_field("amount").type = "integer"  # float -> integer loses information
    assert impacts(compare_contracts(base_contract(), narrowed), "type_changed") == [BREAKING]

    stringy = base_contract("2.0.0")
    stringy.get_field("amount").type = "string"
    assert impacts(compare_contracts(base_contract(), stringy), "type_changed") == [BREAKING]

    untyped = base_contract("2.0.0")
    untyped.get_field("amount").type = "any"  # dropping the type guarantee
    assert impacts(compare_contracts(base_contract(), untyped), "type_changed") == [BREAKING]


def test_nullability_changes():
    relaxed = base_contract("2.0.0")
    relaxed.get_field("order_id").nullable = True
    assert impacts(compare_contracts(base_contract(), relaxed), "became_nullable") == [BREAKING]

    tightened = base_contract("1.1.0")
    tightened.get_field("amount").nullable = False
    assert impacts(compare_contracts(base_contract(), tightened), "became_required") == [NON_BREAKING]


def test_rule_removed_added_and_severity_changes():
    removed = base_contract("2.0.0")
    removed.quality_rules = removed.quality_rules[:1]
    assert impacts(compare_contracts(base_contract(), removed), "rule_removed") == [BREAKING]

    added = base_contract("1.1.0").add_rule("expect_column_to_not_be_null", column="status")
    assert impacts(compare_contracts(base_contract(), added), "rule_added") == [NON_BREAKING]

    down = base_contract("2.0.0")
    down.quality_rules[0].severity = "warning"
    assert impacts(compare_contracts(base_contract(), down), "severity_downgraded") == [BREAKING]

    up = base_contract("1.1.0")
    up.quality_rules[1].severity = "critical"
    assert impacts(compare_contracts(base_contract(), up), "severity_upgraded") == [NON_BREAKING]


@pytest.mark.parametrize(
    "key,old,new,expected",
    [
        ("min_value", 0, 10, NON_BREAKING),  # lower bound raised: stricter
        ("min_value", 0, -5, BREAKING),  # lower bound lowered: looser
        ("max_value", 1000, 500, NON_BREAKING),
        ("max_value", 1000, 5000, BREAKING),
        ("max_value", 1000, None, BREAKING),  # bound removed
        ("min_value", None, 0, NON_BREAKING),  # bound introduced
    ],
)
def test_range_rule_argument_changes(key, old, new, expected):
    new_contract = base_contract("1.1.0")
    old_contract = base_contract()
    old_contract.quality_rules[1].kwargs[key] = old
    new_contract.quality_rules[1].kwargs[key] = new
    if new is None:
        del new_contract.quality_rules[1].kwargs[key]
    diff = compare_contracts(old_contract, new_contract)
    assert impacts(diff, "rule_argument_changed") == [expected]


def test_value_set_changes_respect_rule_polarity():
    def contract(expectation, values):
        return DataContract.create("t").add_rule(expectation, column="s", value_set=values)

    allow = "expect_column_values_to_be_in_set"
    forbid = "expect_column_values_to_not_be_in_set"
    assert impacts(
        compare_contracts(contract(allow, ["a", "b"]), contract(allow, ["a", "b", "c"])), "rule_argument_changed"
    ) == [BREAKING]
    assert impacts(compare_contracts(contract(allow, ["a", "b"]), contract(allow, ["a"])), "rule_argument_changed") == [
        NON_BREAKING
    ]
    assert impacts(
        compare_contracts(contract(forbid, ["x"]), contract(forbid, ["x", "y"])), "rule_argument_changed"
    ) == [NON_BREAKING]
    assert impacts(
        compare_contracts(contract(forbid, ["x", "y"]), contract(forbid, ["x"])), "rule_argument_changed"
    ) == [BREAKING]


def test_threshold_changes():
    base = base_contract()
    base.thresholds.minimum_quality_score = 95
    lowered = base_contract("2.0.0")
    lowered.thresholds.minimum_quality_score = 80
    assert impacts(compare_contracts(base, lowered), "minimum_quality_score_changed") == [BREAKING]

    raised = base_contract("1.1.0")
    raised.thresholds.minimum_quality_score = 99
    assert impacts(compare_contracts(base, raised), "minimum_quality_score_changed") == [NON_BREAKING]

    more_critical = base_contract("2.0.0")
    more_critical.thresholds.minimum_quality_score = 95
    more_critical.thresholds.max_critical_failures = 3
    assert impacts(compare_contracts(base, more_critical), "max_critical_failures_changed") == [BREAKING]


def test_owner_and_description_changes_are_informational():
    new = base_contract("1.0.1")
    new.owner.steward = "someone_else"
    new.description = "now documented"
    diff = compare_contracts(base_contract(), new)
    assert {c.impact for c in diff.changes} == {INFO}
    assert diff.required_bump == "patch" and diff.version_ok


def test_insufficient_version_bump_is_flagged():
    new = base_contract("1.1.0")  # minor bump...
    new.schema = [f for f in new.schema if f.name != "status"]  # ...but a breaking removal
    diff = compare_contracts(base_contract(), new)
    assert diff.required_bump == "major" and diff.actual_bump == "minor"
    assert diff.version_ok is False
    assert "VERSION BUMP TOO SMALL" in diff.summary()
    assert diff.to_dict()["breaking"] is True
    assert "| breaking |" in diff.to_markdown()


# ---------------------------------------------------------------------------
# Registry (versioned storage)
# ---------------------------------------------------------------------------


def test_registry_register_get_list_and_latest(tmp_path):
    reg = ContractRegistry(tmp_path / "contracts")
    assert reg.list_contracts() == [] and reg.latest_version("orders") is None
    reg.register(base_contract("1.0.0"))
    reg.register(base_contract("1.1.0").add_field("channel", "string"))
    assert reg.list_contracts() == ["orders"]
    assert reg.versions("orders") == ["1.0.0", "1.1.0"]
    assert reg.get("orders").version == "1.1.0"
    assert reg.get("orders", "1.0").version == "1.0.0"
    assert reg.diff("orders", "1.0.0", "1.1.0").non_breaking_changes


def test_registry_versions_sort_numerically(tmp_path):
    reg = ContractRegistry(tmp_path)
    c = base_contract("1.2.0")
    reg.register(c)
    reg.register(base_contract("1.10.0").add_field("n", "string"))
    assert reg.versions("orders") == ["1.2.0", "1.10.0"]
    assert reg.latest_version("orders") == "1.10.0"


def test_registry_reregistering_same_content_is_idempotent_but_edits_rejected(tmp_path):
    reg = ContractRegistry(tmp_path)
    reg.register(base_contract())
    reg.register(base_contract())  # no-op
    edited = base_contract().add_field("sneaky", "string")
    with pytest.raises(ContractError, match="immutable"):
        reg.register(edited)


def test_registry_rejects_non_increasing_versions(tmp_path):
    reg = ContractRegistry(tmp_path)
    reg.register(base_contract("2.0.0"))
    with pytest.raises(ContractError, match="greater than"):
        reg.register(base_contract("1.5.0"))


def test_registry_blocks_breaking_change_under_minor_bump(tmp_path):
    reg = ContractRegistry(tmp_path)
    reg.register(base_contract("1.0.0"))
    breaking = base_contract("1.1.0")
    breaking.schema = [f for f in breaking.schema if f.name != "status"]
    with pytest.raises(ContractError, match="require a major bump"):
        reg.register(breaking)
    breaking.version = "2.0.0"
    reg.register(breaking)  # same change under a major bump is fine
    assert reg.latest_version("orders") == "2.0.0"


def test_registry_semver_enforcement_can_be_disabled(tmp_path):
    reg = ContractRegistry(tmp_path)
    reg.register(base_contract("1.0.0"))
    breaking = base_contract("1.0.1")
    breaking.schema = []
    reg.register(breaking, enforce_semver=False)
    assert reg.latest_version("orders") == "1.0.1"


def test_registry_rejects_invalid_definitions_and_unknown_lookups(tmp_path):
    reg = ContractRegistry(tmp_path)
    with pytest.raises(ContractError):
        reg.register(DataContract.create("ok").add_rule("expect_not_a_thing"))
    with pytest.raises(ContractError):
        reg.get("missing")
    reg.register(base_contract())
    with pytest.raises(ContractError):
        reg.get("orders", "9.9.9")


def test_vx_namespace_exports():
    assert vx.DataContract is DataContract
    assert vx.validate_contract is validate_contract
    assert vx.compare_contracts is compare_contracts


def test_cli_contract_commands(tmp_path):
    from click.testing import CliRunner
    from validatex.cli.main import cli

    runner = CliRunner()
    v1_path = base_contract("1.0.0").save(tmp_path / "v1.yaml")
    v2_path = base_contract("1.1.0").add_field("channel", "string").save(tmp_path / "v2.yaml")
    data_path = tmp_path / "data.csv"
    pd.DataFrame(
        {"order_id": [1, 2], "amount": [10.0, 20.0], "status": ["paid", "pending"], "channel": ["web", "app"]}
    ).to_csv(data_path, index=False)

    assert runner.invoke(cli, ["contract", "lint", str(v1_path)]).exit_code == 0
    assert (
        runner.invoke(
            cli,
            [
                "contract",
                "validate",
                "-c",
                str(v1_path),
                "-d",
                str(data_path),
                "--store",
                str(tmp_path / "runs.db"),
                "--run-name",
                "ci_check",
            ],
        ).exit_code
        == 0
    )
    diff_res = runner.invoke(cli, ["contract", "diff", str(v1_path), str(v2_path), "--check-version"])
    assert diff_res.exit_code == 0 and "1.0.0 -> 1.1.0" in diff_res.output
    reg_dir = tmp_path / "registry"
    assert runner.invoke(cli, ["contract", "register", str(v1_path), "-r", str(reg_dir)]).exit_code == 0
    assert runner.invoke(cli, ["contract", "register", str(v2_path), "-r", str(reg_dir)]).exit_code == 0
