"""
Unit tests for validatex.storage.run_store.RunStore
"""

import pandas as pd
import validatex as vx
from validatex.storage.run_store import RunStore

def test_run_store_logging_and_history(tmp_path):
    db_file = str(tmp_path / "test_runs.db")
    store = RunStore(db_path=db_file)

    df = pd.DataFrame({"user_id": [1, 2, 3], "age": [25, 30, 35]})
    suite = vx.ExpectationSuite("test_suite").add("expect_column_to_not_be_null", column="user_id")

    result = vx.validate(df, suite)
    run_id = store.log_run(result)

    assert run_id > 0

    history = store.get_history(suite_name="test_suite")
    assert len(history) == 1
    assert history[0]["suite_name"] == "test_suite"
    assert history[0]["quality_score"] == 100.0

    # Also test helper method on ValidationResult
    run_id_2 = result.save_to_store(store_path=db_file)
    assert run_id_2 > run_id
