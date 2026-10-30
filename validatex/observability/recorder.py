"""
Run recorder - the glue between a finished validation and the RunStore / alert policy.
"""

from __future__ import annotations

import os
from typing import Any, Optional

from validatex.core.introspect import get_row_count, get_schema


def resolve_store(store: Any) -> Any:
    """
    Turn the ``store`` argument of :func:`validatex.validate` into a RunStore (or ``None``).

    ``None`` / ``False`` disables recording; ``True`` uses ``./.validatex/runs.db``; a string or path opens that
    SQLite file; an existing :class:`RunStore` is used as-is.
    """
    if store is None or store is False:
        return None
    from validatex.storage.run_store import RunStore

    if store is True:
        return RunStore()
    if isinstance(store, (str, os.PathLike)):
        return RunStore(db_path=os.fspath(store))
    return store


def record_run(
    result: Any,
    target_data: Any,
    engine: str,
    run_name: Optional[str] = None,
    dataset_name: Optional[str] = None,
    store: Any = None,
    alert_policy: Any = None,
) -> Optional[int]:
    """
    Attach run metadata to *result*, log it to the store, and evaluate the alert policy.

    Returns the new run id, or ``None`` when nothing was recorded.
    """
    result.run_name = run_name
    result.dataset_name = dataset_name or result.data_source or result.suite_name

    run_store = resolve_store(store)
    run_id: Optional[int] = None
    if run_store is not None:
        result.schema = get_schema(target_data, engine) or None
        result.row_count = get_row_count(target_data, engine)
        run_id = int(run_store.log_run(result))
        result.run_id = run_id

    if alert_policy is not None:
        result.alerts = alert_policy.evaluate(result, store=run_store)
        if result.alerts and (alert_policy.slack_webhook or alert_policy.teams_webhook):
            from validatex.observability.alerting import dispatch_alerts

            dispatch_alerts(result.alerts, alert_policy.slack_webhook, alert_policy.teams_webhook)
    return run_id
