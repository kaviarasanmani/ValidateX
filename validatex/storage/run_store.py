"""
ValidateX Storage - SQLite RunStore for historical data quality monitoring.

Every validation run is recorded (run id, dataset, suite version, execution time, expectation counts, quality
score, timestamp), together with each expectation's outcome and the dataset's schema, so quality scores, failure
trends, schema changes and volume anomalies can be analysed over time without parsing raw report files.

Existing databases created by earlier versions are migrated in place.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Tuple

if TYPE_CHECKING:
    from validatex.core.result import ValidationResult
    from validatex.observability.trends import FailureTrend, QualityTrend, SchemaChange

# Columns added after the first release; applied with ALTER TABLE on older databases.
_RUN_MIGRATION_COLUMNS: List[Tuple[str, str]] = [
    ("run_name", "TEXT"),
    ("dataset_name", "TEXT"),
    ("suite_version", "TEXT"),
    ("row_count", "INTEGER"),
    ("schema_hash", "TEXT"),
    ("success", "INTEGER"),
    ("contract_name", "TEXT"),
    ("contract_version", "TEXT"),
]

_DATASET_EXPR = "COALESCE(NULLIF(dataset_name, ''), NULLIF(data_source, ''), suite_name)"


def schema_fingerprint(schema: Dict[str, str]) -> str:
    """Stable hash of a ``{column: dtype}`` mapping (order-insensitive)."""
    canonical = json.dumps(sorted(schema.items()))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]


class RunStore:
    """
    SQLite catalog for recording and querying validation runs.

    Examples
    --------
    >>> store = RunStore()                      # .validatex/runs.db
    >>> result = vx.validate(df, suite, run_name="daily_customer_validation", store=store)
    >>> store.get_runs(dataset_name="customers", limit=30)
    >>> store.quality_trend(dataset_name="customers").direction
    'stable'
    """

    def __init__(self, db_path: Optional[str] = None):
        if db_path is None:
            base_dir = os.path.join(os.getcwd(), ".validatex")
            os.makedirs(base_dir, exist_ok=True)
            db_path = os.path.join(base_dir, "runs.db")
        else:
            dirname = os.path.dirname(db_path)
            if dirname:
                os.makedirs(dirname, exist_ok=True)

        self.db_path = db_path
        self._init_db()

    # -- connection helpers ------------------------------------------------

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    @contextmanager
    def _session(self) -> Iterator[sqlite3.Connection]:
        """Open a connection, commit on success, and always close it (important on Windows)."""
        conn = self._get_connection()
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _init_db(self) -> None:
        with self._session() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS validation_runs (
                    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    suite_name TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    quality_score REAL NOT NULL,
                    total_expectations INTEGER NOT NULL,
                    passed INTEGER NOT NULL,
                    failed INTEGER NOT NULL,
                    errors INTEGER NOT NULL,
                    success_percent REAL NOT NULL,
                    engine TEXT NOT NULL,
                    data_source TEXT,
                    duration_seconds REAL NOT NULL
                )
                """)
            existing = {row["name"] for row in conn.execute("PRAGMA table_info(validation_runs)")}
            for column, ddl in _RUN_MIGRATION_COLUMNS:
                if column not in existing:
                    conn.execute(f"ALTER TABLE validation_runs ADD COLUMN {column} {ddl}")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS expectation_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER NOT NULL,
                    expectation_type TEXT NOT NULL,
                    column_name TEXT,
                    rule_id TEXT,
                    severity TEXT,
                    owner TEXT,
                    status TEXT NOT NULL,
                    success INTEGER NOT NULL,
                    element_count INTEGER,
                    unexpected_count INTEGER,
                    unexpected_percent REAL,
                    error TEXT,
                    FOREIGN KEY (run_id) REFERENCES validation_runs (run_id)
                )
                """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_exp_runs_run ON expectation_runs (run_id)")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS schema_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id INTEGER NOT NULL,
                    dataset_name TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    schema_hash TEXT NOT NULL,
                    columns_json TEXT NOT NULL
                )
                """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_schema_dataset ON schema_snapshots (dataset_name, id)")

    # -- writing -----------------------------------------------------------

    def log_run(
        self,
        result: "ValidationResult",
        run_name: Optional[str] = None,
        dataset_name: Optional[str] = None,
        schema: Optional[Dict[str, str]] = None,
        row_count: Optional[int] = None,
    ) -> int:
        """Record a validation run (and its per-expectation outcomes and schema) and return the new run id."""
        stats = result.compute_statistics()
        timestamp = result.run_time.isoformat() if result.run_time else datetime.now().isoformat()
        run_name = run_name or getattr(result, "run_name", None)
        dataset = (
            dataset_name or getattr(result, "dataset_name", None) or result.data_source or result.suite_name or None
        )
        schema = schema if schema is not None else getattr(result, "schema", None)
        row_count = row_count if row_count is not None else getattr(result, "row_count", None)
        meta = result.suite_meta or {}

        with self._session() as conn:
            cursor = conn.execute(
                """
                INSERT INTO validation_runs (
                    suite_name, timestamp, quality_score, total_expectations,
                    passed, failed, errors, success_percent, engine, data_source, duration_seconds,
                    run_name, dataset_name, suite_version, row_count, schema_hash, success,
                    contract_name, contract_version
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    result.suite_name,
                    timestamp,
                    stats.get("quality_score", 100.0),
                    stats.get("total", 0),
                    stats.get("passed", 0),
                    stats.get("failed", 0),
                    stats.get("errors", 0),
                    stats.get("success_percent", 100.0),
                    result.engine,
                    str(result.data_source or ""),
                    stats.get("run_duration_seconds", 0.0),
                    run_name,
                    dataset,
                    str(meta.get("version")) if meta.get("version") is not None else None,
                    row_count,
                    schema_fingerprint(schema) if schema else None,
                    1 if result.success else 0,
                    meta.get("contract"),
                    meta.get("contract_version"),
                ),
            )
            run_id = int(cursor.lastrowid or 0)

            conn.executemany(
                """
                INSERT INTO expectation_runs (
                    run_id, expectation_type, column_name, rule_id, severity, owner, status, success,
                    element_count, unexpected_count, unexpected_percent, error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        run_id,
                        r.expectation_type,
                        r.column,
                        (r.meta or {}).get("rule_id"),
                        r.severity,
                        (r.meta or {}).get("owner"),
                        r.status,
                        1 if r.success else 0,
                        r.element_count,
                        r.unexpected_count,
                        r.unexpected_percent,
                        r.exception_info,
                    )
                    for r in result.results
                ],
            )

            if schema and dataset:
                fingerprint = schema_fingerprint(schema)
                last = conn.execute(
                    "SELECT schema_hash FROM schema_snapshots WHERE dataset_name = ? ORDER BY id DESC LIMIT 1",
                    (dataset,),
                ).fetchone()
                if last is None or last["schema_hash"] != fingerprint:
                    conn.execute(
                        "INSERT INTO schema_snapshots (run_id, dataset_name, timestamp, schema_hash, columns_json) "
                        "VALUES (?, ?, ?, ?, ?)",
                        (run_id, dataset, timestamp, fingerprint, json.dumps(schema, sort_keys=True)),
                    )
        return run_id

    # -- reading -----------------------------------------------------------

    @staticmethod
    def _normalize(row: sqlite3.Row) -> Dict[str, Any]:
        d = dict(row)
        failed = int(d.get("failed") or 0)
        errors = int(d.get("errors") or 0)
        d["dataset_name"] = d.get("dataset_name") or d.get("data_source") or d.get("suite_name")
        d["execution_time"] = d.get("duration_seconds")
        d["passed_expectations"] = d.get("passed")
        d["failed_expectations"] = failed + errors
        d["errored_expectations"] = errors
        return d

    def get_history(self, suite_name: Optional[str] = None, limit: int = 30) -> List[Dict[str, Any]]:
        """Query historical run records (raw columns, newest first)."""
        with self._session() as conn:
            if suite_name:
                cursor = conn.execute(
                    "SELECT * FROM validation_runs WHERE suite_name = ? ORDER BY run_id DESC LIMIT ?",
                    (suite_name, limit),
                )
            else:
                cursor = conn.execute("SELECT * FROM validation_runs ORDER BY run_id DESC LIMIT ?", (limit,))
            return [dict(row) for row in cursor.fetchall()]

    def get_runs(
        self,
        dataset_name: Optional[str] = None,
        suite_name: Optional[str] = None,
        run_name: Optional[str] = None,
        since: Optional[str] = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """
        Query runs, newest first, with the monitoring field names
        (``run_id, dataset_name, suite_version, execution_time, total_expectations, passed_expectations,
        failed_expectations, quality_score, timestamp``).

        ``since`` is an ISO-8601 timestamp; only runs at or after it are returned.
        """
        clauses: List[str] = []
        params: List[Any] = []
        if dataset_name:
            clauses.append(f"{_DATASET_EXPR} = ?")
            params.append(dataset_name)
        if suite_name:
            clauses.append("suite_name = ?")
            params.append(suite_name)
        if run_name:
            clauses.append("run_name = ?")
            params.append(run_name)
        if since:
            clauses.append("timestamp >= ?")
            params.append(since)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        params.append(limit)
        with self._session() as conn:
            rows = conn.execute(
                f"SELECT * FROM validation_runs {where} ORDER BY run_id DESC LIMIT ?",  # nosec B608
                params,
            ).fetchall()
        return [self._normalize(r) for r in rows]

    def get_run(self, run_id: int) -> Optional[Dict[str, Any]]:
        with self._session() as conn:
            row = conn.execute("SELECT * FROM validation_runs WHERE run_id = ?", (run_id,)).fetchone()
        return self._normalize(row) if row else None

    def latest_run(
        self, dataset_name: Optional[str] = None, suite_name: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        runs = self.get_runs(dataset_name=dataset_name, suite_name=suite_name, limit=1)
        return runs[0] if runs else None

    def get_expectation_results(self, run_id: int) -> List[Dict[str, Any]]:
        """Per-expectation outcomes recorded for one run."""
        with self._session() as conn:
            rows = conn.execute("SELECT * FROM expectation_runs WHERE run_id = ? ORDER BY id", (run_id,)).fetchall()
        return [dict(r) for r in rows]

    def get_row_counts(
        self, dataset_name: str, limit: int = 30, exclude_run_id: Optional[int] = None
    ) -> List[Tuple[int, int]]:
        """``(run_id, row_count)`` pairs for a dataset, oldest first (runs without a row count are skipped)."""
        params: List[Any] = [dataset_name]
        exclude = ""
        if exclude_run_id is not None:
            exclude = "AND run_id != ?"
            params.append(exclude_run_id)
        params.append(limit)
        with self._session() as conn:
            rows = conn.execute(
                f"SELECT run_id, row_count FROM validation_runs WHERE {_DATASET_EXPR} = ? "  # nosec B608
                f"AND row_count IS NOT NULL {exclude} ORDER BY run_id DESC LIMIT ?",
                params,
            ).fetchall()
        return [(int(r["run_id"]), int(r["row_count"])) for r in reversed(rows)]

    def get_schema_snapshots(self, dataset_name: str) -> List[Dict[str, Any]]:
        """Recorded schema versions for a dataset, oldest first (a snapshot is stored only when the schema changes)."""
        with self._session() as conn:
            rows = conn.execute(
                "SELECT * FROM schema_snapshots WHERE dataset_name = ? ORDER BY id", (dataset_name,)
            ).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            d["columns"] = json.loads(d.pop("columns_json"))
            out.append(d)
        return out

    # -- analytics (implemented in validatex.observability.trends) -----------

    def quality_trend(
        self, dataset_name: Optional[str] = None, suite_name: Optional[str] = None, limit: int = 30
    ) -> "QualityTrend":
        """Historical quality scores with summary statistics and trend direction."""
        from validatex.observability.trends import quality_trend

        return quality_trend(self, dataset_name=dataset_name, suite_name=suite_name, limit=limit)

    def failed_expectation_trends(
        self,
        dataset_name: Optional[str] = None,
        suite_name: Optional[str] = None,
        limit: int = 30,
        top: Optional[int] = None,
    ) -> List["FailureTrend"]:
        """Which expectations fail most often, and whether they are getting better or worse."""
        from validatex.observability.trends import failed_expectation_trends

        return failed_expectation_trends(self, dataset_name=dataset_name, suite_name=suite_name, limit=limit, top=top)

    def schema_changes(self, dataset_name: str) -> List["SchemaChange"]:
        """Schema change history for a dataset."""
        from validatex.observability.trends import schema_changes

        return schema_changes(self, dataset_name)
