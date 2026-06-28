"""
ValidateX Storage — Lightweight Time-Series RunStore.

Provides zero-overhead, local SQLite indexing for validation run metrics, enabling
30-day historical trend tracking without parsing raw report files.
"""

from __future__ import annotations

import os
import sqlite3
from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, List, Optional

if TYPE_CHECKING:
    from validatex.core.result import ValidationResult


class RunStore:
    """
    Lightweight SQLite catalog for recording and querying validation run metrics.

    Examples
    --------
    >>> store = RunStore()
    >>> store.log_run(result)
    >>> history = store.get_history(suite_name="user_quality", limit=30)
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

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._get_connection() as conn:
            conn.execute(
                """
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
                """
            )
            conn.commit()

    def log_run(self, result: "ValidationResult") -> int:
        """Record a validation run into the store."""
        stats = result.compute_statistics()
        timestamp = result.run_time.isoformat() if result.run_time else datetime.now().isoformat()

        with self._get_connection() as conn:
            cursor = conn.execute(
                """
                INSERT INTO validation_runs (
                    suite_name, timestamp, quality_score, total_expectations,
                    passed, failed, errors, success_percent, engine, data_source, duration_seconds
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                ),
            )
            conn.commit()
            return cursor.lastrowid or 0

    def get_history(self, suite_name: Optional[str] = None, limit: int = 30) -> List[Dict[str, Any]]:
        """Query historical run records."""
        with self._get_connection() as conn:
            if suite_name:
                cursor = conn.execute(
                    """
                    SELECT * FROM validation_runs
                    WHERE suite_name = ?
                    ORDER BY run_id DESC LIMIT ?
                    """,
                    (suite_name, limit),
                )
            else:
                cursor = conn.execute(
                    """
                    SELECT * FROM validation_runs
                    ORDER BY run_id DESC LIMIT ?
                    """,
                    (limit,),
                )
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
