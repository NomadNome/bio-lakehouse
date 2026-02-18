"""
Bio Insights Engine - Query Log

SQLite-backed log of all NL-to-SQL translations for observability.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path


DEFAULT_DB = Path(__file__).parent.parent.parent / "query_log.db"


class QueryLog:
    """Logs NL-to-SQL queries to a local SQLite database."""

    def __init__(self, db_path: str | Path = None):
        self.db_path = str(db_path or DEFAULT_DB)
        self._ensure_table()

    def _ensure_table(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS query_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT DEFAULT (datetime('now')),
                    natural_language_query TEXT,
                    generated_sql TEXT,
                    execution_time_ms INTEGER,
                    row_count INTEGER,
                    confidence REAL,
                    success INTEGER,
                    error_message TEXT
                )
            """)

    def log(
        self,
        question: str,
        sql: str = "",
        execution_time_ms: int = 0,
        row_count: int = 0,
        confidence: float = 0.0,
        success: bool = True,
        error: str = None,
    ):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """INSERT INTO query_log
                   (natural_language_query, generated_sql, execution_time_ms,
                    row_count, confidence, success, error_message)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (question, sql, execution_time_ms, row_count, confidence,
                 1 if success else 0, error),
            )

    def get_recent(self, limit: int = 20) -> list[dict]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM query_log ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
            return [dict(r) for r in rows]
