"""SQLite 原始库：批次状态、检测记录、收件回执、演示开关。

并发安全设计：
- batches.batch_dir 与 results.(batch_id, sample_no, test_item) 均有唯一约束；
- 整批入库在单个事务内完成，冲突时回滚，天然幂等；
- WAL 模式 + busy_timeout，保证 API 与后台扫描线程并发读写不丢不重。
"""
from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Sequence

from .config import settings

SCHEMA = """
CREATE TABLE IF NOT EXISTS batches (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_dir       TEXT NOT NULL UNIQUE,
    status          TEXT NOT NULL CHECK (status IN ('INGESTED','ARCHIVED','REJECTED')),
    record_count    INTEGER NOT NULL DEFAULT 0,
    receipt_no      TEXT,
    csv_checksum    TEXT,
    archive_path    TEXT,
    archive_attempts INTEGER NOT NULL DEFAULT 0,
    error_reasons   TEXT,
    discovered_at   TEXT NOT NULL,
    ingested_at     TEXT,
    archived_at     TEXT,
    rejected_at     TEXT,
    updated_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS results (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id     INTEGER NOT NULL REFERENCES batches(id),
    sample_no    TEXT NOT NULL,
    test_item    TEXT NOT NULL,
    result_value TEXT NOT NULL,
    row_no       INTEGER NOT NULL,
    UNIQUE (batch_id, sample_no, test_item)
);

CREATE TABLE IF NOT EXISTS receipts (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id     INTEGER NOT NULL UNIQUE REFERENCES batches(id),
    receipt_no   TEXT NOT NULL UNIQUE,
    batch_dir    TEXT NOT NULL,
    record_count INTEGER NOT NULL,
    checksum     TEXT NOT NULL,
    created_at   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS app_settings (
    key        TEXT PRIMARY KEY,
    value      TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
"""


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def init_db(db_path: str | None = None) -> None:
    path = Path(db_path or settings.db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with connect(db_path) as conn:
        conn.executescript(SCHEMA)
        conn.execute(
            "INSERT OR IGNORE INTO app_settings(key, value, updated_at) "
            "VALUES ('archive_refused', 'false', ?)",
            (utcnow(),),
        )


@contextmanager
def connect(db_path: str | None = None) -> Iterator[sqlite3.Connection]:
    path = Path(db_path or settings.db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
    finally:
        conn.close()


# ------------------------- app_settings（演示开关） -------------------------

def get_flag(conn: sqlite3.Connection, key: str, default: str = "false") -> bool:
    row = conn.execute(
        "SELECT value FROM app_settings WHERE key=?", (key,)
    ).fetchone()
    return (row["value"] if row else default).lower() == "true"


def set_flag(conn: sqlite3.Connection, key: str, value: bool) -> None:
    conn.execute(
        "INSERT INTO app_settings(key, value, updated_at) VALUES (?,?,?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value, "
        "updated_at=excluded.updated_at",
        (key, "true" if value else "false", utcnow()),
    )


# ------------------------------ 批次与记录 ------------------------------

def insert_batch_skeleton(conn: sqlite3.Connection, batch_dir: str) -> bool:
    """登记新发现的批次（INGESTED 占位，记录数 0）。

    返回 True 表示本次登记成功；False 表示批次已存在（并发/重启去重）。
    """
    try:
        conn.execute(
            "INSERT INTO batches(batch_dir, status, discovered_at, updated_at) "
            "VALUES (?, 'INGESTED', ?, ?)",
            (batch_dir, utcnow(), utcnow()),
        )
        return True
    except sqlite3.IntegrityError:
        return False


def batch_row(conn: sqlite3.Connection, batch_dir: str) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM batches WHERE batch_dir=?", (batch_dir,)
    ).fetchone()


def commit_batch(
    conn: sqlite3.Connection,
    batch_id: int,
    records: Sequence[dict[str, Any]],
    receipt_no: str,
    checksum: str,
) -> None:
    """单事务写入整批记录 + 回执，并把批次标记为完整 INGESTED。"""
    now = utcnow()
    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.executemany(
            "INSERT INTO results(batch_id, sample_no, test_item, result_value, row_no) "
            "VALUES (:batch_id, :sample_no, :test_item, :result_value, :row_no)",
            [
                {
                    "batch_id": batch_id,
                    "sample_no": r["sample_no"],
                    "test_item": r["test_item"],
                    "result_value": r["result_value"],
                    "row_no": r["row_no"],
                }
                for r in records
            ],
        )
        conn.execute(
            "INSERT INTO receipts(batch_id, receipt_no, batch_dir, record_count, "
            "checksum, created_at) VALUES (?,?,?,?,?,?)",
            (
                batch_id,
                receipt_no,
                conn.execute(
                    "SELECT batch_dir FROM batches WHERE id=?", (batch_id,)
                ).fetchone()["batch_dir"],
                len(records),
                checksum,
                now,
            ),
        )
        conn.execute(
            "UPDATE batches SET status='INGESTED', record_count=?, receipt_no=?, "
            "csv_checksum=?, ingested_at=?, updated_at=? WHERE id=?",
            (len(records), receipt_no, checksum, now, now, batch_id),
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def reject_batch(conn: sqlite3.Connection, batch_id: int, reasons: list[str]) -> None:
    now = utcnow()
    conn.execute(
        "UPDATE batches SET status='REJECTED', error_reasons=?, rejected_at=?, "
        "updated_at=? WHERE id=?",
        (json.dumps(reasons, ensure_ascii=False), now, now, batch_id),
    )


def mark_archived(
    conn: sqlite3.Connection, batch_id: int, archive_path: str
) -> None:
    now = utcnow()
    conn.execute(
        "UPDATE batches SET status='ARCHIVED', archive_path=?, archived_at=?, "
        "updated_at=? WHERE id=?",
        (archive_path, now, now, batch_id),
    )


def bump_archive_attempt(
    conn: sqlite3.Connection, batch_id: int, error: str
) -> None:
    conn.execute(
        "UPDATE batches SET archive_attempts=archive_attempts+1, error_reasons=?, "
        "updated_at=? WHERE id=?",
        (json.dumps([error], ensure_ascii=False), utcnow(), batch_id),
    )


def list_batches(
    conn: sqlite3.Connection, status: str | None, limit: int, offset: int
) -> list[sqlite3.Row]:
    sql = "SELECT * FROM batches"
    params: list[Any] = []
    if status:
        sql += " WHERE status=?"
        params.append(status)
    sql += " ORDER BY id DESC LIMIT ? OFFSET ?"
    params.extend([limit, offset])
    return list(conn.execute(sql, params).fetchall())


def count_batches(conn: sqlite3.Connection, status: str | None) -> int:
    if status:
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM batches WHERE status=?", (status,)
        ).fetchone()
    else:
        row = conn.execute("SELECT COUNT(*) AS c FROM batches").fetchone()
    return int(row["c"])


def pending_archive_batches(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    """已入库但未归档的批次（含归档失败待重试、崩溃后待接续）。"""
    return list(
        conn.execute(
            "SELECT * FROM batches WHERE status='INGESTED' "
            "AND receipt_no IS NOT NULL ORDER BY id"
        ).fetchall()
    )


def list_results(
    conn: sqlite3.Connection,
    batch_dir: str | None,
    sample_no: str | None,
    test_item: str | None,
    limit: int,
    offset: int,
) -> list[sqlite3.Row]:
    sql = (
        "SELECT r.id, r.batch_id, b.batch_dir, r.sample_no, r.test_item, "
        "r.result_value, r.row_no FROM results r JOIN batches b ON b.id=r.batch_id "
        "WHERE 1=1"
    )
    params: list[Any] = []
    if batch_dir:
        sql += " AND b.batch_dir=?"
        params.append(batch_dir)
    if sample_no:
        sql += " AND r.sample_no=?"
        params.append(sample_no)
    if test_item:
        sql += " AND r.test_item=?"
        params.append(test_item)
    sql += " ORDER BY r.id LIMIT ? OFFSET ?"
    params.extend([limit, offset])
    return list(conn.execute(sql, params).fetchall())


def count_results(
    conn: sqlite3.Connection,
    batch_dir: str | None,
    sample_no: str | None,
    test_item: str | None,
) -> int:
    sql = "SELECT COUNT(*) AS c FROM results r JOIN batches b ON b.id=r.batch_id WHERE 1=1"
    params: list[Any] = []
    if batch_dir:
        sql += " AND b.batch_dir=?"
        params.append(batch_dir)
    if sample_no:
        sql += " AND r.sample_no=?"
        params.append(sample_no)
    if test_item:
        sql += " AND r.test_item=?"
        params.append(test_item)
    return int(conn.execute(sql, params).fetchone()["c"])


def get_receipt(conn: sqlite3.Connection, batch_id: int) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM receipts WHERE batch_id=?", (batch_id,)
    ).fetchone()
