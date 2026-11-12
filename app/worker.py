"""后台采集工作线程。

处理模型（每批目录全局唯一，READY 出现后内容冻结）：

  发现批次 ──► 校验 CSV ──► 单事务整批入库 + 回执 ──► 远端整目录归档
                │失败                              │失败
                ▼                                  ▼
            REJECTED（留原因）              INGESTED 保留，下轮自动重试

崩溃/并发安全：
- 线程锁 + 单 worker 进程，同一时刻只有一次扫描；
- batches.batch_dir 唯一约束负责跨进程/重启去重，insert 冲突即跳过；
- 入库是单事务：未提交则崩溃后不存在半成品；
- 归档目标已存在视为成功（幂等），归档前数据库已持久化，
  故“入库后/移动前崩溃”重启自动补归档，“移动后/标记前崩溃”重启对账为 ARCHIVED；
- 坏批不入库（只登记批次状态行与原因）、不归档。
"""
from __future__ import annotations

import logging
import threading
import time
import uuid
from typing import Any, Callable

from . import database as db
from .config import settings
from .sftp_client import (
    ArchiveRefusedError,
    RemoteError,
    SftpClient,
    SftpProtocol,
    join_remote,
)
from .validation import validate_csv

logger = logging.getLogger("ingest.worker")


class IngestWorker:
    def __init__(self, client_factory: Callable[[], SftpProtocol] | None = None) -> None:
        # 默认走真实 SFTP；测试可注入 LocalFtpClient 工厂
        self._client_factory = client_factory or SftpClient
        self._client: SftpProtocol | None = None
        self._stop = threading.Event()
        self._wakeup = threading.Event()
        self._scan_lock = threading.Lock()
        self._scan_generation = 0
        self._thread: threading.Thread | None = None
        self.last_summary: dict[str, Any] = {}

    # ----------------------------- 生命周期 -----------------------------

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run_loop, name="ingest-worker", daemon=True
        )
        self._thread.start()
        logger.info("后台采集线程已启动，扫描间隔 %ss", settings.scan_interval_seconds)

    def stop(self) -> None:
        self._stop.set()
        self._wakeup.set()
        if self._thread:
            self._thread.join(timeout=15)
        if self._client is not None:
            self._client.close()
            self._client = None

    def is_alive(self) -> bool:
        return bool(self._thread and self._thread.is_alive())

    def trigger(self, wait: float = 5.0) -> bool:
        """唤醒一次立即扫描（手动触发采集）。

        返回 True 表示等待到了本轮（或恰好正在进行的一轮）扫描完成。
        """
        with self._scan_lock:
            generation = self._scan_generation
        self._wakeup.set()
        if wait <= 0:
            return False
        deadline = time.monotonic() + wait
        while time.monotonic() < deadline:
            with self._scan_lock:
                done = self._scan_generation > generation
            if done:
                return True
            time.sleep(0.1)
        return False

    # ----------------------------- 主循环 -----------------------------

    def _run_loop(self) -> None:
        # 不在启动瞬间扫描：统一由周期定时器或 POST /collect 触发，
        # 避免与启动阶段的开关设置竞争，也给 SFTP 就绪留出窗口
        while not self._stop.is_set():
            self._wakeup.wait(timeout=settings.scan_interval_seconds)
            self._wakeup.clear()
            if self._stop.is_set():
                break
            try:
                self.scan_once()
            except Exception:  # noqa: BLE001 - 工作线程不能因异常退出
                logger.exception("扫描轮次异常，将在下个周期重试")

    def _get_client(self) -> SftpProtocol:
        if self._client is None:
            self._client = self._client_factory()
            self._client.ensure_dirs()
        return self._client

    def scan_once(self) -> dict[str, Any]:
        """执行一轮扫描。并发调用会被锁串行化，不会并行入库/归档。"""
        if not self._scan_lock.acquire(blocking=False):
            return {"skipped": "已有扫描正在执行，本次触发已合并"}
        try:
            summary = {
                "discovered": 0,
                "ingested": 0,
                "rejected": 0,
                "archived": 0,
                "archive_retried_failed": 0,
                "errors": [],
            }
            try:
                client = self._get_client()
            except RemoteError as exc:
                # SFTP 暂不可达：不改动任何批次状态，下轮自动重试
                summary["errors"].append(f"SFTP 连接失败，本轮跳过：{exc}")
                logger.error(summary["errors"][-1])
                self.last_summary = summary
                self._scan_generation += 1
                return summary
            with db.connect() as conn:
                # 1) 先接续历史：所有已入库未归档的批次重试归档（覆盖崩溃恢复）
                self._retry_pending_archives(conn, client, summary)
                # 2) 扫描收件箱的新批次
                self._scan_inbound(conn, client, summary)
            self.last_summary = summary
            self._scan_generation += 1
            logger.info("扫描完成：%s", summary)
            return summary
        finally:
            self._scan_lock.release()

    # ----------------------------- 归档接续 -----------------------------

    def _retry_pending_archives(
        self, conn, client: SftpProtocol, summary: dict[str, Any]
    ) -> None:
        for row in db.pending_archive_batches(conn):
            archived = self._archive(conn, client, row["batch_dir"], row["id"])
            if archived:
                summary["archived"] += 1
            else:
                summary["archive_retried_failed"] += 1

    # ----------------------------- 收件扫描 -----------------------------

    def _scan_inbound(self, conn, client: SftpProtocol, summary: dict[str, Any]) -> None:
        inbound = settings.remote_inbound
        try:
            entries = client.list_dir(inbound)
        except RemoteError as exc:
            summary["errors"].append(f"无法列出收件目录 {inbound}：{exc}")
            logger.error(summary["errors"][-1])
            return

        for name in entries:
            if self._stop.is_set():
                break
            batch_dir = name  # 批次目录名（全局唯一）
            batch_path = join_remote(inbound, name)

            # READY 未出现：实验室可能还在投递，跳过等下轮
            if not client.exists(join_remote(batch_path, "READY")):
                continue
            summary["discovered"] += 1

            # 登记批次（唯一约束去重：并发触发/重启后已存在则跳过处理）
            if not db.insert_batch_skeleton(conn, batch_dir):
                existing = db.batch_row(conn, batch_dir)
                if existing and existing["status"] == "INGESTED" and existing["receipt_no"]:
                    # 入库后还没归档（可能是上轮归档失败或进程中断），补归档
                    if self._archive(conn, client, batch_dir, existing["id"]):
                        summary["archived"] += 1
                elif existing and existing["status"] == "ARCHIVED":
                    # 异常：已归档的全局唯一名再次出现在收件箱。
                    # 保留现场、不删除不入库，记录原因供人工核查
                    msg = (
                        f"批次 {batch_dir} 已归档至 {existing['archive_path']}，"
                        "收件箱却再次出现同名目录（违反全局唯一约定），已保留现场待核查"
                    )
                    summary["errors"].append(msg)
                    logger.error(msg)
                continue

            row = db.batch_row(conn, batch_dir)
            batch_id = int(row["id"])

            # 结构检查：results.csv 必须存在
            csv_path = join_remote(batch_path, "results.csv")
            if not client.exists(csv_path):
                db.reject_batch(conn, batch_id, ["批次目录缺少 results.csv"])
                summary["rejected"] += 1
                continue

            try:
                raw = client.read_file(csv_path)
            except RemoteError as exc:
                # 读取失败不判坏批（可能是瞬时传输问题），下轮重试
                summary["errors"].append(f"读取 {csv_path} 失败：{exc}")
                logger.warning(summary["errors"][-1])
                continue

            result = validate_csv(raw)
            if not result.ok:
                db.reject_batch(conn, batch_id, result.reasons)
                summary["rejected"] += 1
                logger.warning("批次 %s 校验失败：%s", batch_dir, result.reasons)
                continue

            # 整批入库 + 回执（单事务，唯一约束兜底防重复）
            receipt_no = f"RC-{uuid.uuid4().hex[:16].upper()}"
            try:
                db.commit_batch(
                    conn, batch_id, result.records, receipt_no, result.checksum
                )
            except Exception as exc:  # noqa: BLE001
                summary["errors"].append(f"批次 {batch_dir} 入库失败：{exc}")
                logger.exception("批次 %s 入库事务失败", batch_dir)
                continue
            summary["ingested"] += 1
            logger.info("批次 %s 入库 %d 条，回执 %s",
                        batch_dir, len(result.records), receipt_no)

            # 入库成功后归档；失败则保留结果，下轮自动重试
            if self._archive(conn, client, batch_dir, batch_id):
                summary["archived"] += 1

    # ----------------------------- 归档 -----------------------------

    def _archive(self, conn, client: SftpProtocol, batch_dir: str, batch_id: int) -> bool:
        """把远端整目录移到归档区。返回是否已处于归档态。"""
        with db.connect() as flag_conn:
            if db.get_flag(flag_conn, "archive_refused"):
                reason = "演示开关已打开：暂时拒绝归档，结果已安全入库，将自动重试"
                db.bump_archive_attempt(flag_conn, batch_id, reason)
                logger.warning("批次 %s %s", batch_dir, reason)
                return False

        src = join_remote(settings.remote_inbound, batch_dir)
        dst = join_remote(settings.remote_archive, batch_dir)
        try:
            client.move(src, dst)
        except ArchiveRefusedError as exc:
            db.bump_archive_attempt(conn, batch_id, str(exc))
            logger.warning("批次 %s 归档被拒：%s", batch_dir, exc)
            return False
        except RemoteError as exc:
            db.bump_archive_attempt(conn, batch_id, f"归档移动失败：{exc}")
            logger.warning("批次 %s 归档失败，将在下轮重试：%s", batch_dir, exc)
            return False

        # 移动成功（或目标已存在的幂等成功）后对账标记
        db.mark_archived(conn, batch_id, dst)
        logger.info("批次 %s 已归档至 %s", batch_dir, dst)
        return True
