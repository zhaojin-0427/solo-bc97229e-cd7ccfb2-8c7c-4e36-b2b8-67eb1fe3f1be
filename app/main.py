"""药检数据采集 API。

启动后后台线程周期性扫描 SFTP 收件目录；也可通过 POST /collect 立即触发。
交互式文档：/docs（Swagger UI），健康检查：/health。
"""
from __future__ import annotations

import json
import sqlite3
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException, Query

from . import database as db
from .config import settings
from .schemas import (
    ArchiveGateOut,
    BatchListResponse,
    BatchOut,
    CollectResponse,
    ReceiptOut,
    RemoteBatchOut,
    RemoteOverview,
    ResultListResponse,
    ResultOut,
)
from .sftp_client import SftpClient, SftpProtocol, join_remote
from .worker import IngestWorker

worker = IngestWorker()


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init_db()
    worker.start()
    yield
    worker.stop()


app = FastAPI(
    title="药检数据采集 API",
    description=(
        "从外包实验室 SFTP 收件目录采集检测 CSV，校验通过后整批入库、"
        "生成收件回执并把远端目录移至归档区。支持坏批拒收、归档失败自动重试、"
        "崩溃接续与并发幂等。\n\n"
        "**演示路径**：① `POST /admin/archive-gate` 打开拒绝归档开关 → "
        "② `POST /collect` 触发采集 → ③ `GET /batches` 观察 INGESTED 待归档 → "
        "④ 关闭开关后再次 `POST /collect` → 批次变为 ARCHIVED。"
    ),
    version="1.0.0",
    lifespan=lifespan,
)


def get_conn():
    with db.connect() as conn:
        yield conn


def _row_to_batch(row: sqlite3.Row) -> BatchOut:
    data = dict(row)
    reasons = data.get("error_reasons")
    data["error_reasons"] = json.loads(reasons) if reasons else None
    return BatchOut(**data)


# ------------------------------- 采集触发 -------------------------------

@app.post(
    "/collect",
    response_model=CollectResponse,
    tags=["采集"],
    summary="触发一次后台采集扫描",
)
def collect() -> CollectResponse:
    completed = worker.trigger(wait=5.0)
    if completed:
        message = "采集已完成，可用 GET /batches 与 GET /results 查询"
    else:
        message = "采集已触发且仍在后台执行，可用 GET /collect/status 与 GET /batches 跟踪"
    return CollectResponse(
        triggered=True,
        message=message,
        latest_scan=worker.last_summary or None,
    )


@app.get("/collect/status", tags=["采集"], summary="最近一次扫描摘要")
def collect_status() -> dict:
    return {
        "scan_interval_seconds": settings.scan_interval_seconds,
        "last_scan": worker.last_summary or None,
        "worker_alive": bool(worker.is_alive()),
    }


# ------------------------------- 批次查询 -------------------------------

@app.get(
    "/batches",
    response_model=BatchListResponse,
    tags=["批次"],
    summary="批次状态列表",
)
def list_batches(
    status: str | None = Query(
        None,
        description="按状态过滤：INGESTED（已入库待归档/重试中）、ARCHIVED、REJECTED",
        pattern="^(INGESTED|ARCHIVED|REJECTED)$",
    ),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    conn: sqlite3.Connection = Depends(get_conn),
) -> BatchListResponse:
    rows = db.list_batches(conn, status, limit, offset)
    return BatchListResponse(
        total=db.count_batches(conn, status),
        limit=limit,
        offset=offset,
        items=[_row_to_batch(r) for r in rows],
    )


@app.get(
    "/batches/{batch_dir}",
    response_model=BatchOut,
    tags=["批次"],
    summary="按批次目录名查询批次状态（含坏批原因/归档重试信息）",
)
def get_batch(batch_dir: str, conn: sqlite3.Connection = Depends(get_conn)) -> BatchOut:
    row = db.batch_row(conn, batch_dir)
    if not row:
        raise HTTPException(status_code=404, detail=f"未找到批次：{batch_dir}")
    return _row_to_batch(row)


@app.get(
    "/batches/{batch_dir}/receipt",
    response_model=ReceiptOut,
    tags=["批次"],
    summary="查询批次收件回执（仅已入库批次）",
)
def get_receipt(batch_dir: str, conn: sqlite3.Connection = Depends(get_conn)) -> ReceiptOut:
    row = db.batch_row(conn, batch_dir)
    if not row:
        raise HTTPException(status_code=404, detail=f"未找到批次：{batch_dir}")
    receipt = db.get_receipt(conn, int(row["id"]))
    if not receipt:
        raise HTTPException(
            status_code=409,
            detail=f"批次 {batch_dir} 尚未成功入库（状态 {row['status']}），无回执",
        )
    return ReceiptOut(**dict(receipt))


# ------------------------------- 记录查询 -------------------------------

@app.get(
    "/results",
    response_model=ResultListResponse,
    tags=["记录"],
    summary="原始检测记录查询（可按批次/样品号/项目过滤）",
)
def list_results(
    batch_dir: str | None = Query(None, description="批次目录名"),
    sample_no: str | None = Query(None, description="样品号精确匹配"),
    test_item: str | None = Query(None, description="检测项目精确匹配"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    conn: sqlite3.Connection = Depends(get_conn),
) -> ResultListResponse:
    rows = db.list_results(conn, batch_dir, sample_no, test_item, limit, offset)
    return ResultListResponse(
        total=db.count_results(conn, batch_dir, sample_no, test_item),
        limit=limit,
        offset=offset,
        items=[ResultOut(**dict(r)) for r in rows],
    )


# ------------------------------- 演示开关 -------------------------------

@app.get(
    "/admin/archive-gate",
    response_model=ArchiveGateOut,
    tags=["演示开关"],
    summary="查看“暂时拒绝归档”开关",
)
def get_archive_gate(conn: sqlite3.Connection = Depends(get_conn)) -> ArchiveGateOut:
    refused = db.get_flag(conn, "archive_refused")
    row = conn.execute(
        "SELECT updated_at FROM app_settings WHERE key='archive_refused'"
    ).fetchone()
    return ArchiveGateOut(
        archive_refused=refused,
        updated_at=row["updated_at"] if row else None,
        message="当前拒绝归档" if refused else "当前允许归档",
    )


@app.put(
    "/admin/archive-gate",
    response_model=ArchiveGateOut,
    tags=["演示开关"],
    summary="设置“暂时拒绝归档”开关（模拟归档区故障）",
)
def set_archive_gate(
    archive_refused: bool = Query(
        ..., description="true=拒绝归档并让系统自动重试；false=恢复归档"
    ),
    conn: sqlite3.Connection = Depends(get_conn),
) -> ArchiveGateOut:
    db.set_flag(conn, "archive_refused", archive_refused)
    return ArchiveGateOut(
        archive_refused=archive_refused,
        updated_at=db.utcnow(),
        message=(
            "已打开：新批次会正常入库，但远端移动将被拒绝并自动重试"
            if archive_refused
            else "已关闭：下轮扫描将把所有待归档批次补归档"
        ),
    )


# ------------------------------- 远端观测 -------------------------------

@app.get(
    "/remote/overview",
    response_model=RemoteOverview,
    tags=["远端 SFTP"],
    summary="查看 SFTP 收件箱与归档区当前内容（演示用）",
)
def remote_overview() -> RemoteOverview:
    client: SftpProtocol = SftpClient()
    try:
        client.ensure_dirs()

        def inspect(base: str, location: str) -> list[RemoteBatchOut]:
            items: list[RemoteBatchOut] = []
            for name in client.list_dir(base):
                path = join_remote(base, name)
                items.append(
                    RemoteBatchOut(
                        name=name,
                        location=location,
                        has_ready=client.exists(join_remote(path, "READY")),
                        has_results_csv=client.exists(join_remote(path, "results.csv")),
                    )
                )
            return items

        return RemoteOverview(
            inbound=inspect(settings.remote_inbound, "inbound"),
            archive=inspect(settings.remote_archive, "archive"),
        )
    finally:
        client.close()


@app.get("/health", tags=["系统"], summary="健康检查")
def health() -> dict:
    return {"status": "ok"}
