"""API 响应模型（同时驱动交互式文档的示例展示）。"""
from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class CollectResponse(BaseModel):
    triggered: bool = Field(..., description="是否已唤醒后台扫描")
    message: str
    latest_scan: dict[str, Any] | None = Field(
        None, description="最近一次扫描摘要（若已执行过）"
    )


class BatchOut(BaseModel):
    id: int
    batch_dir: str = Field(..., description="批次目录名（全局唯一）")
    status: str = Field(
        ..., description="INGESTED=已入库待归档/归档失败待重试；ARCHIVED=已归档；REJECTED=坏批"
    )
    record_count: int
    receipt_no: str | None
    csv_checksum: str | None
    archive_path: str | None
    archive_attempts: int
    error_reasons: list[str] | None
    discovered_at: str
    ingested_at: str | None
    archived_at: str | None
    rejected_at: str | None
    updated_at: str


class BatchListResponse(BaseModel):
    total: int
    limit: int
    offset: int
    items: list[BatchOut]


class ResultOut(BaseModel):
    id: int
    batch_id: int
    batch_dir: str
    sample_no: str = Field(..., description="样品号")
    test_item: str = Field(..., description="检测项目")
    result_value: str = Field(..., description="检测结果")
    row_no: int


class ResultListResponse(BaseModel):
    total: int
    limit: int
    offset: int
    items: list[ResultOut]


class ReceiptOut(BaseModel):
    receipt_no: str
    batch_dir: str
    record_count: int
    checksum: str
    created_at: str


class ArchiveGateOut(BaseModel):
    archive_refused: bool = Field(
        ..., description="true=暂时拒绝归档（演示故障恢复）；false=允许归档"
    )
    updated_at: str | None
    message: str


class RemoteBatchOut(BaseModel):
    name: str
    location: str = Field(..., description="inbound=收件箱；archive=归档区")
    has_ready: bool
    has_results_csv: bool


class RemoteOverview(BaseModel):
    inbound: list[RemoteBatchOut]
    archive: list[RemoteBatchOut]
