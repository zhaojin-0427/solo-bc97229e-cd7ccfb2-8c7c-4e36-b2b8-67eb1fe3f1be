"""端到端集成测试（不依赖 Docker）：用本地文件系统模拟远端 SFTP。

运行：.venv/bin/python -m tests.test_e2e
覆盖：
1. 正常批次：整批入库 + 回执 + 远端移动到归档区；
2. 坏批：不入库、不归档，返回原因；
3. READY 门控：未就绪批次不被扫描；
4. 演示开关：拒绝归档时结果保留并计数重试，关闭后自动补归档；
5. 崩溃接续：模拟“入库后移动前”与“移动后标记前”两类中断；
6. 并发触发：多线程同时扫描不重复入库、不漏批。
"""
from __future__ import annotations

import os
import shutil
import sys
import tempfile
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 必须在导入应用模块前指定本地 DB 与 SFTP 根
_TMP = tempfile.mkdtemp(prefix="pharma-e2e-")
os.environ["DB_PATH"] = os.path.join(_TMP, "ingest.db")
os.environ["SCAN_INTERVAL_SECONDS"] = "60"
SFTP_ROOT = os.path.join(_TMP, "sftp-root")

from app import database as db  # noqa: E402
from app.config import settings  # noqa: E402
from app.sftp_client import LocalFtpClient  # noqa: E402
from app.worker import IngestWorker  # noqa: E402

GOOD_CSV = (
    "sample_no,test_item,result_value\n"
    "S1,含量,98.6\nS1,有关物质,0.2\nS2,含量,99.1\n"
)
BAD_CSV = (
    "sample_no,test_item,result_value\n"
    "S3,含量,\nS4,,98.0\nS5,含量,97.0\nS5,含量,97.1\n"
)


def _seed_batch(root: str, name: str, csv_text: str, ready: bool = True) -> None:
    inbound_abs = os.path.join(root, settings.remote_inbound.lstrip("/"), name)
    os.makedirs(inbound_abs, exist_ok=True)
    with open(os.path.join(inbound_abs, "results.csv"), "w", encoding="utf-8") as fp:
        fp.write(csv_text)
    if ready:
        with open(os.path.join(inbound_abs, "READY"), "w", encoding="utf-8") as fp:
            fp.write("ready\n")


def _remote_path(root: str, *parts: str) -> str:
    return os.path.join(root, *[p.lstrip("/") for p in parts])


def main() -> None:
    failures: list[str] = []

    def check(cond: bool, label: str) -> None:
        print(("  PASS " if cond else "  FAIL ") + label)
        if not cond:
            failures.append(label)

    print("== 1. 正常采集、坏批拒收、READY 门控 ==")
    db.init_db()
    client = LocalFtpClient(SFTP_ROOT)
    _seed_batch(SFTP_ROOT, "BATCH-GOOD-1", GOOD_CSV)
    _seed_batch(SFTP_ROOT, "BATCH-BAD-1", BAD_CSV)
    _seed_batch(SFTP_ROOT, "BATCH-WAIT-1", GOOD_CSV, ready=False)

    worker = IngestWorker(client_factory=lambda: client)
    summary = worker.scan_once()
    check(summary["ingested"] == 1, f"好批入库 1 个（实际 {summary['ingested']}）")
    check(summary["rejected"] == 1, f"坏批拒收 1 个（实际 {summary['rejected']}）")
    check(summary["archived"] == 1, f"好批归档 1 个（实际 {summary['archived']}）")

    with db.connect() as conn:
        good = db.batch_row(conn, "BATCH-GOOD-1")
        bad = db.batch_row(conn, "BATCH-BAD-1")
        wait = db.batch_row(conn, "BATCH-WAIT-1")
        n_results = db.count_results(conn, "BATCH-GOOD-1", None, None)
        receipt = db.get_receipt(conn, int(good["id"]))
    check(good["status"] == "ARCHIVED", "好批状态 ARCHIVED")
    check(n_results == 3, f"好批 3 条记录（实际 {n_results}）")
    check(receipt is not None and receipt["receipt_no"].startswith("RC-"), "生成收件回执")
    check(bad["status"] == "REJECTED", "坏批状态 REJECTED")
    import json as _json
    reasons = _json.loads(bad["error_reasons"])
    check(len(reasons) == 3, f"坏批给出 3 条原因（实际 {len(reasons)}：{reasons}）")
    check(wait is None, "未就绪批次未登记")
    check(
        os.path.isdir(_remote_path(SFTP_ROOT, settings.remote_archive, "BATCH-GOOD-1")),
        "好批目录已移到归档区",
    )
    check(
        os.path.isdir(_remote_path(SFTP_ROOT, settings.remote_inbound, "BATCH-BAD-1")),
        "坏批目录保留在收件箱、未归档",
    )
    check(
        os.path.isdir(_remote_path(SFTP_ROOT, settings.remote_inbound, "BATCH-WAIT-1")),
        "未就绪批次仍在收件箱",
    )

    print("== 2. 演示开关：拒绝归档 -> 保留入库结果并自动重试 -> 恢复后补归档 ==")
    _seed_batch(SFTP_ROOT, "BATCH-GOOD-2", GOOD_CSV)
    # 与 API 的 PUT /admin/archive-gate 走同一条数据库开关路径
    with db.connect() as conn:
        db.set_flag(conn, "archive_refused", True)
    s2 = worker.scan_once()
    check(s2["ingested"] == 1, "拒绝归档期间批次仍正常入库")
    with db.connect() as conn:
        row2 = db.batch_row(conn, "BATCH-GOOD-2")
        kept = db.count_results(conn, "BATCH-GOOD-2", None, None)
    check(row2["archive_attempts"] >= 1, "本轮归档被拒并记录尝试次数，等待自动重试")
    check(row2["status"] == "INGESTED", "新批次保持 INGESTED 待重试")
    check(kept == 3, "已入库结果完整保留（3 条）")
    check(row2["archive_attempts"] >= 1, "归档尝试次数被记录")
    check(
        os.path.isdir(_remote_path(SFTP_ROOT, settings.remote_inbound, "BATCH-GOOD-2")),
        "远端目录未被移动",
    )
    # 再扫两轮：持续失败、持续重试，不产生重复数据
    worker.scan_once()
    worker.scan_once()
    with db.connect() as conn:
        row2 = db.batch_row(conn, "BATCH-GOOD-2")
        kept2 = db.count_results(conn, "BATCH-GOOD-2", None, None)
        n_batches = db.count_batches(conn, None)
    check(kept2 == 3, "重试期间记录不重复（仍 3 条）")
    check(row2["archive_attempts"] >= 3, f"失败重试累计（实际 {row2['archive_attempts']}）")
    check(n_batches == 3, f"批次不重复登记（实际 {n_batches}）")

    # 关闭开关，下轮自动补归档
    with db.connect() as conn:
        db.set_flag(conn, "archive_refused", False)
    s3 = worker.scan_once()
    check(s3["archived"] == 1, "恢复后补归档成功")
    with db.connect() as conn:
        row2 = db.batch_row(conn, "BATCH-GOOD-2")
    check(row2["status"] == "ARCHIVED", "批次最终 ARCHIVED")
    check(
        os.path.isdir(_remote_path(SFTP_ROOT, settings.remote_archive, "BATCH-GOOD-2")),
        "目录已补移到归档区",
    )

    print("== 3. 崩溃接续：入库后/移动前中断，重启自动补归档 ==")
    _seed_batch(SFTP_ROOT, "BATCH-CRASH-1", GOOD_CSV)
    # 只跑入库前半段：用一个归档必失败的临时客户端模拟进程在移动前死掉
    crash_client = LocalFtpClient(SFTP_ROOT, refuse_archive=True)
    crash_worker = IngestWorker(client_factory=lambda: crash_client)
    crash_worker.scan_once()
    with db.connect() as conn:
        c1 = db.batch_row(conn, "BATCH-CRASH-1")
    check(c1["status"] == "INGESTED" and c1["receipt_no"] is not None, "崩溃前已入库并持久化")
    # “重启”：新建 worker（新线程/新连接），开关恢复
    restart_client = LocalFtpClient(SFTP_ROOT, refuse_archive=False)
    restart_worker = IngestWorker(client_factory=lambda: restart_client)
    restart_worker.scan_once()
    with db.connect() as conn:
        c1 = db.batch_row(conn, "BATCH-CRASH-1")
        c1_results = db.count_results(conn, "BATCH-CRASH-1", None, None)
    check(c1["status"] == "ARCHIVED", "重启后自动补归档")
    check(c1_results == 3, "重启后记录无重复无丢失")

    print("== 4. 崩溃接续：移动后/标记前中断，重启对账 ==")
    _seed_batch(SFTP_ROOT, "BATCH-CRASH-2", GOOD_CSV)
    crash2 = LocalFtpClient(SFTP_ROOT, refuse_archive=True)
    cw2 = IngestWorker(client_factory=lambda: crash2)
    cw2.scan_once()  # 入库，归档被拒
    # 人工/外部把目录移走，模拟“移动成功但未来得及更新数据库就断电”
    src = _remote_path(SFTP_ROOT, settings.remote_inbound, "BATCH-CRASH-2")
    dst = _remote_path(SFTP_ROOT, settings.remote_archive, "BATCH-CRASH-2")
    os.rename(src, dst)
    restart2 = IngestWorker(client_factory=lambda: LocalFtpClient(SFTP_ROOT))
    restart2.scan_once()
    with db.connect() as conn:
        c2 = db.batch_row(conn, "BATCH-CRASH-2")
    check(c2["status"] == "ARCHIVED", "移动后中断：重启对账为 ARCHIVED（幂等）")

    print("== 5. 并发触发：多线程同时扫描，不重复不遗漏 ==")
    for i in range(5):
        _seed_batch(SFTP_ROOT, f"BATCH-PAR-{i}", GOOD_CSV)
    parallel_worker = IngestWorker(client_factory=lambda: LocalFtpClient(SFTP_ROOT))
    errors: list[Exception] = []

    def _safe_scan() -> None:
        try:
            parallel_worker.scan_once()
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=_safe_scan) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    check(not errors, f"并发扫描无异常（{len(errors)} 个异常）")
    with db.connect() as conn:
        for i in range(5):
            row = db.batch_row(conn, f"BATCH-PAR-{i}")
            n = db.count_results(conn, f"BATCH-PAR-{i}", None, None)
            check(row is not None and row["status"] == "ARCHIVED",
                  f"BATCH-PAR-{i} 已归档且仅处理一次")
            check(n == 3, f"BATCH-PAR-{i} 恰好 3 条记录（实际 {n}）")
        total = db.count_batches(conn, None)
    expected_names = {
        "BATCH-GOOD-1", "BATCH-BAD-1", "BATCH-GOOD-2",
        "BATCH-CRASH-1", "BATCH-CRASH-2",
        *[f"BATCH-PAR-{i}" for i in range(5)],
    }
    check(total == len(expected_names), f"批次总数正确（实际 {total}，期望 {len(expected_names)}）")

    print("== 6. 坏批原因内容核对 ==")
    with db.connect() as conn:
        bad = db.batch_row(conn, "BATCH-BAD-1")
        reasons = _json.loads(bad["error_reasons"])
    check(any("结果为空" in r for r in reasons), "捕获空结果")
    check(any("项目为空" in r for r in reasons), "捕获空项目")
    check(any("重复" in r for r in reasons), "捕获批内组合重复")

    print()
    if failures:
        print(f"失败 {len(failures)} 项：")
        for f in failures:
            print(" - " + f)
        sys.exit(1)
    print("全部通过 ✔")
    shutil.rmtree(_TMP, ignore_errors=True)


if __name__ == "__main__":
    main()
