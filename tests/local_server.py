"""本地冒烟启动器：用 LocalFtpClient 替代真实 SFTP 跑完整 HTTP 服务。

仅用于无 Docker 环境的人工/脚本验证；交付的 Docker 环境使用真实 paramiko SFTP。
启动：.venv/bin/python -I tests/local_server.py
"""
from __future__ import annotations

import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

_TMP = tempfile.mkdtemp(prefix="pharma-smoke-")
os.environ.setdefault("DB_PATH", os.path.join(_TMP, "ingest.db"))
os.environ.setdefault("SCAN_INTERVAL_SECONDS", "3600")  # 冒烟时只靠手动触发
_ROOT = os.path.join(_TMP, "sftp-root")

from app import main  # noqa: E402
from app.sftp_client import LocalFtpClient  # noqa: E402

# 注入本地文件系统 SFTP
_local = LocalFtpClient(_ROOT)
main.worker = main.IngestWorker(client_factory=lambda: _local)

# 预播一个好批、一个坏批
from app.config import settings  # noqa: E402

for name, csv_text, ready in [
    (
        "SMOKE-GOOD",
        "sample_no,test_item,result_value\nS1,含量,98.6\nS1,有关物质,0.2\n",
        True,
    ),
    (
        "SMOKE-BAD",
        "sample_no,test_item,result_value\nS9,含量,\n",
        True,
    ),
]:
    base = os.path.join(_ROOT, settings.remote_inbound.lstrip("/"), name)
    os.makedirs(base, exist_ok=True)
    with open(os.path.join(base, "results.csv"), "w", encoding="utf-8") as fp:
        fp.write(csv_text)
    if ready:
        with open(os.path.join(base, "READY"), "w", encoding="utf-8") as fp:
            fp.write("ready\n")

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(main.app, host="127.0.0.1", port=8000, workers=1)
