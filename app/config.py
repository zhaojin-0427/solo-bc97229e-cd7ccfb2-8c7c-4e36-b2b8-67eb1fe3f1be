"""集中配置：所有参数均可通过环境变量覆盖（见 docker-compose.yml）。"""
from __future__ import annotations

import os
from dataclasses import dataclass


def _env(key: str, default: str) -> str:
    value = os.environ.get(key)
    return value if value not in (None, "") else default


def _env_int(key: str, default: int) -> int:
    try:
        return int(_env(key, str(default)))
    except ValueError:
        return default


@dataclass(frozen=True)
class Settings:
    # SFTP（外包实验室投递端）
    sftp_host: str = _env("SFTP_HOST", "sftp")
    sftp_port: int = _env_int("SFTP_PORT", 22)
    sftp_user: str = _env("SFTP_USER", "lab")
    sftp_password: str = _env("SFTP_PASSWORD", "labpass")
    # 远端 SFTP 上的目录（相对该用户根目录）
    remote_inbound: str = _env("REMOTE_INBOUND", "/inbound")
    remote_archive: str = _env("REMOTE_ARCHIVE", "/archive")

    # 本地原始库
    db_path: str = _env("DB_PATH", "/data/ingest.db")

    # 后台扫描
    scan_interval_seconds: int = _env_int("SCAN_INTERVAL_SECONDS", 10)

    # SFTP 操作重试
    sftp_retries: int = _env_int("SFTP_RETRIES", 3)
    sftp_retry_backoff_seconds: float = float(_env("SFTP_RETRY_BACKOFF_SECONDS", "2"))


settings = Settings()
