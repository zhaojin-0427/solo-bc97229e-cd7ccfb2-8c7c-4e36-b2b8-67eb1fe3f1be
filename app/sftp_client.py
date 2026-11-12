"""SFTP 客户端抽象。

- SftpClient：生产实现，基于 paramiko，连接外包实验室的 SFTP 服务；
- LocalFtpClient：与生产接口完全一致的本地文件系统实现，
  用于在没有 Docker 的环境做端到端故障恢复验证（测试代码使用，不参与交付运行）。

关键操作 move()：把收件目录整体移动到归档区。两个实现都保证
“目标已存在时不覆盖”，因此归档重试天然幂等，不会覆盖已归档数据。
"""
from __future__ import annotations

import os
import posixpath
import shutil
import time
from typing import Protocol

from .config import settings


class RemoteError(RuntimeError):
    """远端（SFTP/本地模拟）操作失败。"""


class ArchiveRefusedError(RemoteError):
    """演示开关打开：暂时拒绝归档。"""


class SftpProtocol(Protocol):
    def ensure_dirs(self) -> None: ...
    def list_dir(self, path: str) -> list[str]: ...
    def exists(self, path: str) -> bool: ...
    def read_file(self, path: str) -> bytes: ...
    def move(self, src: str, dst: str) -> None: ...
    def close(self) -> None: ...


class SftpClient:
    """基于 paramiko 的真实 SFTP 客户端，带瞬时故障重试。"""

    def __init__(self) -> None:
        import paramiko  # 延迟导入：本地测试无需安装

        self._paramiko = paramiko
        self._client = paramiko.SSHClient()
        self._client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self._sftp = None
        self._connect()

    def _connect(self) -> None:
        last_err: Exception | None = None
        for attempt in range(1, settings.sftp_retries + 1):
            try:
                self._client.connect(
                    hostname=settings.sftp_host,
                    port=settings.sftp_port,
                    username=settings.sftp_user,
                    password=settings.sftp_password,
                    timeout=10,
                    allow_agent=False,
                    look_for_keys=False,
                )
                self._sftp = self._client.open_sftp()
                return
            except Exception as exc:  # noqa: BLE001 - 连接层统一重试
                last_err = exc
                time.sleep(settings.sftp_retry_backoff_seconds * attempt)
        raise RemoteError(f"SFTP 连接失败（已重试 {settings.sftp_retries} 次）：{last_err}")

    def _run(self, op, *args):
        """带断线重连的操作执行。"""
        last_err: Exception | None = None
        for attempt in range(settings.sftp_retries):
            try:
                return op(*args)
            except EOFError as exc:  # 连接被对端断开
                last_err = exc
                self.close()
                self._connect()
            except OSError as exc:
                last_err = exc
                # 瞬时网络错误：退避后重连重试
                time.sleep(settings.sftp_retry_backoff_seconds * (attempt + 1))
                try:
                    self.close()
                    self._connect()
                except RemoteError:
                    pass
        raise RemoteError(f"SFTP 操作失败：{last_err}")

    def ensure_dirs(self) -> None:
        def _make() -> None:
            assert self._sftp is not None
            for base in (settings.remote_inbound, settings.remote_archive):
                parts = base.strip("/").split("/")
                cur = ""
                for part in parts:
                    cur = f"/{part}" if not cur else f"{cur}/{part}"
                    try:
                        self._sftp.stat(cur)
                    except IOError:
                        self._sftp.mkdir(cur)
        self._run(_make)

    def list_dir(self, path: str) -> list[str]:
        def _list() -> list[str]:
            assert self._sftp is not None
            return self._sftp.listdir(path)
        return self._run(_list)

    def exists(self, path: str) -> bool:
        def _exists() -> bool:
            assert self._sftp is not None
            try:
                self._sftp.stat(path)
                return True
            except IOError:
                return False
        return self._run(_exists)

    def read_file(self, path: str) -> bytes:
        def _read() -> bytes:
            assert self._sftp is not None
            with self._sftp.open(path, "rb") as fp:
                chunks: list[bytes] = []
                while True:
                    chunk = fp.read(65536)
                    if not chunk:
                        break
                    chunks.append(chunk)
                return b"".join(chunks)
        return self._run(_read)

    def move(self, src: str, dst: str) -> None:
        def _move() -> None:
            assert self._sftp is not None
            if self.exists(dst):
                # 幂等：目标已存在说明此前已归档成功，仅清理来源
                try:
                    self._sftp.rmdir(src) if self._is_empty_dir(src) else None
                except OSError:
                    pass
                return
            # SFTP rename 在同文件系统上是原子操作
            self._sftp.rename(src, dst)
        self._run(_move)

    def _is_empty_dir(self, path: str) -> bool:
        assert self._sftp is not None
        return len(self._sftp.listdir(path)) == 0

    def close(self) -> None:
        try:
            if self._sftp is not None:
                self._sftp.close()
        except Exception:
            pass
        try:
            self._client.close()
        except Exception:
            pass
        self._sftp = None


class LocalFtpClient:
    """本地文件系统版“SFTP”，接口与 SftpClient 一致，仅用于离线集成测试。

    构造参数 root 模拟 SFTP 用户根目录；remote_inbound/remote_archive
    均按相对该根目录的 POSIX 路径解析。
    """

    def __init__(self, root: str, refuse_archive: bool = False) -> None:
        self.root = root
        self.refuse_archive = refuse_archive
        os.makedirs(self._abs(settings.remote_inbound), exist_ok=True)
        os.makedirs(self._abs(settings.remote_archive), exist_ok=True)

    def _abs(self, remote_path: str) -> str:
        return os.path.join(self.root, remote_path.lstrip("/"))

    def ensure_dirs(self) -> None:
        os.makedirs(self._abs(settings.remote_inbound), exist_ok=True)
        os.makedirs(self._abs(settings.remote_archive), exist_ok=True)

    def list_dir(self, path: str) -> list[str]:
        target = self._abs(path)
        if not os.path.isdir(target):
            raise RemoteError(f"远端目录不存在：{path}")
        return sorted(os.listdir(target))

    def exists(self, path: str) -> bool:
        return os.path.exists(self._abs(path))

    def read_file(self, path: str) -> bytes:
        target = self._abs(path)
        if not os.path.isfile(target):
            raise RemoteError(f"远端文件不存在：{path}")
        with open(target, "rb") as fp:
            return fp.read()

    def move(self, src: str, dst: str) -> None:
        if self.refuse_archive:
            raise ArchiveRefusedError("演示开关已打开：当前暂时拒绝归档")
        src_abs, dst_abs = self._abs(src), self._abs(dst)
        if os.path.exists(dst_abs):
            # 幂等：归档目标已存在，视为成功
            if os.path.isdir(src_abs):
                shutil.rmtree(src_abs)
            return
        os.makedirs(os.path.dirname(dst_abs), exist_ok=True)
        os.rename(src_abs, dst_abs)  # 同文件系统原子 rename

    def close(self) -> None:
        return None


def join_remote(*parts: str) -> str:
    return posixpath.join(*parts)
