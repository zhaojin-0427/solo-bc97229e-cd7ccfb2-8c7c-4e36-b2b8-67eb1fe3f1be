"""真实 SFTP 协议联调：进程内 paramiko SFTPServer + 生产 SftpClient。

验证生产代码的 paramiko 路径（连接、列目录、读文件、rename 归档、重试）
在真实 SFTP 线上可用，无需 Docker 或 root。
运行：.venv/bin/python -I tests/test_sftp_real.py
"""
from __future__ import annotations

import os
import socket
import stat
import sys
import tempfile
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import paramiko  # noqa: E402

_TMP = tempfile.mkdtemp(prefix="pharma-sftp-")
os.environ["DB_PATH"] = os.path.join(_TMP, "ingest.db")
os.environ["SFTP_HOST"] = "127.0.0.1"
os.environ["SFTP_PORT"] = "0"  # 由测试回填
os.environ["SFTP_USER"] = "lab"
os.environ["SFTP_PASSWORD"] = "labpass"
os.environ["REMOTE_INBOUND"] = "/inbound"
os.environ["REMOTE_ARCHIVE"] = "/archive"

SFTP_ROOT = os.path.join(_TMP, "remote")
os.makedirs(os.path.join(SFTP_ROOT, "inbound"), exist_ok=True)
os.makedirs(os.path.join(SFTP_ROOT, "archive"), exist_ok=True)
HOST_KEY = paramiko.RSAKey.generate(2048)


class _Server(paramiko.ServerInterface):
    def __init__(self) -> None:
        self.event = threading.Event()

    def check_channel_request(self, kind, chanid):
        return paramiko.OPEN_SUCCEEDED if kind == "session" else paramiko.OPEN_FAILED_ADMINISTRATIVELY_PROHIBITED

    def check_auth_password(self, username, password):
        if username == "lab" and password == "labpass":
            return paramiko.AUTH_SUCCESSFUL
        return paramiko.AUTH_FAILED

    def get_allowed_auths(self, username):
        return "password"

    def check_channel_shell_request(self, channel):
        self.event.set()
        return True

    def check_channel_pty_request(self, channel, term, width, height, pixelwidth, pixelheight, modes):
        return True

    def check_channel_subsystem_request(self, channel, name):
        # 必须走基类：默认实现负责实例化 set_subsystem_handler 注册的 SFTPServer
        if name != "sftp":
            return False
        return super().check_channel_subsystem_request(channel, name)


class _SFTP(paramiko.SFTPServerInterface):
    """把 SFTP 操作映射到本地 SFTP_ROOT（chroot 语义）。"""

    ROOT = SFTP_ROOT

    def _abs(self, path: str) -> str:
        path = path or "/"
        return os.path.join(self.ROOT, path.lstrip("/"))

    @staticmethod
    def _attrs(full: str, name: str | None = None) -> paramiko.SFTPAttributes:
        st = os.stat(full)
        attr = paramiko.SFTPAttributes()
        attr.st_size = st.st_size
        attr.st_mode = (
            stat.S_IFDIR | 0o755 if os.path.isdir(full) else stat.S_IFREG | 0o644
        )
        attr.st_mtime = int(st.st_mtime)
        attr.st_uid = 0
        attr.st_gid = 0
        if name is not None:
            attr.filename = name
        return attr

    def list_folder(self, path):
        target = self._abs(path)
        if not os.path.isdir(target):
            return paramiko.SFTP_NO_SUCH_FILE
        return [
            self._attrs(os.path.join(target, name), name)
            for name in sorted(os.listdir(target))
        ]

    def stat(self, path):
        target = self._abs(path)
        if not os.path.exists(target):
            return paramiko.SFTP_NO_SUCH_FILE
        return self._attrs(target)

    lstat = stat

    def open(self, path, flags, attr):
        target = self._abs(path)
        try:
            fd = os.open(target, flags, getattr(attr, "st_mode", 0o644) or 0o644)
        except OSError:
            return paramiko.SFTP_NO_SUCH_FILE
        handle = paramiko.SFTPHandle(flags)
        handle.readfile = os.fdopen(fd, "rb")
        handle.writefile = os.fdopen(fd, "wb")
        return handle

    def rename(self, oldpath, newpath):
        try:
            os.rename(self._abs(oldpath), self._abs(newpath))
        except OSError:
            return paramiko.SFTP_FAILURE
        return paramiko.SFTP_OK

    def mkdir(self, path, attr):
        try:
            os.mkdir(self._abs(path))
        except FileExistsError:
            return paramiko.SFTP_FAILURE
        return paramiko.SFTP_OK

    def rmdir(self, path):
        try:
            os.rmdir(self._abs(path))
        except OSError:
            return paramiko.SFTP_FAILURE
        return paramiko.SFTP_OK

    def remove(self, path):
        try:
            os.remove(self._abs(path))
        except OSError:
            return paramiko.SFTP_FAILURE
        return paramiko.SFTP_OK

    def chattr(self, path, attr):
        return paramiko.SFTP_OK

    def canonicalize(self, path):
        return path if path.startswith("/") else "/" + path


def _serve(sock: socket.socket) -> None:
    transport = paramiko.Transport(sock)
    transport.add_server_key(HOST_KEY)
    transport.set_subsystem_handler("sftp", paramiko.SFTPServer, _SFTP)
    server = _Server()
    try:
        transport.start_server(server=server)
        channel = transport.accept(20)
        if channel is None:
            return
        # 等待子系统线程结束（客户端关闭通道后 transport 自动失活）
        for _ in range(100):
            if not transport.is_active():
                break
            time.sleep(0.1)
    except Exception:
        pass
    finally:
        transport.close()


def main() -> None:
    failures: list[str] = []

    def check(cond: bool, label: str) -> None:
        print(("  PASS " if cond else "  FAIL ") + label)
        if not cond:
            failures.append(label)

    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.bind(("127.0.0.1", 0))
    server_sock.listen(5)
    port = server_sock.getsockname()[1]
    os.environ["SFTP_PORT"] = str(port)

    stop = threading.Event()

    def accept_loop() -> None:
        server_sock.settimeout(0.5)
        while not stop.is_set():
            try:
                conn, _ = server_sock.accept()
            except socket.timeout:
                continue
            threading.Thread(target=_serve, args=(conn,), daemon=True).start()

    threading.Thread(target=accept_loop, daemon=True).start()

    # 准备远端批次
    batch = os.path.join(SFTP_ROOT, "inbound", "REAL-SFTP-BATCH")
    os.makedirs(batch)
    with open(os.path.join(batch, "results.csv"), "w", encoding="utf-8") as fp:
        fp.write("sample_no,test_item,result_value\nS1,含量,98.6\nS2,有关物质,0.2\n")
    with open(os.path.join(batch, "READY"), "w", encoding="utf-8") as fp:
        fp.write("ready\n")

    # 延迟导入，确保读到环境变量
    from app import database as db
    from app.sftp_client import SftpClient
    from app.worker import IngestWorker

    db.init_db()
    print("== 真实 paramiko SftpClient 基础操作 ==")
    client = SftpClient()
    client.ensure_dirs()
    check("inbound" in client.list_dir("/"), "listdir 根目录可见 inbound/archive")
    check(client.exists("/inbound/REAL-SFTP-BATCH/READY"), "stat 检测 READY")
    raw = client.read_file("/inbound/REAL-SFTP-BATCH/results.csv")
    check(b"98.6" in raw, "open+read 读取 results.csv")
    client.close()

    print("== 通过真实 SFTP 跑完整采集+归档 ==")
    worker = IngestWorker(client_factory=SftpClient)
    summary = worker.scan_once()
    check(summary["ingested"] == 1, f"真实 SFTP 批次入库（{summary}）")
    check(summary["archived"] == 1, f"真实 SFTP rename 归档（{summary}）")
    check(os.path.isdir(os.path.join(SFTP_ROOT, "archive", "REAL-SFTP-BATCH")),
          "远端目录已 rename 到 /archive")
    check(not os.path.exists(os.path.join(SFTP_ROOT, "inbound", "REAL-SFTP-BATCH")),
          "收件箱原目录已消失")
    with db.connect() as conn:
        row = db.batch_row(conn, "REAL-SFTP-BATCH")
        n = db.count_results(conn, "REAL-SFTP-BATCH", None, None)
        receipt = db.get_receipt(conn, int(row["id"]))
    check(row["status"] == "ARCHIVED" and n == 2 and receipt is not None,
          "数据库状态/记录/回执一致")

    print("== 归档幂等：归档区已有同名目录时不报错不覆盖 ==")
    # 直接验证 move 原语：目标已存在时不覆盖、不抛错
    client2 = SftpClient()
    os.makedirs(os.path.join(SFTP_ROOT, "inbound", "DUP-DIR"))
    open(os.path.join(SFTP_ROOT, "inbound", "DUP-DIR", "READY"), "w").write("x")
    os.makedirs(os.path.join(SFTP_ROOT, "archive", "DUP-DIR"))
    open(os.path.join(SFTP_ROOT, "archive", "DUP-DIR", "KEEP"), "w").write("original")
    client2.move("/inbound/DUP-DIR", "/archive/DUP-DIR")
    check(
        open(os.path.join(SFTP_ROOT, "archive", "DUP-DIR", "KEEP")).read() == "original",
        "move 幂等：归档区已有目录的内容不被覆盖",
    )
    client2.close()

    # 业务层：已归档批次的同名目录重投到收件箱 -> 保留现场、记录异常、不重复入库
    batch2 = os.path.join(SFTP_ROOT, "inbound", "REAL-SFTP-BATCH")
    os.makedirs(batch2)
    with open(os.path.join(batch2, "results.csv"), "w", encoding="utf-8") as fp:
        fp.write("sample_no,test_item,result_value\nS9,含量,1.0\n")
    with open(os.path.join(batch2, "READY"), "w", encoding="utf-8") as fp:
        fp.write("ready\n")
    s = worker.scan_once()
    check(os.path.exists(batch2), "重投同名目录保留在收件箱待人工核查")
    check(any("全局唯一" in e for e in s["errors"]), "扫描摘要记录全局唯一冲突异常")
    with db.connect() as conn:
        n = db.count_results(conn, "REAL-SFTP-BATCH", None, None)
    check(n == 2, "数据库不重复入库，原始 2 条记录保持不变")

    stop.set()
    server_sock.close()
    print()
    if failures:
        print(f"失败 {len(failures)} 项：{failures}")
        sys.exit(1)
    print("真实 SFTP 联调全部通过 ✔")


if __name__ == "__main__":
    main()
