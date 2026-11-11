"""共享 fixture：启动/停止 broker 进程，随机端口"""

import socket
from pathlib import Path
import pytest
import time
import subprocess
import signal

BROKER_BIN = Path(__file__).resolve().parent.parent / "mqtt-broker"


def _free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _wait_port(host: str, port: int, timeout: float = 3.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            s = socket.create_connection((host, port), timeout=0.2)
            s.close()
            return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError(f"broker did not start listening on {host}:{port}")


@pytest.fixture
def broker_port(tmp_path):
    """启动 broker，返回端口；测试结束 kill 掉"""
    if not BROKER_BIN.exists():
        pytest.skip(f"broker binary not found: {BROKER_BIN} (run make first)")

    port = _free_port()
    log_path = tmp_path / f"broker-{port}.log"

    log_write_f = open(log_path, "w")
    proc = subprocess.Popen(
        [str(BROKER_BIN), "--port", str(port)],
        stdout=log_write_f,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        _wait_port("127.0.0.1", port)
        yield port
    finally:
        if proc.poll() is None:
            proc.send_signal(signal.SIGTERM)
            try:
                proc.wait(timeout=2)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()

        log_write_f.close()

        if log_path.exists():
            content = log_path.read_text(errors="replace")
            if content:
                print(f"\n--- broker log ({log_path}) ---\n{content}\n--------")
        # tmp_path 由 pytest 自动清理
