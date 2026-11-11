# tests/helpers.py
import pytest

def recv_exact(sock, n: int, timeout: float = 2.0) -> bytes:
    sock.settimeout(timeout)
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            break
        buf += chunk
    return buf


def expect_eof(sock, timeout: float = 2.0):
    sock.settimeout(timeout)
    data = sock.recv(16)
    assert data == b"", f"expected broker to close connection, got {data.hex()}"
