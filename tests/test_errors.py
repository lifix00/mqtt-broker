"""非法报文应导致断开连接，不应崩溃或 hang"""
import socket

def _send_and_expect_close(port: int, pkt: bytes, timeout=2.0):
    s = socket.create_connection(("127.0.0.1", port), timeout=timeout)
    try:
        s.sendall(pkt)
        data = s.recv(16)
        assert data == b"", f"expected close, got {data.hex()}"
    finally:
        s.close()


def test_invalid_packet_type(broker_port):
    """type=15 非法"""
    _send_and_expect_close(broker_port, b"\xf0\x00")


def test_type_0_illegal(broker_port):
    """type=0 保留，非法"""
    _send_and_expect_close(broker_port, b"\x00\x00")


def test_connect_with_bad_flags(broker_port):
    """CONNECT 固定头 flags 必须 0，0x11 非法"""
    _send_and_expect_close(broker_port, b"\x11\x00")

def test_subscribe_with_bad_flags(broker_port):
    """SUBSCRIBE 固定头必须是 0x82，0x80 非法"""
    # 先发一个正常 CONNECT
    s = socket.create_connection(("127.0.0.1", broker_port), timeout=2)
    try:
        payload = (
            b"\x00\x04MQTT\x04\x02\x00\x3c\x00\x01t"
        )
        s.sendall(b"\x10" + bytes([len(payload)]) + payload)
        _ = s.recv(4)  # CONNACK

        # 发一个 flags=0（而非 0x2）的 SUBSCRIBE，最小 body: mid + filter + qos
        body = b"\x00\x01" + b"\x00\x03abc" + b"\x00"
        s.sendall(b"\x80" + bytes([len(body)]) + body)
        data = s.recv(16)
        assert data == b"", "broker should reject SUBSCRIBE with flags=0"
    finally:
        s.close()
