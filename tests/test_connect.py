"""CONNECT / CONNACK"""

import socket

from mqtt_client import MQTTClient


def test_connect_accept_normal_client(broker_port):
    c = MQTTClient(port=broker_port, client_id="client-a")
    try:
        session_present, code = c.connect()
        assert code == 0, f"expected CONNACK 0, got {code}"
        assert session_present == 0
    finally:
        c.disconnect()


def test_connect_rejects_bad_protocol_level(broker_port):
    """协议级别不等于 4 应返回 CONNACK code 1 然后断开。"""
    s = socket.create_connection(("127.0.0.1", broker_port), timeout=2)
    try:
        # protocol name "MQTT",level=5, clean, keepalive=60, client id "x"
        payload = (
            b"\x00\x04MQTT"  # protocol name
            b"\x05"  # protocol level = 5 (非法)
            b"\x02"  # flags: clean
            b"\x00\x3c"  # keep alive
            b"\x00\x01x"  # client ID 'x'
        )
        pkt = b"\x10" + bytes([len(payload)]) + payload
        s.sendall(pkt)

        data = s.recv(16)
        assert data[:2] == b"\x20\x02", f"expected CONNACK, got {data.hex()}"
        assert data[3] == 1, f"expected return code 1, got {data[3]}"

        # 之后 broker 应该关闭连接
        rest = s.recv(16)
        assert rest == b"", "broker should close after refusing CONNACK"
    finally:
        s.close()


def test_connect_with_empty_client_id_and_clean_session(broker_port):
    """空 client id + clean session=1 是允许的"""
    c = MQTTClient(port=broker_port, client_id="", clean_session=True)
    try:
        _, code = c.connect()
        assert code == 0
    finally:
        c.disconnect()


def test_connect_with_empty_client_id_and_present_session(broker_port):
    """空client id + clean session=0 应该被拒绝，返回码2"""
    s = socket.create_connection(("127.0.0.1", broker_port), timeout=2)
    try:
        payload = (
            b"\x00\x04MQTT"  # protocol name
            b"\x04"  # protocol level
            b"\x00"  # connect flags (clean session=0)
            b"\x00\x3c"  # keep alive
            b"\x00\x00"  # 空的 client id
        )
        pkt = b"\x10" + bytes([len(payload)]) + payload
        s.sendall(pkt)

        data = s.recv(16)
        assert data[:2] == b"\x20\x02", f"expected CONNACK, got {data.hex()}"
        assert data[3] == 2, f"expected return code 2, got {data[3]}"
    finally:
        s.close()
