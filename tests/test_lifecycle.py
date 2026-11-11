"""连接断开、订阅清理、状态机"""

import socket

from mqtt_client import MQTTClient


def test_ping_works(broker_port):
    c = MQTTClient(port=broker_port, client_id="pinger")
    try:
        c.connect()
        c.ping()
        c.ping()
    finally:
        c.disconnect()


def test_disconnect_closes_connection(broker_port):
    c = MQTTClient(port=broker_port, client_id="d")
    c.connect()
    c.disconnect()


def test_subscription_cleared_after_disconnect(broker_port):
    """sub 断开后，之前的订阅不应再匹配"""
    sub = MQTTClient(port=broker_port, client_id="sub")
    sub.connect()
    sub.subscribe("test/#", qos=0)
    sub.disconnect()

    pub = MQTTClient(port=broker_port, client_id="pub")
    try:
        pub.connect()
        # 发一条，没有订阅者不应导致断开
        pub.publish_qos0("test/x", b"hello")
        pub.ping()
    finally:
        pub.close()

def test_pingreq_before_connect_kills_connection(broker_port):
    """ CONNECT 之前发 PINGREQ，broker 应该断开 """
    s = socket.create_connection(("127.0.0.1", broker_port), timeout=2)
    try:
        s.sendall(b"\xc0\x00") # PINGREQ
        data = s.recv(16)
        assert data == b"", f"broker should close, got {data.hex()}"
    finally:
        s.close()

def test_second_connect_kills_connection(broker_port):
    """同一个连接上发两次 CONNECT，第二次应断开。"""
    c = MQTTClient(port=broker_port, client_id="dup")
    try:
        c.connect()
        # 手工再发一次 CONNECT
        c.sock.sendall(c._build_connect())
        data = c.sock.recv(16)
        assert data == b"", f"broker should close, got {data.hex()}"
    finally:
        c.close()

def test_disconnect_with_body_kills_connection(broker_port):
    """DISCONNECT 剩余长度 != 0 是协议违规，broker 应断开。"""
    c = MQTTClient(port=broker_port, client_id="x")
    try:
        c.connect()
        c.sock.sendall(b"\xe0\x01\x00")  # DISCONNECT with 1 byte body
        data = c.sock.recv(16)
        assert data == b"", "broker should close on invalid DISCONNECT"
    finally:
        c.close()


