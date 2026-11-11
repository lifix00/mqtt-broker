"""Keep Alive：超时断开，PINGREQ 续命，超时后发布 Will。"""

import time

from mqtt_client import MQTTClient


def test_keepalive_timeout_closes_connection(broker_port):
    c = MQTTClient(port=broker_port, client_id="ka-timeout", keep_alive=1)
    try:
        c.connect()
        c.sock.settimeout(4)
        data = c.sock.recv(16)
        assert data == b"", f"broker should close idle connection, got {data.hex()}"
    finally:
        c.close()


def test_ping_keeps_connection_alive(broker_port):
    c = MQTTClient(port=broker_port, client_id="ka-ping", keep_alive=2)
    try:
        c.connect()
        # keep_alive=2，超时阈值 3s；每 1s 发一次 PINGREQ 应保持连接
        for _ in range(3):
            time.sleep(1.0)
            c.ping()
        c.ping()
    finally:
        c.disconnect()


def test_keepalive_timeout_publishes_will(broker_port):
    sub = MQTTClient(port=broker_port, client_id="ka-sub")
    sub.connect()
    assert sub.subscribe("ka/#", qos=0) == (0,)

    c = MQTTClient(
        port=broker_port,
        client_id="ka-will",
        keep_alive=1,
        will_topic="ka/will",
        will_payload=b"timeout",
        will_qos=0,
    )
    try:
        c.connect()
        # 不发送任何报文，等待 broker 在 1.5s 后判定超时并发布 Will
        topic, payload, qos, retain, _ = sub.recv_publish(timeout=4)
        assert topic == "ka/will"
        assert payload == b"timeout"
        assert qos == 0
        assert retain == 0
    finally:
        c.close()
        sub.close()
