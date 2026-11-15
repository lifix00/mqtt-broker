"""离线持久会话过期：只有 clean_session=0 的会话会进入过期计时。"""

import time

from mqtt_client import MQTTClient


def _connect(port, client_id):
    c = MQTTClient(port=port, client_id=client_id, clean_session=False)
    present, code = c.connect()
    assert code == 0
    return c, present


def test_offline_session_expires(broker_factory):
    port, _ = broker_factory("--session-expiry-ms", "300")

    c, present = _connect(port, "expire-me")
    assert present == 0
    assert c.subscribe("expire/#", qos=1) == (1,)
    c.disconnect()

    # 过期扫描每秒一次，TTL 300ms，等两个扫描周期
    time.sleep(2.0)

    c2, present = _connect(port, "expire-me")
    try:
        assert present == 0, "expired session must not be restored"

        # 旧订阅也随会话一起删除
        pub = MQTTClient(port=port, client_id="pub")
        pub.connect()
        try:
            pub.publish("expire/x", b"x", qos=0)
            c2.ping()
            assert c2.try_recv_publish(timeout=0.5) is None
        finally:
            pub.disconnect()
    finally:
        c2.disconnect()


def test_online_session_is_not_expired(broker_factory):
    port, _ = broker_factory("--session-expiry-ms", "1000")

    c, present = _connect(port, "online-me")
    assert present == 0
    assert c.subscribe("online/#", qos=0) == (0,)

    # 在线时间超过 TTL，但连接仍然存在，不能过期
    time.sleep(1.5)
    c.ping()
    c.disconnect()

    c2, present = _connect(port, "online-me")
    try:
        assert present == 1, "online session must survive while connected"
    finally:
        c2.disconnect()
