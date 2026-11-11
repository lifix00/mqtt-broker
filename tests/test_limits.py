"""资源上限与大报文：会话/retained/订阅上限，动态读写缓冲。"""

from mqtt_client import MQTTClient


def _persistent(port, client_id):
    c = MQTTClient(port=port, client_id=client_id, clean_session=False)
    present, _ = c.connect()
    return c, present


def test_max_sessions_evicts_oldest_offline(broker_factory):
    port, _ = broker_factory("--max-sessions", "2")

    s1, present = _persistent(port, "s1")
    assert present == 0
    s1.disconnect()

    s2, present = _persistent(port, "s2")
    assert present == 0
    s2.disconnect()

    # 创建第三个持久会话，应该淘汰最久未活动的 s1
    s3, present = _persistent(port, "s3")
    assert present == 0
    s3.disconnect()

    # s2 仍然在
    s2b, present = _persistent(port, "s2")
    assert present == 1
    s2b.disconnect()

    # s1 已被淘汰，重连时 session_present=0
    s1b, present = _persistent(port, "s1")
    assert present == 0
    s1b.disconnect()


def test_max_retained_evicts_oldest(broker_factory):
    port, _ = broker_factory("--max-retained", "2")

    pub = MQTTClient(port=port, client_id="pub")
    pub.connect()
    try:
        for i in range(3):
            pub.publish(f"r/{i}", f"v{i}".encode(), qos=0, retain=True)
            pub.ping()  # 确保每条都被 Broker 处理
    finally:
        pub.close()

    sub = MQTTClient(port=port, client_id="sub")
    sub.connect()
    try:
        assert sub.subscribe("r/#", qos=0) == (0,)
        topics = []
        for _ in range(2):
            got = sub.try_recv_publish(timeout=1)
            assert got is not None
            topics.append(got[0])
        assert set(topics) == {"r/1", "r/2"}, f"oldest retained should be evicted: {topics}"
        assert sub.try_recv_publish(timeout=0.4) is None
    finally:
        sub.close()


def test_max_subscriptions_returns_failure(broker_factory):
    port, _ = broker_factory("--max-subscriptions", "1")

    sub = MQTTClient(port=port, client_id="sub")
    pub = MQTTClient(port=port, client_id="pub")
    try:
        sub.connect()
        pub.connect()

        assert sub.subscribe("a", qos=0) == (0,)
        assert sub.subscribe("b", qos=0) == (0x80,)

        pub.publish("a", b"ok", qos=0)
        topic, payload, *_ = sub.recv_publish(timeout=2)
        assert (topic, payload) == ("a", b"ok")

        pub.publish("b", b"rejected", qos=0)
        assert sub.try_recv_publish(timeout=0.4) is None
    finally:
        sub.close()
        pub.close()


def test_large_payload_uses_dynamic_buffers(broker_port):
    sub = MQTTClient(port=broker_port, client_id="sub", timeout=5)
    pub = MQTTClient(port=broker_port, client_id="pub", timeout=5)
    try:
        sub.connect()
        pub.connect()
        assert sub.subscribe("big", qos=0) == (0,)

        payload = bytes((i * 7) % 256 for i in range(128 * 1024))
        pub.publish("big", payload, qos=0)

        topic, got, qos, _, _ = sub.recv_publish(timeout=5)
        assert topic == "big"
        assert qos == 0
        assert got == payload, f"payload mismatch: {len(got)} != {len(payload)}"
    finally:
        sub.close()
        pub.close()
