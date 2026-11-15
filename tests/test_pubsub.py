"""PUBLISH / SUBSCRIBE 最小环路"""

from mqtt_client import MQTTClient


def test_pub_sub_exact_topic(broker_port):
    sub = MQTTClient(port=broker_port, client_id="sub-1")
    pub = MQTTClient(port=broker_port, client_id="pub-1")
    try:
        sub.connect()
        pub.connect()

        codes = sub.subscribe("test/topic", qos=0)
        assert codes == (0,), f"SUBACK codes: {codes}"

        pub.publish("test/topic", b"hello", qos=0, retain=False)
        topic, payload, qos, retain, _ = sub.recv_publish(timeout=2)
        assert topic == "test/topic"
        assert payload == b"hello"
        assert qos == 0
        assert retain == False
    finally:
        sub.disconnect()
        pub.disconnect()


def test_pub_with_no_subscribes_is_ok(broker_port):
    """没有订阅者时发 PUBLISH 不应该断连接"""
    pub = MQTTClient(port=broker_port, client_id="pub-1")
    try:
        pub.connect()
        pub.publish("nobody/listens", b"lost", qos=0)
        # 发个PING 确认连接是否活着
        pub.ping()
    finally:
        pub.disconnect()


def test_multiple_subscribers_recive(broker_port):
    sub_a = MQTTClient(port=broker_port, client_id="sub-a")
    sub_b = MQTTClient(port=broker_port, client_id="sub-b")
    pub = MQTTClient(port=broker_port, client_id="pub")
    try:
        for c in (sub_a, sub_b, pub):
            c.connect()
        sub_a.subscribe("test/#", qos=0)
        sub_b.subscribe("test/#", qos=0)

        pub.publish("test/hello", b"hi", qos=0)

        for sub in (sub_a, sub_b):
            topic, payload, *_ = sub.recv_publish(timeout=2)
            assert topic == "test/hello"
            assert payload == b"hi"
    finally:
        for c in (sub_a, sub_b, pub):
            c.disconnect()


def test_unsubscribe_stops_delivery(broker_port):
    """UNSUBSCRIBE 后不应再收到该 filter 的消息（回归测试）"""
    sub = MQTTClient(port=broker_port, client_id="sub")
    pub = MQTTClient(port=broker_port, client_id="pub")
    try:
        sub.connect()
        pub.connect()
        sub.subscribe("a/b", qos=0)
        sub.unsubscribe("a/b")

        pub.publish("a/b", b"after-unsub", qos=0)
        assert sub.try_recv_publish(timeout=0.5) is None

        # 哨兵：确认链路仍然正常
        sub.subscribe("sentinel/ok", qos=0)
        pub.publish("sentinel/ok", b"ok", qos=0)
        got = sub.try_recv_publish(timeout=1)
        assert got is not None and got[0] == "sentinel/ok"
    finally:
        sub.disconnect()
        pub.disconnect()


def test_binary_payload(broker_port):
    """payload 是任意字节， 包括 0x00"""
    sub = MQTTClient(port=broker_port, client_id="sub")
    pub = MQTTClient(port=broker_port, client_id="pub")
    try:
        sub.connect()
        pub.connect()
        sub.subscribe("bin", qos=0)

        payload = bytes(range(256))
        pub.publish("bin", payload, qos=0)
        _, got, _, _, _ = sub.recv_publish(timeout=2)
        assert got == payload, f"payload mismatch: {got[:16].hex()}..."
    finally:
        sub.disconnect()
        pub.disconnect()


def test_large_qos0_burst_keeps_connection(broker_port):
    """持续大包入站不应把读缓冲撑到上限并断开连接。"""
    pub = MQTTClient(port=broker_port, client_id="burst-pub", timeout=10)
    try:
        pub.connect()
        payload = b"x" * 1024
        for _ in range(3000):
            pub.publish("burst/no-sub", payload, qos=0)
        pub.ping()
    finally:
        pub.disconnect()
