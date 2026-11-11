"""QoS 1 PUBLISH: 入站 PUBACK、出站 QoS1、DUP 重传。"""

import time

from mqtt_client import MQTTClient

# 与 broker 的 QOS1_RETRY_TIMEOUT_MS 保持一致（2s）
QOS1_RETRY_TIMEOUT = 2.0


def test_pub_qos1_gets_puback(broker_port):
    """QoS 1 PUBLISH → broker 必须回 PUBACK（同 packet_id）"""
    pub = MQTTClient(port=broker_port, client_id="pub")
    try:
        pub.connect()
        pid = pub.publish("test/x", b"hello", qos=1)
        got = pub.expect_puback(pid, timeout=2)
        assert got == pid
    finally:
        pub.close()


def test_pub_qos1_no_subscribers_still_pubacks(broker_port):
    """无订阅者时也必须回 PUBACK"""
    pub = MQTTClient(port=broker_port, client_id="pub")
    try:
        pub.connect()
        pid = pub.publish("nobody/listens", b"x", qos=1)
        got = pub.expect_puback(pid, timeout=2)
        assert got == pid
    finally:
        pub.close()


def test_pub_qos1_multiple_in_sequence(broker_port):
    """连续发多条 QoS 1，每个 PID 都要有对应 PUBACK，顺序可乱但 ID 不能丢"""
    pub = MQTTClient(port=broker_port, client_id="pub")
    try:
        pub.connect()
        pids = [pub.publish("t", f"m{i}".encode(), qos=1) for i in range(5)]
        got = sorted(pub.expect_puback(timeout=2) for _ in range(5))
        assert got == sorted(pids)
    finally:
        pub.close()


def test_pub_qos1_to_sub_delivers_qos0(broker_port):
    """pub QoS 1 → sub QoS 0：订阅者收到 QoS 0（无 PID）"""
    sub = MQTTClient(port=broker_port, client_id="sub")
    pub = MQTTClient(port=broker_port, client_id="pub")
    try:
        sub.connect()
        pub.connect()
        sub.subscribe("test/#", qos=0)

        pid = pub.publish("test/x", b"hi", qos=1)
        pub.expect_puback(pid, timeout=2)

        topic, payload, qos, retain, got_pid = sub.recv_publish(timeout=2)
        assert topic == "test/x"
        assert payload == b"hi"
        assert qos == 0
        assert got_pid is None
    finally:
        sub.close()
        pub.close()


def test_pub_qos1_to_sub_qos1_delivers_qos1(broker_port):
    """pub QoS 1 + sub QoS 1：订阅者收到 QoS 1，并带回可确认的 packet id"""
    sub = MQTTClient(port=broker_port, client_id="sub")
    pub = MQTTClient(port=broker_port, client_id="pub")
    try:
        sub.connect()
        pub.connect()
        codes = sub.subscribe("test/#", qos=1)
        # broker 应返回 granted qos 1
        assert codes == (1,), f"SUBACK granted qos: {codes}"

        pid = pub.publish("test/x", b"hi", qos=1)
        pub.expect_puback(pid, timeout=2)

        topic, payload, qos, retain, got_pid, dup = sub.recv_publish_ex(timeout=2)
        assert topic == "test/x"
        assert payload == b"hi"
        assert qos == 1, "broker should deliver QoS 1 to a QoS 1 subscriber"
        assert got_pid is not None and 1 <= got_pid <= 65535
        assert dup == 0

        # 回 PUBACK，broker 应清理 inflight，不再重传
        sub.send_puback(got_pid)
        assert sub.try_recv_publish(timeout=QOS1_RETRY_TIMEOUT + 0.5) is None
    finally:
        sub.close()
        pub.close()


def test_puback_preserves_high_packet_id(broker_port):
    """packet id 高字节不能被截断：0x1234 -> PUBACK 0x1234（回归测试）"""
    pub = MQTTClient(port=broker_port, client_id="pub")
    try:
        pub.connect()
        pid = pub.publish("t", b"x", qos=1, packet_id=0x1234)
        assert pub.expect_puback(pid, timeout=2) == 0x1234
    finally:
        pub.close()


def test_qos1_multiple_messages_unique_packet_ids(broker_port):
    """同一订阅者连续收到多条 QoS1，packet id 互不重复，逐条 PUBACK 都能清掉"""
    sub = MQTTClient(port=broker_port, client_id="sub")
    pub = MQTTClient(port=broker_port, client_id="pub")
    try:
        sub.connect()
        pub.connect()
        assert sub.subscribe("q1/multi", qos=1) == (1,)

        for i in range(5):
            pid = pub.publish("q1/multi", f"m{i}".encode(), qos=1)
            pub.expect_puback(pid, timeout=2)

        pids = set()
        for _ in range(5):
            topic, payload, qos, _, got_pid, dup = sub.recv_publish_ex(timeout=2)
            assert topic == "q1/multi"
            assert qos == 1
            assert dup == 0
            assert got_pid not in pids, f"duplicate packet id {got_pid}"
            pids.add(got_pid)
            sub.send_puback(got_pid)

        assert len(pids) == 5
        assert sub.try_recv_publish(timeout=QOS1_RETRY_TIMEOUT + 0.5) is None
    finally:
        sub.close()
        pub.close()


def test_qos1_retransmits_with_dup_until_puback(broker_port):
    """订阅者不回 PUBACK 时，broker 超时重传，且重传包 DUP=1、packet id 不变"""
    sub = MQTTClient(port=broker_port, client_id="sub")
    pub = MQTTClient(port=broker_port, client_id="pub")
    try:
        sub.connect()
        pub.connect()
        assert sub.subscribe("q1/retry", qos=1) == (1,)

        pub_pid = pub.publish("q1/retry", b"retry-me", qos=1)
        pub.expect_puback(pub_pid, timeout=2)

        first = sub.recv_publish_ex(timeout=2)
        assert first[0] == "q1/retry"
        assert first[1] == b"retry-me"
        assert first[2] == 1
        first_pid = first[4]
        assert first[5] == 0, "first transmission must not set DUP"

        second = sub.recv_publish_ex(timeout=QOS1_RETRY_TIMEOUT + 2)
        assert second[4] == first_pid, "retransmission must reuse packet id"
        assert second[5] == 1, "retransmission must set DUP"
        assert second[1] == b"retry-me"

        sub.send_puback(first_pid)
        # 给 broker 一点时间处理 PUBACK，确认之后不再重传
        time.sleep(0.3)
        assert sub.try_recv_publish(timeout=QOS1_RETRY_TIMEOUT + 0.5) is None
    finally:
        sub.close()
        pub.close()


def test_pub_qos0_still_works(broker_port):
    """QoS 0 路径回归"""
    sub = MQTTClient(port=broker_port, client_id="sub")
    pub = MQTTClient(port=broker_port, client_id="pub")
    try:
        sub.connect()
        pub.connect()
        sub.subscribe("t", qos=0)
        pub.publish("t", b"x", qos=0)
        topic, payload, qos, _, _ = sub.recv_publish(timeout=2)
        assert topic == "t"
        assert payload == b"x"
        assert qos == 0
    finally:
        sub.close()
        pub.close()


def test_pub_qos2_rejected(broker_port):
    pub = MQTTClient(port=broker_port, client_id="pub")
    try:
        pub.connect()
        pub.publish("t", b"x", qos=2)
        data = pub.sock.recv(16)
        assert data == b"", f"expected EOF, got {data.hex()}"
    finally:
        pub.close()