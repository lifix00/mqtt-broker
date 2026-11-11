"""持久会话（clean_session=0）与 QoS1 断线恢复。"""

from mqtt_client import MQTTClient


def _connect(port, client_id, clean_session=False):
    c = MQTTClient(port=port, client_id=client_id, clean_session=clean_session)
    present, code = c.connect()
    assert code == 0, f"CONNACK return code {code}"
    return c, present


def test_persistent_session_queues_qos1_while_offline(broker_port):
    """clean_session=0 的订阅者离线期间，QoS1 消息应入队并在重连后投递。"""
    sub, present = _connect(broker_port, "persist-queue")
    assert present == 0
    assert sub.subscribe("offline/#", qos=1) == (1,)
    sub.disconnect()

    pub = MQTTClient(port=broker_port, client_id="pub")
    pub.connect()
    try:
        pid = pub.publish("offline/x", b"queued", qos=1)
        pub.expect_puback(pid, timeout=2)
    finally:
        pub.close()

    sub2, present = _connect(broker_port, "persist-queue")
    try:
        assert present == 1, "reconnect should resume the stored session"
        topic, payload, qos, retain, packet_id, dup = sub2.recv_publish_ex(
            timeout=2
        )
        assert topic == "offline/x"
        assert payload == b"queued"
        assert qos == 1
        assert retain == 0
        assert dup == 0
        assert packet_id is not None
        sub2.send_puback(packet_id)
    finally:
        sub2.disconnect()


def test_unacked_qos1_resent_with_dup_after_reconnect(broker_port):
    """未回 PUBACK 的消息在重连后应以原 packet id、DUP=1 重发。"""
    sub, _ = _connect(broker_port, "resume-dup")
    assert sub.subscribe("resume/#", qos=1) == (1,)

    pub = MQTTClient(port=broker_port, client_id="pub")
    pub.connect()
    try:
        pub_pid = pub.publish("resume/x", b"unacked", qos=1)
        pub.expect_puback(pub_pid, timeout=2)

        first = sub.recv_publish_ex(timeout=2)
        assert first[1] == b"unacked"
        assert first[2] == 1
        assert first[5] == 0, "first transmission must not set DUP"
        old_pid = first[4]

        # 不回 PUBACK，直接断开（模拟网络中断）
        sub.close()

        sub2, present = _connect(broker_port, "resume-dup")
        try:
            assert present == 1
            second = sub2.recv_publish_ex(timeout=2)
            assert second[0] == "resume/x"
            assert second[1] == b"unacked"
            assert second[4] == old_pid, "resume must reuse the original pid"
            assert second[5] == 1, "resumed PUBLISH must set DUP"
            sub2.send_puback(second[4])
        finally:
            sub2.disconnect()
    finally:
        pub.close()


def test_clean_session_discards_persistent_state(broker_port):
    """重连时带 clean_session=1 应丢弃旧会话及其订阅。"""
    sub, _ = _connect(broker_port, "discard-me")
    assert sub.subscribe("discard/#", qos=1) == (1,)
    sub.disconnect()

    sub2, present = _connect(broker_port, "discard-me", clean_session=True)
    try:
        assert present == 0, "clean session must not resume old state"
        pub = MQTTClient(port=broker_port, client_id="pub")
        pub.connect()
        try:
            pid = pub.publish("discard/x", b"lost", qos=1)
            pub.expect_puback(pid, timeout=2)
        finally:
            pub.close()
        assert sub2.try_recv_publish(timeout=0.5) is None
    finally:
        sub2.disconnect()


def test_clean_session_does_not_queue_offline_messages(broker_port):
    """clean_session=1 的连接断开后会话即销毁，不应有离线队列。"""
    sub, _ = _connect(broker_port, "ephemeral", clean_session=True)
    assert sub.subscribe("gone/#", qos=1) == (1,)
    sub.disconnect()

    pub = MQTTClient(port=broker_port, client_id="pub")
    pub.connect()
    try:
        pid = pub.publish("gone/x", b"not-queued", qos=1)
        pub.expect_puback(pid, timeout=2)
    finally:
        pub.close()

    sub2, present = _connect(broker_port, "ephemeral", clean_session=True)
    try:
        assert present == 0
        assert sub2.try_recv_publish(timeout=0.5) is None
    finally:
        sub2.disconnect()


def test_qos0_is_not_queued_for_offline_session(broker_port):
    """持久会话只保存 QoS1；离线期间的 QoS0 消息应丢弃。"""
    sub, _ = _connect(broker_port, "qos0-offline")
    assert sub.subscribe("q0/#", qos=1) == (1,)
    sub.disconnect()

    pub = MQTTClient(port=broker_port, client_id="pub")
    pub.connect()
    try:
        pub.publish("q0/x", b"dropped", qos=0)
    finally:
        pub.close()

    sub2, present = _connect(broker_port, "qos0-offline")
    try:
        assert present == 1
        assert sub2.try_recv_publish(timeout=0.5) is None
    finally:
        sub2.disconnect()


def test_same_client_id_takeover_keeps_session(broker_port):
    """同 client id 新连接接管旧连接，旧连接被关闭，订阅保留。"""
    old, present = _connect(broker_port, "same-id")
    assert present == 0
    assert old.subscribe("take/#", qos=1) == (1,)

    new, present = _connect(broker_port, "same-id")
    try:
        assert present == 1, "takeover should resume the existing session"

        old.sock.settimeout(2)
        assert old.sock.recv(16) == b"", "old connection should be closed"

        pub = MQTTClient(port=broker_port, client_id="pub")
        pub.connect()
        try:
            pid = pub.publish("take/x", b"after-takeover", qos=1)
            pub.expect_puback(pid, timeout=2)
        finally:
            pub.close()

        topic, payload, qos, _, packet_id, dup = new.recv_publish_ex(
            timeout=2
        )
        assert (topic, payload, qos, dup) == (
            "take/x",
            b"after-takeover",
            1,
            0,
        )
        new.send_puback(packet_id)
    finally:
        new.disconnect()
        old.close()
