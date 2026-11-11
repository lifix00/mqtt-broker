"""Retained 消息：保存、替换、删除、QoS 与 RETAIN 标志。"""

from mqtt_client import MQTTClient


def _publish_retained(port, topic, payload, qos=0):
    pub = MQTTClient(port=port, client_id="pub")
    try:
        pub.connect()
        if qos == 1:
            pid = pub.publish(topic, payload, qos=1, retain=True)
            pub.expect_puback(pid, timeout=2)
        else:
            pub.publish(topic, payload, qos=0, retain=True)
    finally:
        pub.close()


def _subscribe_and_recv(port, topic_filter, qos=0, timeout=2):
    sub = MQTTClient(port=port, client_id="sub")
    try:
        sub.connect()
        codes = sub.subscribe(topic_filter, qos=qos)
        got = sub.try_recv_publish(timeout=timeout)
        return codes, got
    finally:
        sub.close()


def test_retained_delivered_to_new_subscriber(broker_port):
    _publish_retained(broker_port, "ret/hello", b"world")

    codes, got = _subscribe_and_recv(broker_port, "ret/#")
    assert codes == (0,)
    assert got is not None
    topic, payload, qos, retain, packet_id = got
    assert topic == "ret/hello"
    assert payload == b"world"
    assert qos == 0
    assert retain == 1, "retained delivery must set RETAIN=1"
    assert packet_id is None


def test_retained_replaced_by_latest_message(broker_port):
    _publish_retained(broker_port, "ret/value", b"first")
    _publish_retained(broker_port, "ret/value", b"second")

    _, got = _subscribe_and_recv(broker_port, "ret/value")
    assert got is not None
    assert got[1] == b"second"
    assert got[3] == 1


def test_retained_cleared_by_empty_payload(broker_port):
    _publish_retained(broker_port, "ret/gone", b"value")
    _publish_retained(broker_port, "ret/gone", b"")

    _, got = _subscribe_and_recv(broker_port, "ret/#", timeout=0.6)
    assert got is None, "empty retained payload should delete the retained message"


def test_retained_qos1_delivered_with_qos1_and_inflight(broker_port):
    _publish_retained(broker_port, "ret/q1", b"payload", qos=1)

    sub = MQTTClient(port=broker_port, client_id="sub")
    try:
        sub.connect()
        assert sub.subscribe("ret/q1", qos=1) == (1,)
        topic, payload, qos, retain, packet_id, dup = sub.recv_publish_ex(
            timeout=2
        )
        assert topic == "ret/q1"
        assert payload == b"payload"
        assert qos == 1
        assert retain == 1
        assert dup == 0
        assert packet_id is not None

        sub.send_puback(packet_id)
        assert sub.try_recv_publish(timeout=2.5) is None
    finally:
        sub.close()


def test_retained_live_delivery_has_retain_zero(broker_port):
    live = MQTTClient(port=broker_port, client_id="live")
    pub = MQTTClient(port=broker_port, client_id="pub")
    try:
        live.connect()
        pub.connect()
        assert live.subscribe("ret/live", qos=0) == (0,)

        pub.publish("ret/live", b"now", qos=0, retain=True)

        topic, payload, qos, retain, _ = live.recv_publish(timeout=2)
        assert topic == "ret/live"
        assert payload == b"now"
        assert retain == 0, "live forwarding must clear RETAIN"

        # 新订阅者才应该看到 RETAIN=1
        _, got = _subscribe_and_recv(broker_port, "ret/live")
        assert got is not None
        assert got[1] == b"now"
        assert got[3] == 1
    finally:
        live.close()
        pub.close()
