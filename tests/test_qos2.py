"""QoS2：入站/出站握手、重复包、超时重传、retained。"""

import time

from mqtt_client import MQTTClient

QOS2_RETRY_TIMEOUT = 2.0


def _complete_inbound(pub, packet_id):
    """发布者侧完成 PUBLISH -> PUBREC -> PUBREL -> PUBCOMP。"""
    assert pub.expect_pubrec(packet_id, timeout=2) == packet_id
    pub.send_pubrel(packet_id)
    assert pub.expect_pubcomp(packet_id, timeout=2) == packet_id


def _complete_outbound(sub, packet_id):
    """订阅者侧完成 PUBREC -> PUBREL -> PUBCOMP。"""
    sub.send_pubrec(packet_id)
    assert sub.expect_pubrel(packet_id, timeout=2) == packet_id
    sub.send_pubcomp(packet_id)


def test_inbound_qos2_handshake(broker_port):
    pub = MQTTClient(port=broker_port, client_id="pub")
    try:
        pub.connect()
        pid = pub.publish("q2/handshake", b"hello", qos=2)
        _complete_inbound(pub, pid)
    finally:
        pub.close()


def test_qos2_not_delivered_before_pubrel(broker_port):
    sub = MQTTClient(port=broker_port, client_id="sub")
    pub = MQTTClient(port=broker_port, client_id="pub")
    try:
        sub.connect()
        pub.connect()
        assert sub.subscribe("q2/#", qos=0) == (0,)

        pid = pub.publish("q2/deliver", b"payload", qos=2)
        assert pub.expect_pubrec(pid, timeout=2) == pid

        # 还没收到 PUBREL，不能投递
        assert sub.try_recv_publish(timeout=0.4) is None

        pub.send_pubrel(pid)
        assert pub.expect_pubcomp(pid, timeout=2) == pid

        topic, payload, qos, _, _ = sub.recv_publish(timeout=2)
        assert (topic, payload, qos) == ("q2/deliver", b"payload", 0)
    finally:
        sub.close()
        pub.close()


def test_duplicate_qos2_publish_delivers_once(broker_port):
    sub = MQTTClient(port=broker_port, client_id="sub")
    pub = MQTTClient(port=broker_port, client_id="pub")
    try:
        sub.connect()
        pub.connect()
        assert sub.subscribe("q2/dup", qos=0) == (0,)

        pid = pub.publish("q2/dup", b"once", qos=2)
        assert pub.expect_pubrec(pid, timeout=2) == pid

        # 同一 packet id 的重复 PUBLISH，只重发 PUBREC，不重复投递
        pub.publish("q2/dup", b"once", qos=2, packet_id=pid, dup=True)
        assert pub.expect_pubrec(pid, timeout=2) == pid

        pub.send_pubrel(pid)
        assert pub.expect_pubcomp(pid, timeout=2) == pid

        topic, payload, _, _, _ = sub.recv_publish(timeout=2)
        assert (topic, payload) == ("q2/dup", b"once")
        assert sub.try_recv_publish(timeout=0.5) is None
    finally:
        sub.close()
        pub.close()


def test_qos2_outbound_handshake(broker_port):
    sub = MQTTClient(port=broker_port, client_id="sub")
    pub = MQTTClient(port=broker_port, client_id="pub")
    try:
        sub.connect()
        pub.connect()
        assert sub.subscribe("q2/out", qos=2) == (2,)

        pid = pub.publish("q2/out", b"outbound", qos=2)
        _complete_inbound(pub, pid)

        topic, payload, qos, _, sub_pid, dup = sub.recv_publish_ex(timeout=2)
        assert (topic, payload, qos, dup) == ("q2/out", b"outbound", 2, 0)
        assert sub_pid is not None

        _complete_outbound(sub, sub_pid)
        assert sub.try_recv_publish(timeout=0.5) is None
    finally:
        sub.close()
        pub.close()


def test_qos2_outbound_retransmits_publish(broker_port):
    sub = MQTTClient(port=broker_port, client_id="sub")
    pub = MQTTClient(port=broker_port, client_id="pub")
    try:
        sub.connect()
        pub.connect()
        assert sub.subscribe("q2/retry", qos=2) == (2,)

        pid = pub.publish("q2/retry", b"retry", qos=2)
        _complete_inbound(pub, pid)

        first = sub.recv_publish_ex(timeout=2)
        assert first[2] == 2
        assert first[5] == 0
        first_pid = first[4]

        # 不回 PUBREC，broker 应 DUP=1 重发同一个 packet id
        second = sub.recv_publish_ex(timeout=QOS2_RETRY_TIMEOUT + 2)
        assert second[4] == first_pid
        assert second[5] == 1
        assert second[1] == b"retry"

        _complete_outbound(sub, first_pid)
        assert sub.try_recv_publish(timeout=0.5) is None
    finally:
        sub.close()
        pub.close()


def test_qos2_outbound_retransmits_pubrel(broker_port):
    sub = MQTTClient(port=broker_port, client_id="sub")
    pub = MQTTClient(port=broker_port, client_id="pub")
    try:
        sub.connect()
        pub.connect()
        assert sub.subscribe("q2/rel", qos=2) == (2,)

        pid = pub.publish("q2/rel", b"rel", qos=2)
        _complete_inbound(pub, pid)

        entry = sub.recv_publish_ex(timeout=2)
        sub.send_pubrec(entry[4])

        assert sub.expect_pubrel(entry[4], timeout=2) == entry[4]

        # 不回 PUBCOMP，broker 应重发 PUBREL，而不是 PUBLISH
        again = sub.expect_pubrel(entry[4], timeout=QOS2_RETRY_TIMEOUT + 2)
        assert again == entry[4]

        sub.send_pubcomp(entry[4])
        time.sleep(0.3)
        assert sub.try_recv_publish(timeout=0.5) is None
    finally:
        sub.close()
        pub.close()


def test_qos2_retained_message(broker_port):
    pub = MQTTClient(port=broker_port, client_id="pub")
    try:
        pub.connect()
        pid = pub.publish("q2/retained", b"stored", qos=2, retain=True)
        _complete_inbound(pub, pid)
    finally:
        pub.close()

    sub = MQTTClient(port=broker_port, client_id="sub")
    try:
        sub.connect()
        assert sub.subscribe("q2/#", qos=2) == (2,)
        topic, payload, qos, retain, packet_id, dup = sub.recv_publish_ex(
            timeout=2
        )
        assert (topic, payload, qos, retain, dup) == (
            "q2/retained",
            b"stored",
            2,
            1,
            0,
        )
        _complete_outbound(sub, packet_id)
    finally:
        sub.close()
