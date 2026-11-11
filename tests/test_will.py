"""Will 消息：异常断开发布，正常 DISCONNECT 不发布。"""

from mqtt_client import MQTTClient


def _make_subscriber(port, topic_filter="will/#", qos=0):
    sub = MQTTClient(port=port, client_id="will-sub")
    sub.connect()
    assert sub.subscribe(topic_filter, qos=qos) == (qos,)
    return sub


def test_will_published_on_abnormal_disconnect(broker_port):
    sub = _make_subscriber(broker_port)
    will_client = MQTTClient(
        port=broker_port,
        client_id="will-client",
        will_topic="will/x",
        will_payload=b"gone",
        will_qos=0,
    )
    try:
        will_client.connect()
        will_client.close()  # 直接关 socket，等价于网络异常断开

        topic, payload, qos, retain, packet_id = sub.recv_publish(timeout=3)
        assert topic == "will/x"
        assert payload == b"gone"
        assert qos == 0
        assert retain == 0
        assert packet_id is None
    finally:
        sub.close()
        will_client.close()


def test_no_will_on_normal_disconnect(broker_port):
    sub = _make_subscriber(broker_port)
    will_client = MQTTClient(
        port=broker_port,
        client_id="will-client",
        will_topic="will/x",
        will_payload=b"should-not-appear",
    )
    try:
        will_client.connect()
        will_client.disconnect()  # 正常 DISCONNECT，Will 必须丢弃

        assert sub.try_recv_publish(timeout=1) is None
    finally:
        sub.close()


def test_will_qos1_and_retain(broker_port):
    sub = _make_subscriber(broker_port, "will/#", qos=1)
    will_client = MQTTClient(
        port=broker_port,
        client_id="will-client",
        will_topic="will/q1",
        will_payload=b"retained-will",
        will_qos=1,
        will_retain=True,
    )
    try:
        will_client.connect()
        will_client.close()

        # 实时投递：QoS1，但 RETAIN 必须是 0
        topic, payload, qos, retain, packet_id, dup = sub.recv_publish_ex(
            timeout=3
        )
        assert (topic, payload, qos, retain, dup) == (
            "will/q1",
            b"retained-will",
            1,
            0,
            0,
        )
        sub.send_puback(packet_id)

        # 新订阅者应拿到 retained 版本的 Will，RETAIN=1
        sub2 = MQTTClient(port=broker_port, client_id="will-sub2")
        try:
            sub2.connect()
            assert sub2.subscribe("will/q1", qos=1) == (1,)
            entry = sub2.recv_publish_ex(timeout=2)
            assert entry[0] == "will/q1"
            assert entry[1] == b"retained-will"
            assert entry[2] == 1
            assert entry[3] == 1
            sub2.send_puback(entry[4])
        finally:
            sub2.close()
    finally:
        sub.close()
        will_client.close()


def test_will_published_on_protocol_error(broker_port):
    sub = _make_subscriber(broker_port)
    will_client = MQTTClient(
        port=broker_port,
        client_id="will-client",
        will_topic="will/proto",
        will_payload=b"bad-packet",
        will_qos=0,
    )
    try:
        will_client.connect()
        # type=15 是非法包类型，broker 会断开连接
        will_client.sock.sendall(b"\xf0\x00")

        topic, payload, _, _, _ = sub.recv_publish(timeout=3)
        assert topic == "will/proto"
        assert payload == b"bad-packet"
    finally:
        sub.close()
        will_client.close()
