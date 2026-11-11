"""主题匹配规则 （MQTT 3.1.1）"""

import pytest

from mqtt_client import MQTTClient


@pytest.fixture
def pair(broker_port):
    sub = MQTTClient(port=broker_port, client_id="sub")
    pub = MQTTClient(port=broker_port, client_id="pub")
    sub.connect()
    pub.connect()
    try:
        yield sub, pub
    finally:
        sub.disconnect()
        pub.disconnect()


def _expect_match(sub, pub, filter_topic, pub_topic, should_match):
    sub.subscribe(filter_topic, qos=0)
    pub.publish(pub_topic, b"x", qos=0)
    got = sub.try_recv_publish(timeout=0.5)
    if should_match:
        assert got is not None, f"expected match {filter_topic} <-> {pub_topic}"
        assert got[0] == pub_topic
        assert got[1] == b"x"
    else:
        assert got is None, f"expected NO match {filter_topic} <-> {pub_topic}"
        # 哨兵：确认链路正常，排除"没收到是因为坏了"
        sentinel = "sentinel/always"
        sub.subscribe(sentinel, qos=0)
        pub.publish(sentinel, b"s", qos=0)
        got2 = sub.try_recv_publish(timeout=0.5)
        assert got2 is not None and got2[0] == sentinel, (
            "link broken, NO-match result unreliable"
        )


@pytest.mark.parametrize(
    "filter_topic, pub_topic, should_match",
    [
        # + 匹配一层
        ("sport/+/score", "sport/tennis/score", True),
        ("sport/+/score", "sport/football/score", True),
        ("sport/+/score", "sport/tennis/player", False),
        ("sport/+/score", "sport/tennis/a/score", False),
        # + 匹配空层
        ("sport/+/score", "sport//score", True),
        # # 匹配剩余所有层
        ("sport/#", "sport/tennis/score", True),
        ("sport/#", "sport", True),
        ("sport/#", "sport/", True),
        ("sport/#", "sportx", False),
        # 完整路径
        ("a/b/c", "a/b/c", True),
        ("a/b/c", "a/b/c/d", False),
        ("a/b/c", "a/b", False),
        # 混合
        ("a/+/c/#", "a/b/c/d/e", True),
        ("a/+/c/#", "a/b/x/d", False),
    ],
)
def test_wildcard(pair, filter_topic, pub_topic, should_match):
    sub, pub = pair
    _expect_match(sub, pub, filter_topic, pub_topic, should_match)
