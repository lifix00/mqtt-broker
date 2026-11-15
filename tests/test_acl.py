"""Topic ACL：发布/订阅/Will 的授权。"""

import pytest

from mqtt_client import MQTTClient, MQTTProtocolError


def _write_acl(tmp_path, text):
    path = tmp_path / "acl.txt"
    path.write_text(text)
    return path


def _auth_client(port, client_id):
    return MQTTClient(
        port=port,
        client_id=client_id,
        username="alice",
        password="secret",
        timeout=1,
    )


def test_acl_allows_scoped_subscribe_and_publish(tmp_path, broker_factory):
    acl = _write_acl(
        tmp_path,
        """
user alice
topic read sensors/#
topic write sensors/#
""",
    )
    port, _ = broker_factory(
        "--username", "alice", "--password", "secret",
        "--acl-file", str(acl),
    )

    sub = _auth_client(port, "acl-sub")
    pub = _auth_client(port, "acl-pub")
    try:
        sub.connect()
        pub.connect()

        assert sub.subscribe("sensors/room1", qos=0) == (0,)
        assert sub.subscribe("sensors/+/temp", qos=0) == (0,)
        assert sub.subscribe("admin/#", qos=0) == (0x80,)
        assert sub.subscribe("#", qos=0) == (0x80,)

        pub.publish("sensors/room1/temp", b"21", qos=0)
        got = sub.try_recv_publish(timeout=1)
        # 两条订阅都可能命中，取第一条并确认主题/内容
        assert got is not None
        assert got[0] == "sensors/room1/temp"
        assert got[1] == b"21"

        # 被拒绝的订阅不能收到消息
        pub.publish("admin/x", b"no", qos=0)
        assert sub.try_recv_publish(timeout=0.3) is None
    finally:
        sub.close()
        pub.close()


def test_acl_denied_publish_disconnects(tmp_path, broker_factory):
    acl = _write_acl(
        tmp_path,
        """
user alice
topic read sensors/#
topic write sensors/#
""",
    )
    port, _ = broker_factory(
        "--username", "alice", "--password", "secret",
        "--acl-file", str(acl),
    )

    pub = _auth_client(port, "acl-denied-pub")
    try:
        pub.connect()
        try:
            pub.publish("admin/x", b"no", qos=0)
        except OSError:
            pass

        # 未授权 PUBLISH 会断开连接
        with pytest.raises((ConnectionError, TimeoutError, OSError)):
            pub.ping()
    finally:
        pub.close()


def test_acl_denies_will_topic(tmp_path, broker_factory):
    acl = _write_acl(
        tmp_path,
        """
user alice
topic write cmd/#
""",
    )
    port, _ = broker_factory(
        "--username", "alice", "--password", "secret",
        "--acl-file", str(acl),
    )

    c = MQTTClient(
        port=port,
        client_id="acl-will",
        username="alice",
        password="secret",
        timeout=1,
        will_topic="admin/will",
        will_payload=b"bye",
    )
    try:
        with pytest.raises(MQTTProtocolError, match="return code=5"):
            c.connect()
    finally:
        c.close()


def test_acl_wildcard_rule_does_not_cover_wider_filter(
    tmp_path, broker_factory
):
    acl = _write_acl(
        tmp_path,
        """
user alice
topic read sensors/+
topic write sensors/+
""",
    )
    port, _ = broker_factory(
        "--username", "alice", "--password", "secret",
        "--acl-file", str(acl),
    )

    c = _auth_client(port, "acl-wildcard")
    try:
        c.connect()
        assert c.subscribe("sensors/+", qos=0) == (0,)
        assert c.subscribe("sensors/room1", qos=0) == (0,)
        assert c.subscribe("sensors/#", qos=0) == (0x80,)
        assert c.subscribe("sensors/+/temp", qos=0) == (0x80,)
    finally:
        c.close()


def test_acl_anonymous_identity_cannot_be_spoofed(
    tmp_path, broker_factory
):
    acl = _write_acl(
        tmp_path,
        """
user alice
topic read secret/#

user anonymous
topic read public/#
""",
    )
    # 不启用认证：客户端即使自报 username=alice，也只能按 anonymous 授权
    port, _ = broker_factory("--acl-file", str(acl))

    c = MQTTClient(
        port=port,
        client_id="acl-anon",
        username="alice",
        timeout=1,
    )
    try:
        c.connect()
        assert c.subscribe("public/x", qos=0) == (0,)
        assert c.subscribe("secret/x", qos=0) == (0x80,)
    finally:
        c.close()
