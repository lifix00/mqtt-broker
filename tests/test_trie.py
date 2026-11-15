"""Topic trie 的结构性回归测试：幂等、删除、剪枝、上限释放、多根节点。"""

from mqtt_client import MQTTClient


def test_duplicate_subscribe_is_idempotent(broker_port):
    """同一会话重复订阅同一 filter 只更新 QoS，不应产生两条订阅。"""
    sub = MQTTClient(port=broker_port, client_id="trie-dup")
    pub = MQTTClient(port=broker_port, client_id="pub")
    try:
        sub.connect()
        pub.connect()

        assert sub.subscribe("trie/dup", qos=0) == (0,)
        assert sub.subscribe("trie/dup", qos=0) == (0,)

        pub.publish("trie/dup", b"once", qos=0)
        topic, payload, *_ = sub.recv_publish(timeout=2)
        assert (topic, payload) == ("trie/dup", b"once")
        assert sub.try_recv_publish(timeout=0.3) is None
    finally:
        sub.disconnect()
        pub.disconnect()


def test_unsubscribe_keeps_other_subscriber(broker_port):
    """两个会话订阅同一节点，删除其中一个不能影响另一个。"""
    a = MQTTClient(port=broker_port, client_id="trie-a")
    b = MQTTClient(port=broker_port, client_id="trie-b")
    pub = MQTTClient(port=broker_port, client_id="pub")
    try:
        for c in (a, b, pub):
            c.connect()

        assert a.subscribe("shared/x", qos=0) == (0,)
        assert b.subscribe("shared/x", qos=0) == (0,)
        a.unsubscribe("shared/x")

        pub.publish("shared/x", b"still-there", qos=0)
        topic, payload, *_ = b.recv_publish(timeout=2)
        assert (topic, payload) == ("shared/x", b"still-there")
        assert a.try_recv_publish(timeout=0.3) is None

        # a 的链路仍然正常
        assert a.subscribe("shared/sentinel", qos=0) == (0,)
        pub.publish("shared/sentinel", b"ok", qos=0)
        topic, payload, *_ = a.recv_publish(timeout=2)
        assert (topic, payload) == ("shared/sentinel", b"ok")
    finally:
        for c in (a, b, pub):
            c.disconnect()


def test_unsubscribe_prunes_and_resubscribe(broker_port):
    """删除最后一个订阅后应回收空节点，之后还能重新创建同一路径。"""
    sub = MQTTClient(port=broker_port, client_id="trie-prune")
    pub = MQTTClient(port=broker_port, client_id="pub")
    try:
        sub.connect()
        pub.connect()

        assert sub.subscribe("root/branch/leaf", qos=0) == (0,)
        sub.unsubscribe("root/branch/leaf")
        pub.publish("root/branch/leaf", b"gone", qos=0)
        assert sub.try_recv_publish(timeout=0.3) is None

        # 路径已被剪枝，重新订阅必须能完整重建
        assert sub.subscribe("root/branch/leaf", qos=0) == (0,)
        pub.publish("root/branch/leaf", b"back", qos=0)
        topic, payload, *_ = sub.recv_publish(timeout=2)
        assert (topic, payload) == ("root/branch/leaf", b"back")
    finally:
        sub.disconnect()
        pub.disconnect()


def test_subscription_limit_released_after_unsubscribe(broker_factory):
    """每会话订阅计数必须随 UNSUBSCRIBE 下降。"""
    port, _ = broker_factory("--max-subscriptions", "1")
    sub = MQTTClient(port=port, client_id="trie-limit")
    pub = MQTTClient(port=port, client_id="pub")
    try:
        sub.connect()
        pub.connect()

        assert sub.subscribe("limit/a", qos=0) == (0,)
        assert sub.subscribe("limit/b", qos=0) == (0x80,)

        sub.unsubscribe("limit/a")
        assert sub.subscribe("limit/b", qos=0) == (0,)

        pub.publish("limit/b", b"ok", qos=0)
        topic, payload, *_ = sub.recv_publish(timeout=2)
        assert (topic, payload) == ("limit/b", b"ok")
    finally:
        sub.disconnect()
        pub.disconnect()


def test_many_root_topics_match_independently(broker_factory):
    """大量互不相同的一级 topic 共享同一个 root，匹配结果必须互不串扰。"""
    # 关掉持久化，避免每个 SUBSCRIBE 都 fsync 拖慢测试
    port, _ = broker_factory("--state-file", "")
    n = 200
    sub = MQTTClient(port=port, client_id="trie-many")
    pub = MQTTClient(port=port, client_id="pub")
    try:
        sub.connect()
        pub.connect()

        for i in range(n):
            assert sub.subscribe(f"root{i}/value", qos=0) == (0,)

        wanted = [0, 1, 57, 199]
        for i in wanted:
            pub.publish(f"root{i}/value", f"v{i}".encode(), qos=0)

        got = {}
        for _ in wanted:
            topic, payload, *_ = sub.recv_publish(timeout=2)
            got[topic] = payload

        assert got == {
            f"root{i}/value": f"v{i}".encode() for i in wanted
        }
        assert sub.try_recv_publish(timeout=0.3) is None
    finally:
        sub.disconnect()
        pub.disconnect()


def test_long_topic_matches_root_wildcard(broker_port):
    """转发路径已去掉 256 字节 topic 缓冲，长 topic 仍应能被 '#' 匹配。"""
    sub = MQTTClient(port=broker_port, client_id="trie-long")
    pub = MQTTClient(port=broker_port, client_id="pub")
    try:
        sub.connect()
        pub.connect()
        assert sub.subscribe("#", qos=0) == (0,)

        topic = "long/" + "x" * 300
        pub.publish(topic, b"long", qos=0)
        got_topic, payload, *_ = sub.recv_publish(timeout=2)
        assert (got_topic, payload) == (topic, b"long")
    finally:
        sub.disconnect()
        pub.disconnect()
