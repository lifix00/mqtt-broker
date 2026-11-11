"""优雅退出：SIGTERM 后发布 Will、flush 数据并退出。"""

import signal

from mqtt_client import MQTTClient


def test_sigterm_publishes_will_and_exits(broker_factory):
    port, proc = broker_factory()

    sub = MQTTClient(port=port, client_id="watch", timeout=3)
    sub.connect()
    assert sub.subscribe("will/#", qos=0) == (0,)

    will_client = MQTTClient(
        port=port,
        client_id="will-client",
        will_topic="will/x",
        will_payload=b"bye",
        will_qos=0,
    )
    will_client.connect()

    proc.send_signal(signal.SIGTERM)

    topic, payload, qos, retain, _ = sub.recv_publish(timeout=3)
    assert topic == "will/x"
    assert payload == b"bye"
    assert qos == 0
    assert retain == 0

    assert proc.wait(timeout=3) == 0, "broker should exit cleanly on SIGTERM"

    sub.close()
    will_client.close()


def test_sigterm_exits_with_no_clients(broker_factory):
    _, proc = broker_factory()
    proc.send_signal(signal.SIGTERM)
    assert proc.wait(timeout=3) == 0
