"""状态持久化：retained、会话、离线队列、QoS2 inflight 跨 Broker 重启恢复。"""

import signal
import socket
import struct
import subprocess
import time
from pathlib import Path

from mqtt_client import MQTTClient

BROKER_BIN = Path(__file__).resolve().parent.parent / "mqtt-broker"


def _free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _wait_port(host: str, port: int, timeout: float = 3.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            s = socket.create_connection((host, port), timeout=0.2)
            s.close()
            return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError(f"broker did not start on {host}:{port}")


class RestartableBroker:
    """同一个端口 + 同一个 state 文件，可反复 stop/start。"""

    def __init__(self, state_path: Path, args=()):
        self.state_path = state_path
        self.args = args
        self.port = _free_port()
        self.proc = None
        self.log = None

    def start(self):
        assert self.proc is None
        self.log = open(self.state_path.with_suffix(".log"), "a")
        self.proc = subprocess.Popen(
            [
                str(BROKER_BIN),
                "--port",
                str(self.port),
                "--state-file",
                str(self.state_path),
                *self.args,
            ],
            stdout=self.log,
            stderr=subprocess.STDOUT,
            text=True,
        )
        _wait_port("127.0.0.1", self.port)
        return self.port

    def stop(self):
        if self.proc is not None and self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
            try:
                self.proc.wait(timeout=2)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
        self.proc = None
        if self.log is not None:
            self.log.close()
            self.log = None

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.stop()


def _u32(v: int) -> bytes:
    return struct.pack(">I", v)


def _blob(v: bytes) -> bytes:
    return _u32(len(v)) + v


def _write_v1_state(path: Path) -> None:
    """手写一个版本 1 状态文件，验证旧格式仍可加载。"""
    data = b"MQTTSN1\n" + _u32(1) + _u32(0) + _u32(1)
    data += _blob(b"legacy") + struct.pack(">HH", 60, 1)
    data += _u32(1) + _blob(b"legacy/#") + b"\x00"
    data += _u32(0) + _u32(0) + _u32(0)
    path.write_bytes(data)


def _complete_inbound(pub, packet_id):
    assert pub.expect_pubrec(packet_id, timeout=2) == packet_id
    pub.send_pubrel(packet_id)
    assert pub.expect_pubcomp(packet_id, timeout=2) == packet_id


def _complete_outbound(sub, packet_id):
    sub.send_pubrec(packet_id)
    assert sub.expect_pubrel(packet_id, timeout=2) == packet_id
    sub.send_pubcomp(packet_id)


def test_retained_survives_restart(tmp_path):
    broker = RestartableBroker(tmp_path / "state.bin")
    broker.start()
    try:
        pub = MQTTClient(port=broker.port, client_id="pub")
        pub.connect()
        pid = pub.publish("ret/x", b"retained", qos=1, retain=True)
        pub.expect_puback(pid, timeout=2)
        pub.ping()  # 确保 broker 已处理完并落盘
        pub.close()
    finally:
        broker.stop()

    broker.start()
    try:
        sub = MQTTClient(port=broker.port, client_id="sub")
        sub.connect()
        assert sub.subscribe("ret/#", qos=1) == (1,)
        topic, payload, qos, retain, packet_id, dup = sub.recv_publish_ex(
            timeout=2
        )
        assert (topic, payload, qos, retain, dup) == (
            "ret/x",
            b"retained",
            1,
            1,
            0,
        )
        sub.send_puback(packet_id)
        sub.disconnect()
    finally:
        broker.stop()


def test_state_file_written_while_broker_runs(tmp_path):
    """状态修改由后台 writer 落盘，不需要等到进程退出。"""
    state = tmp_path / "state.bin"
    broker = RestartableBroker(state)
    broker.start()
    try:
        pub = MQTTClient(port=broker.port, client_id="pub")
        pub.connect()
        pid = pub.publish("async/x", b"written", qos=1, retain=True)
        pub.expect_puback(pid, timeout=2)
        pub.ping()  # 确保 Broker 已处理完
        pub.close()

        deadline = time.time() + 3
        data = b""
        while time.time() < deadline:
            if state.exists():
                data = state.read_bytes()
                if data.startswith(b"MQTTSN1\n") and b"async/x" in data:
                    break
            time.sleep(0.02)

        assert data.startswith(b"MQTTSN1\n")
        assert b"async/x" in data
    finally:
        broker.stop()


def test_persistent_session_subscription_survives_restart(tmp_path):
    broker = RestartableBroker(tmp_path / "state.bin")
    broker.start()
    try:
        sub = MQTTClient(
            port=broker.port, client_id="persist", clean_session=False
        )
        sub.connect()
        assert sub.subscribe("sess/#", qos=1) == (1,)
        sub.ping()
        sub.disconnect()
    finally:
        broker.stop()

    broker.start()
    try:
        sub = MQTTClient(
            port=broker.port, client_id="persist", clean_session=False
        )
        present, _ = sub.connect()
        assert present == 1, "persistent session should survive restart"

        pub = MQTTClient(port=broker.port, client_id="pub")
        pub.connect()
        pid = pub.publish("sess/x", b"after-restart", qos=1)
        pub.expect_puback(pid, timeout=2)

        topic, payload, qos, _, packet_id = sub.recv_publish(timeout=2)
        assert (topic, payload, qos) == ("sess/x", b"after-restart", 1)
        sub.send_puback(packet_id)
        pub.close()
        sub.disconnect()
    finally:
        broker.stop()


def test_multiple_subscriptions_survive_restart(tmp_path):
    """同一持久会话的多个 filter 必须全部按会话归属恢复。"""
    broker = RestartableBroker(tmp_path / "state.bin")
    broker.start()
    try:
        sub = MQTTClient(
            port=broker.port, client_id="multi", clean_session=False
        )
        sub.connect()
        assert sub.subscribe("multi/one/#", qos=0) == (0,)
        assert sub.subscribe("multi/two/+", qos=0) == (0,)
        sub.ping()
        sub.disconnect()
    finally:
        broker.stop()

    broker.start()
    try:
        sub = MQTTClient(
            port=broker.port, client_id="multi", clean_session=False
        )
        present, _ = sub.connect()
        assert present == 1, "persistent session should survive restart"

        pub = MQTTClient(port=broker.port, client_id="pub")
        pub.connect()
        try:
            pub.publish("multi/one/a", b"one", qos=0)
            pub.publish("multi/two/b", b"two", qos=0)

            got = {}
            for _ in range(2):
                topic, payload, *_ = sub.recv_publish(timeout=2)
                got[topic] = payload
            assert got == {
                "multi/one/a": b"one",
                "multi/two/b": b"two",
            }
        finally:
            pub.close()
        sub.disconnect()
    finally:
        broker.stop()


def test_v1_state_file_still_loads(tmp_path):
    """状态文件升级到 v2 后，v1 旧文件仍应能加载。"""
    state = tmp_path / "v1.bin"
    _write_v1_state(state)
    broker = RestartableBroker(state)
    broker.start()
    try:
        sub = MQTTClient(
            port=broker.port, client_id="legacy", clean_session=False
        )
        present, code = sub.connect()
        assert code == 0
        assert present == 1

        pub = MQTTClient(port=broker.port, client_id="pub")
        pub.connect()
        try:
            pub.publish("legacy/x", b"old", qos=0)
            topic, payload, *_ = sub.recv_publish(timeout=2)
            assert (topic, payload) == ("legacy/x", b"old")
        finally:
            pub.close()
        sub.disconnect()
    finally:
        broker.stop()


def test_expired_session_not_restored_after_restart(tmp_path):
    """过期时间要落盘：Broker 重启后，离线超时的会话不能再恢复。"""
    broker = RestartableBroker(
        tmp_path / "state.bin", ("--session-expiry-ms", "300")
    )
    broker.start()
    try:
        sub = MQTTClient(
            port=broker.port, client_id="expired", clean_session=False
        )
        sub.connect()
        assert sub.subscribe("expired/#", qos=0) == (0,)
        sub.ping()
        sub.disconnect()
    finally:
        broker.stop()

    # 离线时间已经超过 TTL
    time.sleep(1.5)
    broker.start()
    try:
        # 等待启动后的过期扫描
        time.sleep(1.5)
        sub = MQTTClient(
            port=broker.port, client_id="expired", clean_session=False
        )
        present, code = sub.connect()
        assert code == 0
        assert present == 0, "expired session must not survive restart"
        sub.disconnect()
    finally:
        broker.stop()


def test_offline_queue_survives_restart(tmp_path):
    broker = RestartableBroker(tmp_path / "state.bin")
    broker.start()
    try:
        sub = MQTTClient(
            port=broker.port, client_id="queue", clean_session=False
        )
        sub.connect()
        assert sub.subscribe("queue/#", qos=1) == (1,)
        sub.ping()
        sub.disconnect()

        pub = MQTTClient(port=broker.port, client_id="pub")
        pub.connect()
        pid = pub.publish("queue/x", b"offline", qos=1)
        pub.expect_puback(pid, timeout=2)
        pub.ping()
        pub.close()
    finally:
        broker.stop()

    broker.start()
    try:
        sub = MQTTClient(
            port=broker.port, client_id="queue", clean_session=False
        )
        present, _ = sub.connect()
        assert present == 1

        topic, payload, qos, _, packet_id = sub.recv_publish(timeout=2)
        assert (topic, payload, qos) == ("queue/x", b"offline", 1)
        sub.send_puback(packet_id)
        sub.disconnect()
    finally:
        broker.stop()


def test_outbound_qos2_publish_state_survives_restart(tmp_path):
    broker = RestartableBroker(tmp_path / "state.bin")
    broker.start()
    try:
        sub = MQTTClient(
            port=broker.port, client_id="q2sub", clean_session=False
        )
        sub.connect()
        assert sub.subscribe("q2/#", qos=2) == (2,)

        pub = MQTTClient(port=broker.port, client_id="pub")
        pub.connect()
        pid = pub.publish("q2/x", b"unacked", qos=2)
        _complete_inbound(pub, pid)

        first = sub.recv_publish_ex(timeout=2)
        assert first[2] == 2
        assert first[5] == 0
        old_pid = first[4]
        sub.ping()
        sub.close()
        pub.close()
    finally:
        broker.stop()

    broker.start()
    try:
        sub = MQTTClient(
            port=broker.port, client_id="q2sub", clean_session=False
        )
        present, _ = sub.connect()
        assert present == 1

        second = sub.recv_publish_ex(timeout=2)
        assert second[1] == b"unacked"
        assert second[2] == 2
        assert second[4] == old_pid
        assert second[5] == 1, "resumed QoS2 PUBLISH must set DUP"
        _complete_outbound(sub, old_pid)
        sub.disconnect()
    finally:
        broker.stop()


def test_outbound_qos2_pubrel_state_survives_restart(tmp_path):
    broker = RestartableBroker(tmp_path / "state.bin")
    broker.start()
    try:
        sub = MQTTClient(
            port=broker.port, client_id="q2sub", clean_session=False
        )
        sub.connect()
        assert sub.subscribe("q2/#", qos=2) == (2,)

        pub = MQTTClient(port=broker.port, client_id="pub")
        pub.connect()
        pid = pub.publish("q2/rel", b"wait-comp", qos=2)
        _complete_inbound(pub, pid)

        entry = sub.recv_publish_ex(timeout=2)
        sub.send_pubrec(entry[4])
        assert sub.expect_pubrel(entry[4], timeout=2) == entry[4]
        sub.ping()
        sub.close()
        pub.close()
    finally:
        broker.stop()

    broker.start()
    try:
        sub = MQTTClient(
            port=broker.port, client_id="q2sub", clean_session=False
        )
        present, _ = sub.connect()
        assert present == 1

        # 已进入 PUBREL 阶段，重连后应收到 PUBREL 而不是 PUBLISH
        assert sub.expect_pubrel(entry[4], timeout=2) == entry[4]
        assert sub.try_recv_publish(timeout=0.5) is None
        sub.send_pubcomp(entry[4])
        time.sleep(0.3)
        assert sub.try_recv_publish(timeout=0.5) is None
        sub.disconnect()
    finally:
        broker.stop()


def test_incoming_qos2_state_survives_restart(tmp_path):
    broker = RestartableBroker(tmp_path / "state.bin")
    broker.start()
    try:
        pub = MQTTClient(
            port=broker.port, client_id="publisher", clean_session=False
        )
        pub.connect()
        pid = pub.publish("in/q2", b"after-pubrel", qos=2, retain=True)
        assert pub.expect_pubrec(pid, timeout=2) == pid
        pub.ping()
        pub.close()
    finally:
        broker.stop()

    broker.start()
    try:
        pub = MQTTClient(
            port=broker.port, client_id="publisher", clean_session=False
        )
        present, _ = pub.connect()
        assert present == 1

        # incoming QoS2 状态恢复后，broker 会补发 PUBREC
        assert pub.expect_pubrec(pid, timeout=2) == pid
        pub.send_pubrel(pid)
        assert pub.expect_pubcomp(pid, timeout=2) == pid
        pub.ping()
        pub.close()

        # PUBREL 之后才真正投递并写入 retained
        sub = MQTTClient(port=broker.port, client_id="sub")
        sub.connect()
        assert sub.subscribe("in/#", qos=2) == (2,)
        topic, payload, qos, retain, packet_id, dup = sub.recv_publish_ex(
            timeout=2
        )
        assert (topic, payload, qos, retain, dup) == (
            "in/q2",
            b"after-pubrel",
            2,
            1,
            0,
        )
        _complete_outbound(sub, packet_id)
        sub.disconnect()
    finally:
        broker.stop()


def test_clean_session_is_not_persisted(tmp_path):
    broker = RestartableBroker(tmp_path / "state.bin")
    broker.start()
    try:
        sub = MQTTClient(
            port=broker.port, client_id="ephemeral", clean_session=True
        )
        sub.connect()
        assert sub.subscribe("gone/#", qos=1) == (1,)
        sub.ping()
        sub.disconnect()
    finally:
        broker.stop()

    broker.start()
    try:
        sub = MQTTClient(
            port=broker.port, client_id="ephemeral", clean_session=False
        )
        present, _ = sub.connect()
        assert present == 0, "clean session must not be persisted"
        sub.disconnect()
    finally:
        broker.stop()


def test_qos2_offline_queue_survives_restart(tmp_path):
    broker = RestartableBroker(tmp_path / "state.bin")
    broker.start()
    try:
        sub = MQTTClient(
            port=broker.port, client_id="q2queue", clean_session=False
        )
        sub.connect()
        assert sub.subscribe("q2q/#", qos=2) == (2,)
        sub.ping()
        sub.disconnect()

        pub = MQTTClient(port=broker.port, client_id="pub")
        pub.connect()
        pid = pub.publish("q2q/x", b"offline-q2", qos=2)
        _complete_inbound(pub, pid)
        pub.ping()
        pub.close()
    finally:
        broker.stop()

    broker.start()
    try:
        sub = MQTTClient(
            port=broker.port, client_id="q2queue", clean_session=False
        )
        present, _ = sub.connect()
        assert present == 1

        topic, payload, qos, _, packet_id, dup = sub.recv_publish_ex(
            timeout=2
        )
        assert (topic, payload, qos, dup) == ("q2q/x", b"offline-q2", 2, 0)
        _complete_outbound(sub, packet_id)
        sub.disconnect()
    finally:
        broker.stop()


def test_corrupt_state_file_is_ignored(tmp_path):
    state_path = tmp_path / "state.bin"
    state_path.write_bytes(b"not a valid mqtt broker state file")

    broker = RestartableBroker(state_path)
    broker.start()
    try:
        sub = MQTTClient(port=broker.port, client_id="sub")
        sub.connect()
        assert sub.subscribe("ok/#", qos=1) == (1,)
        sub.ping()
        sub.disconnect()
    finally:
        broker.stop()
