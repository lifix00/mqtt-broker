"""Inflight 的出口：ACK 超时和最大重传次数。"""

import signal
import time

from mqtt_client import MQTTClient


def _stats(proc) -> str:
    """触发一次 SIGUSR1，返回这次新生成的 stats 行。"""
    before = proc.log_path.read_text(errors="replace").count("stats:")
    proc.send_signal(signal.SIGUSR1)
    deadline = time.time() + 4.0
    text = ""
    while time.time() < deadline:
        text = proc.log_path.read_text(errors="replace")
        lines = [line for line in text.splitlines() if line.startswith("stats:")]
        if len(lines) > before:
            return lines[-1]
        time.sleep(0.05)
    raise AssertionError(f"no new stats line:\n{text[-1200:]}")


def _wait_stats(proc, predicate, timeout: float = 4.0) -> str:
    """反复触发 stats，直到某一行满足 predicate。"""
    deadline = time.time() + timeout
    last = ""
    while time.time() < deadline:
        last = _stats(proc)
        if predicate(last):
            return last
        time.sleep(0.05)
    raise AssertionError(f"no matching stats line, last={last!r}")


def test_inflight_timeout_closes_connection_and_clears_clean_session(
    broker_factory,
):
    """在线超时后关闭连接；clean session 随连接销毁，inflight 清空。"""
    port, proc = broker_factory(
        "--qos-retry-ms", "100",
        "--inflight-timeout-ms", "500",
    )
    sub = MQTTClient(port=port, client_id="timeout-sub")
    pub = MQTTClient(port=port, client_id="timeout-pub")
    try:
        sub.connect()
        pub.connect()
        assert sub.subscribe("timeout/#", qos=1) == (1,)

        pid = pub.publish("timeout/x", b"x", qos=1)
        pub.expect_puback(pid, timeout=2)

        topic, payload, qos, _, packet_id, dup = sub.recv_publish_ex(timeout=2)
        assert (topic, payload, qos, dup) == ("timeout/x", b"x", 1, 0)
        assert packet_id is not None
        # 故意不回 PUBACK

        # 先确认 inflight 确实被占用
        _wait_stats(proc, lambda line: "inflight=1" in line)

        # 超时后连接被关闭；clean session 随之销毁，inflight 清空
        text = _wait_stats(
            proc,
            lambda line: "inflight=0" in line and "conn_active=1" in line,
            timeout=4.0,
        )
        assert "messages_dropped=0" in text
    finally:
        sub.close()
        pub.close()


def test_max_inflight_retries_closes_connection_for_clean_session(
    broker_factory,
):
    """超过最大重传次数后关闭连接；clean session 不会被持久保留。"""
    port, proc = broker_factory(
        "--qos-retry-ms", "100",
        "--max-inflight-retries", "2",
        "--inflight-timeout-ms", "60000",
    )
    sub = MQTTClient(port=port, client_id="retry-sub")
    pub = MQTTClient(port=port, client_id="retry-pub")
    try:
        sub.connect()
        pub.connect()
        assert sub.subscribe("retry/#", qos=1) == (1,)

        pid = pub.publish("retry/x", b"x", qos=1)
        pub.expect_puback(pid, timeout=2)

        first = sub.recv_publish_ex(timeout=2)
        assert first[5] == 0
        # 两次重传都带 DUP=1
        for _ in range(2):
            again = sub.recv_publish_ex(timeout=2)
            assert again[0] == "retry/x"
            assert again[5] == 1

        # 达到上限后关闭连接；clean session 随连接销毁
        text = _wait_stats(
            proc,
            lambda line: "inflight=0" in line and "conn_active=1" in line,
        )
        assert "messages_dropped=0" in text
    finally:
        sub.close()
        pub.close()


def test_qos2_retry_limit_keeps_inflight_for_reconnect(broker_factory):
    """QoS2 达到重试上限只关闭连接，持久会话重连后继续 PUBREL。"""
    port, proc = broker_factory(
        "--qos-retry-ms", "100",
        "--max-inflight-retries", "1",
        "--inflight-timeout-ms", "60000",
    )
    sub = MQTTClient(
        port=port, client_id="q2-retry", clean_session=False, timeout=2
    )
    pub = MQTTClient(port=port, client_id="q2-pub", timeout=2)
    out_pid = None
    try:
        sub.connect()
        pub.connect()
        assert sub.subscribe("q2/#", qos=2) == (2,)

        pid = pub.publish("q2/x", b"x", qos=2)
        assert pub.expect_pubrec(pid, timeout=2) == pid
        pub.send_pubrel(pid)
        assert pub.expect_pubcomp(pid, timeout=2) == pid

        topic, payload, qos, _, out_pid, dup = sub.recv_publish_ex(timeout=2)
        assert (topic, payload, qos, dup) == ("q2/x", b"x", 2, 0)
        sub.send_pubrec(out_pid)

        # 初始 PUBREL + 一次重传 PUBREL；故意不回 PUBCOMP
        assert sub.expect_pubrel(out_pid, timeout=2) == out_pid
        assert sub.expect_pubrel(out_pid, timeout=2) == out_pid

        # 达到 max_inflight_retries 后 Broker 关闭连接，但保留 inflight
        sub.sock.settimeout(2)
        assert sub.sock.recv(1) == b""
    finally:
        sub.close()

    sub2 = MQTTClient(
        port=port, client_id="q2-retry", clean_session=False, timeout=2
    )
    try:
        present, code = sub2.connect()
        assert (present, code) == (1, 0)
        assert sub2.expect_pubrel(out_pid, timeout=2) == out_pid
        sub2.send_pubcomp(out_pid)

        text = _wait_stats(proc, lambda line: "inflight=0" in line)
        assert "messages_dropped=0" in text
    finally:
        sub2.close()
        pub.close()
