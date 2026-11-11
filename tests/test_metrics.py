"""运行指标：定期统计与 SIGUSR1 触发统计。"""

import signal
import time
from pathlib import Path

from mqtt_client import MQTTClient


def _wait_log_contains(path: Path, needle: str, timeout: float = 3.0) -> str:
    deadline = time.time() + timeout
    text = ""
    while time.time() < deadline:
        text = path.read_text(errors="replace")
        if needle in text:
            return text
        time.sleep(0.05)
    raise AssertionError(f"log does not contain {needle!r}:\n{text[-1000:]}")


def test_periodic_stats_include_message_counters(broker_factory):
    port, proc = broker_factory("--stats-interval-ms", "100")

    c = MQTTClient(port=port, client_id="metrics")
    try:
        c.connect()
        assert c.subscribe("m", qos=0) == (0,)
        c.publish("m", b"x", qos=0)
        c.ping()

        text = _wait_log_contains(proc.log_path, "messages_received=1")
        assert "messages_sent=1" in text
        assert "stats:" in text
    finally:
        c.close()


def test_sigusr1_dumps_stats(broker_factory):
    port, proc = broker_factory()

    c = MQTTClient(port=port, client_id="stats")
    try:
        c.connect()
        proc.send_signal(signal.SIGUSR1)
        text = _wait_log_contains(proc.log_path, "stats:")
        assert "conn_active=1" in text
    finally:
        c.close()
