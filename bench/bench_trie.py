#!/usr/bin/env python3
"""Topic trie 基准：大量订阅下，发布到单个主题的端到端吞吐。

用法（在仓库根目录）：
    python3 bench/bench_trie.py --subscriptions 20000 --messages 2000

脚本会启动一个关闭持久化的 Broker，建立 N 个互不相同的订阅，
然后连续发布 M 条只匹配其中一个订阅的消息，统计“发布 + 接收”耗时。
对比旧版线性扫描时，用同一个脚本分别指向两个 Broker 二进制即可。
"""

import argparse
import socket
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "tests"))

from mqtt_client import MQTTClient  # noqa: E402


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def wait_port(port: int, timeout: float = 5.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            s = socket.create_connection(("127.0.0.1", port), timeout=0.2)
            s.close()
            return
        except OSError:
            time.sleep(0.02)
    raise RuntimeError(f"broker did not start on port {port}")


def run(broker: Path, n_subs: int, n_msgs: int) -> tuple[float, float]:
    port = free_port()
    proc = subprocess.Popen(
        [
            str(broker),
            "--port",
            str(port),
            "--state-file", "",
            "--max-subscriptions", "0",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.STDOUT,
    )
    sub = pub = None
    try:
        wait_port(port)
        sub = MQTTClient(port=port, client_id="bench-sub", timeout=30)
        pub = MQTTClient(port=port, client_id="bench-pub", timeout=30)
        sub.connect()
        pub.connect()

        start = time.perf_counter()
        for i in range(n_subs):
            codes = sub.subscribe(f"bench/{i}/value", qos=0)
            assert codes == (0,), f"subscribe failed: {codes}"
        setup = time.perf_counter() - start

        target = n_subs // 2
        topic = f"bench/{target}/value"

        # 预热，避免把首次分配/日志初始化算进测量
        pub.publish(topic, b"w", qos=0)
        assert sub.recv_publish(timeout=5)[0] == topic

        start = time.perf_counter()
        for _ in range(n_msgs):
            pub.publish(topic, b"x", qos=0)
        for _ in range(n_msgs):
            got = sub.recv_publish(timeout=30)
            assert got[0] == topic, got[0]
        delivery = time.perf_counter() - start
        return setup, delivery
    finally:
        for c in (sub, pub):
            if c is not None:
                try:
                    c.disconnect()
                except OSError:
                    pass
        proc.terminate()
        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--broker", type=Path, default=ROOT / "mqtt-broker")
    parser.add_argument("--subscriptions", type=int, default=20000)
    parser.add_argument("--messages", type=int, default=2000)
    args = parser.parse_args()

    broker = args.broker.resolve()
    if not broker.exists():
        parser.error(f"broker binary not found: {broker} (run make first)")
    if args.subscriptions < 1 or args.messages < 1:
        parser.error("subscriptions/messages must be positive")

    setup, delivery = run(broker, args.subscriptions, args.messages)
    print(
        f"subscriptions={args.subscriptions} messages={args.messages} "
        f"setup={setup:.3f}s delivery={delivery:.3f}s "
        f"rate={args.messages / delivery:.0f} msg/s"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
