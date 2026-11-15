#!/usr/bin/env python3
"""当前 Broker 与 Mosquitto 的端到端性能对比。

只使用 Python 标准库，客户端直接收发 MQTT 3.1.1 报文。所有场景默认
关闭持久化，双方使用相同的 loopback、payload、QoS 和客户端代码。

用法：
    python3 bench/bench_compare.py
    python3 bench/bench_compare.py --quick
    python3 bench/bench_compare.py --broker ./mqtt-broker-o2 --repeat 5
"""

from __future__ import annotations

import argparse
import json
import selectors
import shutil
import socket
import statistics
import struct
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_BROKER = ROOT / "mqtt-broker"
PAYLOAD = b"x" * 64
TOPIC = "bench/one"
FANOUT_TOPIC = "bench/fanout"


# ---------- 基础工具 ----------

def free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
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


def encode_remaining_length(n: int) -> bytes:
    out = bytearray()
    while True:
        b = n % 128
        n //= 128
        if n:
            b |= 0x80
        out.append(b)
        if not n:
            return bytes(out)


def decode_remaining_length(buf: bytearray, pos: int):
    multiplier = 1
    value = 0
    i = pos
    while i < len(buf):
        b = buf[i]
        value += (b & 0x7F) * multiplier
        if not (b & 0x80):
            return value, i - pos + 1
        multiplier *= 128
        i += 1
        if i - pos >= 4:
            raise ValueError("remaining length too long")
    return None, 0


def build_connect(client_id: str) -> bytes:
    cid = client_id.encode()
    body = (
        b"\x00\x04MQTT"
        b"\x04"          # MQTT 3.1.1
        b"\x02"          # clean session
        b"\x00\x3c"      # keepalive 60
        + struct.pack(">H", len(cid))
        + cid
    )
    return b"\x10" + encode_remaining_length(len(body)) + body


def build_subscribe(packet_id: int, topic_filter: str, qos: int = 0) -> bytes:
    f = topic_filter.encode()
    body = struct.pack(">H", packet_id) + struct.pack(">H", len(f)) + f
    body += bytes([qos])
    return b"\x82" + encode_remaining_length(len(body)) + body


def build_publish(topic: str, payload: bytes, qos: int, packet_id: int = 0) -> bytes:
    t = topic.encode()
    body = struct.pack(">H", len(t)) + t
    if qos:
        body += struct.pack(">H", packet_id)
    body += payload
    return bytes([0x30 | (qos << 1)]) + encode_remaining_length(len(body)) + body


class Reader:
    """带缓冲的 MQTT 报文读取器，热路径不做逐包切片。"""

    def __init__(self, sock: socket.socket):
        self.sock = sock
        self.buf = bytearray()
        self.pos = 0

    def _compact(self) -> None:
        if self.pos > 1 << 20:
            del self.buf[: self.pos]
            self.pos = 0

    def _try_next(self, copy_body: bool):
        while True:
            avail = len(self.buf) - self.pos
            if avail >= 2:
                rl, used = decode_remaining_length(self.buf, self.pos + 1)
                if rl is not None:
                    total = 1 + used + rl
                    if avail >= total:
                        first = self.buf[self.pos]
                        body = None
                        if copy_body:
                            start = self.pos + 1 + used
                            body = bytes(self.buf[start : self.pos + total])
                        self.pos += total
                        self._compact()
                        return first, body
            chunk = self.sock.recv(1 << 16)
            if not chunk:
                raise ConnectionError("broker closed connection")
            self.buf += chunk

    def read_until(self, packet_type: int):
        while True:
            first, body = self._try_next(True)
            if (first >> 4) == packet_type:
                return first, body

    def read_publishes(self, count: int) -> None:
        got = 0
        while got < count:
            first, _ = self._try_next(False)
            if (first >> 4) == 3:
                got += 1

    def read_pubacks(self, count: int) -> None:
        got = 0
        while got < count:
            first, _ = self._try_next(False)
            if (first >> 4) == 4:
                got += 1

    def feed(self) -> None:
        chunk = self.sock.recv(1 << 16)
        if not chunk:
            raise ConnectionError("broker closed connection")
        self.buf += chunk

    def parse_available_publishes(self) -> int:
        got = 0
        while True:
            avail = len(self.buf) - self.pos
            if avail < 2:
                return got
            rl, used = decode_remaining_length(self.buf, self.pos + 1)
            if rl is None:
                return got
            total = 1 + used + rl
            if avail < total:
                return got
            first = self.buf[self.pos]
            if (first >> 4) == 3:
                got += 1
            self.pos += total
            self._compact()


class Client:
    def __init__(self, port: int, client_id: str, timeout: float = 20.0):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=timeout)
        self.sock.settimeout(timeout)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.reader = Reader(self.sock)
        self.client_id = client_id

    def connect(self) -> None:
        self.sock.sendall(build_connect(self.client_id))
        _, body = self.reader.read_until(2)
        if len(body) != 2 or body[1] != 0:
            raise RuntimeError(f"CONNACK failed: {body!r}")

    def subscribe(self, topic_filter: str, qos: int = 0) -> None:
        self.sock.sendall(build_subscribe(1, topic_filter, qos))
        _, body = self.reader.read_until(9)
        if len(body) < 3 or body[2] == 0x80:
            raise RuntimeError(f"SUBACK failed: {body!r}")
        if body[2] != qos:
            raise RuntimeError(f"SUBACK qos {body[2]} != {qos}")

    def close(self) -> None:
        try:
            self.sock.close()
        except OSError:
            pass

    def disconnect(self) -> None:
        try:
            self.sock.sendall(b"\xe0\x00")
        except OSError:
            pass
        self.close()


class BrokerProcess:
    def __init__(self, kind: str, broker: Path, mosquitto: str):
        self.kind = kind
        self.broker = broker
        self.mosquitto = mosquitto
        self.port = free_port()
        self.proc: subprocess.Popen | None = None
        self.tmp: tempfile.TemporaryDirectory | None = None

    def __enter__(self):
        self.tmp = tempfile.TemporaryDirectory(prefix=f"bench-{self.kind}-")
        if self.kind == "ours":
            args = [
                str(self.broker),
                "--port", str(self.port),
                "--state-file", "",
            ]
        else:
            conf = Path(self.tmp.name) / "mosquitto.conf"
            conf.write_text(
                f"listener {self.port} 127.0.0.1\n"
                "allow_anonymous true\n"
                "persistence false\n"
                "log_dest none\n"
                "max_inflight_messages 1000\n"
                "max_queued_messages 100000\n"
                "set_tcp_nodelay false\n"
            )
            args = [self.mosquitto, "-c", str(conf)]
        self.proc = subprocess.Popen(
            args,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            wait_port(self.port)
        except Exception:
            self.stop()
            raise
        return self

    def stop(self) -> None:
        if self.proc is not None and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
        self.proc = None
        if self.tmp is not None:
            self.tmp.cleanup()
            self.tmp = None

    def __exit__(self, *exc):
        self.stop()


# ---------- 场景 ----------

def prepare_1to1(port: int):
    sub = Client(port, "bench-sub")
    sub.connect()
    sub.subscribe("bench/#", 0)
    pub = Client(port, "bench-pub")
    pub.connect()
    return sub, pub


def warmup_1to1(sub: Client, pub: Client, packets: bytes, count: int) -> None:
    pub.sock.sendall(packets * count)
    sub.reader.read_publishes(count)


def scenario_qos0_1to1(port: int, n: int, batch: int) -> float:
    sub, pub = prepare_1to1(port)
    try:
        pkt = build_publish(TOPIC, PAYLOAD, 0)
        warmup_1to1(sub, pub, pkt, 200)
        start = time.perf_counter()
        for _ in range(n // batch):
            pub.sock.sendall(pkt * batch)
            sub.reader.read_publishes(batch)
        elapsed = time.perf_counter() - start
        return n / elapsed
    finally:
        sub.disconnect()
        pub.disconnect()


def scenario_qos1_1to1(port: int, n: int, batch: int) -> float:
    sub, pub = prepare_1to1(port)
    try:
        pid = 1
        # warmup
        for _ in range(200):
            pub.sock.sendall(build_publish(TOPIC, PAYLOAD, 1, pid))
            pid = (pid % 65535) + 1
            sub.reader.read_publishes(1)
            pub.reader.read_pubacks(1)

        start = time.perf_counter()
        for _ in range(n // batch):
            packets = []
            for _ in range(batch):
                packets.append(build_publish(TOPIC, PAYLOAD, 1, pid))
                pid = (pid % 65535) + 1
            pub.sock.sendall(b"".join(packets))
            sub.reader.read_publishes(batch)
            pub.reader.read_pubacks(batch)
        elapsed = time.perf_counter() - start
        return n / elapsed
    finally:
        sub.disconnect()
        pub.disconnect()


def scenario_qos1_latency(port: int, n: int) -> tuple[float, float]:
    pub = Client(port, "bench-lat")
    pub.connect()
    try:
        for _ in range(100):
            pub.sock.sendall(build_publish(TOPIC, PAYLOAD, 1, 1))
            pub.reader.read_pubacks(1)

        samples = []
        for i in range(n):
            t0 = time.perf_counter()
            pub.sock.sendall(build_publish(TOPIC, PAYLOAD, 1, (i % 65535) + 1))
            pub.reader.read_pubacks(1)
            samples.append((time.perf_counter() - t0) * 1000.0)
        samples.sort()
        return samples[len(samples) // 2], samples[int(len(samples) * 0.99)]
    finally:
        pub.disconnect()


def scenario_fanout(port: int, subscribers: int, messages: int, batch: int) -> float:
    subs = []
    try:
        for i in range(subscribers):
            c = Client(port, f"bench-sub-{i}")
            c.connect()
            c.subscribe(FANOUT_TOPIC, 0)
            subs.append(c)
        pub = Client(port, "bench-pub")
        pub.connect()

        pkt = build_publish(FANOUT_TOPIC, PAYLOAD, 0)
        counts = [0] * subscribers
        sel = selectors.DefaultSelector()
        for i, c in enumerate(subs):
            sel.register(c.sock, selectors.EVENT_READ, i)

        start = time.perf_counter()
        sent = 0
        for _ in range(messages // batch):
            pub.sock.sendall(pkt * batch)
            target = sent + batch
            while any(c < target for c in counts):
                events = sel.select(timeout=20)
                if not events:
                    raise TimeoutError("fanout subscribers did not receive messages")
                for key, _ in events:
                    idx = key.data
                    try:
                        subs[idx].reader.feed()
                    except ConnectionError:
                        raise RuntimeError(f"fanout subscriber {idx} closed")
                    counts[idx] += subs[idx].reader.parse_available_publishes()
            sent = target
        elapsed = time.perf_counter() - start
        return (messages * subscribers) / elapsed
    finally:
        for c in subs:
            c.disconnect()


def scenario_connect(port: int, n: int) -> float:
    start = time.perf_counter()
    for i in range(n):
        c = Client(port, f"bench-conn-{i}")
        c.connect()
        c.disconnect()
    elapsed = time.perf_counter() - start
    return n / elapsed


# ---------- 运行 ----------

SCENARIOS = ["qos0_1to1", "qos1_1to1", "fanout", "qos1_latency", "connect"]


def run_once(kind: str, broker: Path, mosquitto: str, name: str, args) -> dict:
    with BrokerProcess(kind, broker, mosquitto) as b:
        if name == "qos0_1to1":
            return {"throughput": scenario_qos0_1to1(b.port, args.qos0_n, args.batch)}
        if name == "qos1_1to1":
            return {"throughput": scenario_qos1_1to1(b.port, args.qos1_n, args.batch)}
        if name == "fanout":
            return {
                "throughput": scenario_fanout(
                    b.port, args.fanout_subscribers, args.fanout_messages, args.fanout_batch
                )
            }
        if name == "qos1_latency":
            p50, p99 = scenario_qos1_latency(b.port, args.latency_n)
            return {"p50_ms": p50, "p99_ms": p99}
        if name == "connect":
            return {"throughput": scenario_connect(b.port, args.connections)}
        raise ValueError(name)


def median(values):
    return statistics.median(values)


def run_repeats(kind: str, broker: Path, mosquitto: str, name: str, args) -> dict:
    runs = [run_once(kind, broker, mosquitto, name, args) for _ in range(args.repeat)]
    if name == "qos1_latency":
        return {
            "p50_ms": median([r["p50_ms"] for r in runs]),
            "p99_ms": median([r["p99_ms"] for r in runs]),
        }
    return {"throughput": median([r["throughput"] for r in runs])}


def fmt_rate(v: float) -> str:
    if v >= 1_000_000:
        return f"{v / 1_000_000:.2f}M"
    if v >= 1_000:
        return f"{v / 1_000:.1f}k"
    return f"{v:.0f}"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--broker", type=Path, default=DEFAULT_BROKER)
    parser.add_argument("--mosquitto", default=shutil.which("mosquitto"))
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--qos0-n", type=int, default=50_000)
    parser.add_argument("--qos1-n", type=int, default=30_000)
    parser.add_argument("--batch", type=int, default=2_000)
    parser.add_argument("--fanout-subscribers", type=int, default=100)
    parser.add_argument("--fanout-messages", type=int, default=1_000)
    parser.add_argument("--fanout-batch", type=int, default=100)
    parser.add_argument("--latency-n", type=int, default=2_000)
    parser.add_argument("--connections", type=int, default=500)
    parser.add_argument("--quick", action="store_true")
    parser.add_argument(
        "--scenarios",
        default=",".join(SCENARIOS),
        help="comma-separated subset of: " + ",".join(SCENARIOS),
    )
    parser.add_argument("--json", type=Path)
    args = parser.parse_args()

    if not args.mosquitto:
        parser.error("mosquitto not found; pass --mosquitto PATH")
    args.broker = args.broker.resolve()
    if not args.broker.exists():
        parser.error(f"broker binary not found: {args.broker} (run make first)")

    scenarios = [x for x in args.scenarios.split(",") if x]
    unknown = [x for x in scenarios if x not in SCENARIOS]
    if unknown:
        parser.error(f"unknown scenarios: {','.join(unknown)}")

    if args.quick:
        args.repeat = 1
        args.qos0_n = 5_000
        args.qos1_n = 3_000
        args.batch = 500
        args.fanout_subscribers = 20
        args.fanout_messages = 200
        args.fanout_batch = 20
        args.latency_n = 300
        args.connections = 100

    for name, value in [
        ("qos0-n", args.qos0_n),
        ("qos1-n", args.qos1_n),
        ("batch", args.batch),
        ("fanout-subscribers", args.fanout_subscribers),
        ("fanout-messages", args.fanout_messages),
        ("fanout-batch", args.fanout_batch),
        ("latency-n", args.latency_n),
        ("connections", args.connections),
    ]:
        if value <= 0:
            parser.error(f"--{name} must be positive")
    if args.qos0_n % args.batch or args.qos1_n % args.batch:
        parser.error("qos0-n and qos1-n must be divisible by --batch")
    if args.fanout_messages % args.fanout_batch:
        parser.error("fanout-messages must be divisible by --fanout-batch")

    print(
        f"scenarios={','.join(scenarios)} repeat={args.repeat} "
        f"payload={len(PAYLOAD)}B broker={args.broker}"
    )
    print(
        f"qos0_n={args.qos0_n} qos1_n={args.qos1_n} batch={args.batch} "
        f"fanout={args.fanout_subscribers}x{args.fanout_messages} "
        f"latency_n={args.latency_n} connections={args.connections}"
    )
    print()

    results = {"ours": {}, "mosquitto": {}}
    for kind in ("ours", "mosquitto"):
        for name in scenarios:
            try:
                results[kind][name] = run_repeats(kind, args.broker, args.mosquitto, name, args)
            except Exception as exc:
                print(f"{kind:9s} {name:12s} FAILED: {exc}", file=sys.stderr)
                return 1

    headers = {
        "qos0_1to1": "QoS0 1->1 msg/s",
        "qos1_1to1": "QoS1 1->1 msg/s",
        "fanout": "fanout delivery/s",
        "qos1_latency": "QoS1 RTT p50/p99 ms",
        "connect": "connect/s",
    }
    print(f"{'scenario':24s} {'ours':>14s} {'mosquitto':>14s} {'ours/mosq':>10s}")
    print("-" * 68)
    for name in scenarios:
        ours = results["ours"][name]
        mosq = results["mosquitto"][name]
        if name == "qos1_latency":
            o = f"{ours['p50_ms']:.3f}/{ours['p99_ms']:.3f}"
            m = f"{mosq['p50_ms']:.3f}/{mosq['p99_ms']:.3f}"
            ratio = f"{(ours['p50_ms'] / mosq['p50_ms']):.2f}x"
        else:
            o = fmt_rate(ours["throughput"])
            m = fmt_rate(mosq["throughput"])
            ratio = f"{(ours['throughput'] / mosq['throughput']):.2f}x"
        print(f"{headers[name]:24s} {o:>14s} {m:>14s} {ratio:>10s}")

    if args.json:
        args.json.write_text(json.dumps(results, indent=2, ensure_ascii=False))
        print(f"\njson written to {args.json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
