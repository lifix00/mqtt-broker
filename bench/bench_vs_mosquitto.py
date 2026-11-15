#!/usr/bin/env python3
"""用 mqtt-benchmark 对比当前 Broker 与 Mosquitto。

mqtt-benchmark 的每个客户端只发布、不订阅，因此这里测的是：
  - QoS0：发布路径吞吐（Publish 调用完成，不等 broker 确认）
  - QoS1/2：发布 + broker 确认的吞吐和平均延迟

fanout / 端到端投递不在这个工具的能力范围内，见 bench_compare.py。

用法：
    python3 bench/bench_vs_mosquitto.py --broker ./mqtt-broker-o2
    python3 bench/bench_vs_mosquitto.py --quick
"""

from __future__ import annotations

import argparse
import json
import shutil
import socket
import statistics
import subprocess
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_BROKER = ROOT / "mqtt-broker"
DEFAULT_TOOL = Path("/home/lifix/go/bin/mqtt-benchmark")


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


class BrokerProcess:
    def __init__(self, kind: str, broker: Path):
        self.kind = kind
        self.broker = broker
        self.port = free_port()
        self.proc: subprocess.Popen | None = None
        self.tmp: tempfile.TemporaryDirectory | None = None

    def __enter__(self):
        self.tmp = tempfile.TemporaryDirectory(prefix=f"bench-{self.kind}-")
        if self.kind == "ours":
            args = [
                str(self.broker),
                "--port",
                str(self.port),
                "--state-file",
                "",
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
            args = [shutil.which("mosquitto") or "mosquitto", "-c", str(conf)]

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


def run_mqtt_benchmark(
    tool: Path,
    port: int,
    qos: int,
    clients: int,
    count: int,
    size: int,
    prefix: str,
    timeout: float,
) -> dict:
    cmd = [
        str(tool),
        "-broker",
        f"tcp://127.0.0.1:{port}",
        "-topic",
        "bench/topic",
        "-qos",
        str(qos),
        "-clients",
        str(clients),
        "-count",
        str(count),
        "-message-interval",
        "0",
        "-size",
        str(size),
        "-client-prefix",
        prefix,
        "-quiet",
        "-format",
        "json",
    ]
    cp = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=timeout,
    )
    if cp.returncode != 0:
        raise RuntimeError(
            f"mqtt-benchmark failed rc={cp.returncode}: {cp.stderr.strip()[:500]}"
        )
    try:
        data = json.loads(cp.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"cannot parse mqtt-benchmark output: {exc}\n{cp.stdout[:1000]}"
        ) from exc

    totals = data["totals"]
    success = int(totals["successes"])
    run_time = float(totals["total_run_time"])
    return {
        "successes": success,
        "failures": int(totals["failures"]),
        # mqtt-benchmark 的 total_msgs_per_sec 是每个 client 速率之和，
        # 这里统一用 successes / wall-clock，便于跨 client 数比较。
        "throughput": success / run_time if run_time > 0 else 0.0,
        "sum_client_rate": float(totals["total_msgs_per_sec"]),
        "mean_ms": float(totals["msg_time_mean_avg"]),
        "max_ms": float(totals["msg_time_max"]),
        "min_ms": float(totals["msg_time_min"]),
        "runtime": run_time,
    }


def median_run(runs: list[dict]) -> dict:
    return {
        "throughput": statistics.median(r["throughput"] for r in runs),
        "mean_ms": statistics.median(r["mean_ms"] for r in runs),
        "max_ms": statistics.median(r["max_ms"] for r in runs),
        "min_ms": statistics.median(r["min_ms"] for r in runs),
        "successes": statistics.median(r["successes"] for r in runs),
        "failures": sum(r["failures"] for r in runs),
    }


def run_case(
    kind: str,
    broker: Path,
    tool: Path,
    qos: int,
    clients: int,
    count: int,
    size: int,
    repeat: int,
    timeout: float,
) -> dict:
    runs = []
    with BrokerProcess(kind, broker) as b:
        for rep in range(repeat):
            runs.append(
                run_mqtt_benchmark(
                    tool,
                    b.port,
                    qos,
                    clients,
                    count,
                    size,
                    f"bench-{kind}-q{qos}-c{clients}-{rep}",
                    timeout,
                )
            )
    return median_run(runs)


def fmt_rate(v: float) -> str:
    if v >= 1_000_000:
        return f"{v / 1_000_000:.2f}M"
    if v >= 1_000:
        return f"{v / 1_000:.1f}k"
    return f"{v:.0f}"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--broker", type=Path, default=DEFAULT_BROKER)
    parser.add_argument("--mqtt-benchmark", type=Path, default=DEFAULT_TOOL)
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--clients", default="1,10,50,100")
    parser.add_argument("--qos", default="0,1")
    parser.add_argument("--count", type=int, default=1000)
    parser.add_argument("--size", type=int, default=64)
    parser.add_argument("--timeout", type=float, default=180.0)
    parser.add_argument("--quick", action="store_true")
    parser.add_argument("--json", type=Path)
    args = parser.parse_args()

    args.broker = args.broker.resolve()
    args.mqtt_benchmark = args.mqtt_benchmark.resolve()
    if not args.broker.exists():
        parser.error(f"broker binary not found: {args.broker} (run make first)")
    if not args.mqtt_benchmark.exists():
        parser.error(f"mqtt-benchmark not found: {args.mqtt_benchmark}")

    clients = [int(x) for x in args.clients.split(",") if x]
    qos_list = [int(x) for x in args.qos.split(",") if x]
    if args.quick:
        clients = [1, 10]
        qos_list = [0, 1]
        args.count = 200
        args.repeat = 1
    for q in qos_list:
        if q not in (0, 1, 2):
            parser.error(f"invalid qos: {q}")
    for c in clients:
        if c < 1:
            parser.error(f"invalid clients: {c}")

    print(
        f"tool={args.mqtt_benchmark} broker={args.broker} "
        f"repeat={args.repeat} count/client={args.count} size={args.size}B"
    )
    print(
        "mqtt-benchmark: QoS0 不等确认；QoS1/2 等 broker 确认；"
        "throughput=successes/total_run_time"
    )
    print()

    results = {"ours": {}, "mosquitto": {}}
    for qos in qos_list:
        for n in clients:
            key = f"q{qos}-c{n}"
            for kind in ("ours", "mosquitto"):
                results[kind][key] = run_case(
                    kind,
                    args.broker,
                    args.mqtt_benchmark,
                    qos,
                    n,
                    args.count,
                    args.size,
                    args.repeat,
                    args.timeout,
                )

    print(
        f"{'scenario':16s} {'msgs':>7s} {'ours msg/s':>12s} "
        f"{'mosq msg/s':>12s} {'ratio':>7s} {'ours ms':>9s} {'mosq ms':>9s}"
    )
    print("-" * 80)
    for qos in qos_list:
        for n in clients:
            key = f"q{qos}-c{n}"
            o = results["ours"][key]
            m = results["mosquitto"][key]
            total = args.count * n
            ratio = o["throughput"] / m["throughput"] if m["throughput"] else 0.0
            print(
                f"qos{qos} c={n:<3d}     {total:>7d} "
                f"{fmt_rate(o['throughput']):>12s} "
                f"{fmt_rate(m['throughput']):>12s} "
                f"{ratio:>6.2f}x "
                f"{o['mean_ms']:>9.3f} {m['mean_ms']:>9.3f}"
            )
            if o["failures"] or m["failures"]:
                print(
                    f"  WARNING failures: ours={o['failures']} "
                    f"mosquitto={m['failures']}"
                )
    print()
    print("均值延迟来自 mqtt-benchmark 的 msg_time_mean_avg，包含 QoS1/2 的确认等待。")

    if args.json:
        args.json.write_text(json.dumps(results, indent=2, ensure_ascii=False))
        print(f"json written to {args.json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
