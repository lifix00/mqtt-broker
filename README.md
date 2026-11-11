# MQTT Broker

C11 + epoll 实现的 MQTT 3.1.1 Broker，支持：

- QoS 0 / QoS 1 / QoS 2
- 主题通配符、持久会话、离线队列
- Retained、Will、Keepalive
- 会话 / Retained / QoS2 状态的磁盘持久化
- 动态读写缓冲、资源上限与 LRU 淘汰
- SIGTERM / SIGINT 优雅退出
- 可选用户名密码认证
- 运行指标（`SIGUSR1` 或周期日志）
- GitHub Actions CI

## 构建与运行

```bash
make
./mqtt-broker --port 1883 --state-file mqtt-broker.state
```

`--state-file` 可省略，默认是 `mqtt-broker.state`。状态文件保存 retained、`clean_session=0` 的会话、订阅、inflight、离线队列和入站 QoS2 状态，Broker 重启后自动恢复。

常用参数：

```text
--max-sessions N        持久会话上限，0 = 不限制（默认 1024）
--max-retained N        retained 上限，0 = 不限制（默认 4096）
--max-subscriptions N   每会话订阅上限，0 = 不限制（默认 256）
--max-read-buffer N     读缓冲上限（默认 1 MiB）
--max-write-buffer N    写缓冲上限（默认 2 MiB）
--qos-retry-ms N        QoS1/QoS2 重传间隔（默认 2000ms）
--stats-interval-ms N   周期打印统计，0 = 关闭（默认 0）
--username USER         启用用户名认证
--password PASS         启用密码认证（需同时提供 --username）
```

运行时可以用 `kill -USR1 <pid>` 立即打印一行统计：

```text
stats: conn_active=1 conn_accepted=2 conn_closed=1 ... messages_received=1 messages_sent=1
```

## 测试

```bash
make test
# 或
.venv/bin/python -m pytest tests/ -q
```

## CI

`.github/workflows/ci.yml` 在 push/PR 时执行：

1. `make`
2. 普通 pytest
3. ASan/UBSan 构建 + 全量 pytest

## 文档

- [项目进度与计划](docs/PROJECT_STATUS.md)
