# MQTT Broker

**C11 · epoll · MQTT 3.1.1 · TLS · QoS 0/1/2**

从零实现的 MQTT 3.1.1 Broker：单线程非阻塞网络循环 + 后台持久化 writer，关注协议正确性、低开销 I/O 和可量化性能。

![C11](https://img.shields.io/badge/C-11-283593?logo=c)
![MQTT](https://img.shields.io/badge/MQTT-3.1.1-6a1b9a)
![TLS](https://img.shields.io/badge/TLS-1.2+-2e7d32?logo=openssl)
![Tests](https://img.shields.io/badge/tests-152%20passed-brightgreen)

## 亮点

- **协议完整**：QoS 0/1/2、Retained、Will、Keepalive、持久会话、离线队列、inflight 重传、会话过期。
- **低开销网络层**：单线程 `epoll` 非阻塞事件循环，动态读写缓冲，写缓冲背压，支持 TLS 1.2+。
- **订阅匹配**：topic trie + 全局边哈希 + 通配符缓存；20k 订阅/2k 消息从 1.81s 降至 0.012s，约 **150×**。
- **异步持久化**：dirty 合并 + 后台 writer + 原子 `rename`；QoS1 持久化吞吐从约 0.7 万提升至 5.3 万 msg/s，约 **7×**。
- **安全与质量**：用户名认证、topic ACL、152 个集成测试、ASan/UBSan、GitHub Actions CI。

## 快速开始

```bash
sudo apt-get install build-essential libssl-dev
make
./mqtt-broker --port 1883
```

默认状态文件为 `mqtt-broker.state`；传 `--state-file ""` 可关闭持久化。

```bash
# 持久化到指定文件
./mqtt-broker --port 1883 --state-file data/broker.state

# TLS：所有连接走 TLS，不回落到明文
./mqtt-broker --port 8883 --tls-cert cert.pem --tls-key key.pem

# 认证 + ACL
./mqtt-broker --port 1883 --username alice --password secret --acl-file acl.txt
```

完整参数见 `./mqtt-broker --help`。

## 能力

| 模块 | 支持 |
| --- | --- |
| 协议 | MQTT 3.1.1，QoS 0/1/2，Retained，Will，Keepalive，PING |
| 会话 | `clean_session=0`，订阅恢复，离线队列，packet id 管理，inflight 重传 |
| 可靠性 | ACK 超时、最大重传次数、持久会话 TTL 过期 |
| 网络 | 非阻塞 accept/read/write，动态缓冲，EPOLLOUT 背压，SIGTERM 优雅退出 |
| 安全 | TLS 1.2+，用户名/密码认证，发布/订阅/Will 的 topic ACL |
| 运维 | `SIGUSR1` 统计、周期指标、资源上限、状态文件原子落盘 |
| 质量 | pytest 集成测试、ASan/UBSan、GitHub Actions |

## 性能

本机 loopback 相对基准；与 Mosquitto 的对比关闭持久化、使用 `-O2` 构建：

| 场景 | 结果 |
| --- | --- |
| 20k 订阅 / 2k 消息匹配 | 约 1.81s → 0.012s（**150×**） |
| QoS1 + 持久化 | 约 0.7 万 → 5.3 万 msg/s（**7×**） |
| QoS1 64B（vs Mosquitto 2.0.21） | 1–100 client，**持平至 +45%** |
| QoS2 64B（vs Mosquitto 2.0.21） | 1–50 client，0.95–1.09× |
| QoS0 64B（vs Mosquitto 2.0.21） | 1–100 client，0.86–0.92× |
| QoS0 1KB（vs Mosquitto 2.0.21） | 50–100 client，约 1.0×（0.97–1.21×，读缓冲修复后） |
| fanout 100 订阅者 × 1000 条 | 约 1.00× |

> 以上为本地相对基准，受机器负载影响；不是生产容量承诺。  
> 持久化数字的测试口径：1 发布 / 1 订阅、64B payload、1000 条 QoS1 突发、state 位于 `/tmp`（tmpfs）、`-O0` 构建；不是 Mosquitto 对比项。

## 设计

```text
epoll event loop
  ├─ connection / TLS
  ├─ MQTT state machine
  ├─ session / inflight / offline queue
  ├─ topic trie + edge hash
  └─ persistence: dirty → snapshot → writer thread → fsync + rename
```

网络与协议状态保持在单线程中，避免锁竞争；持久化只把已经构建好的只读快照交给后台线程，主循环不等待磁盘。

## 测试

```bash
make test
# 或
.venv/bin/python -m pytest tests/ -q
```

CI 会执行普通构建、全量 pytest，以及 ASan/UBSan 构建下的全量测试。

## 已知限制 / 下一步

- 持久化仍是全量快照；状态很大时序列化是主要成本，下一步做 WAL / 增量快照。
- 1KB 高并发 QoS0 已从 0.2–0.5× 提升到约 1.0×（50–100 client，0.97–1.21×），基本与 Mosquitto 持平。
- TLS 只做传输加密，不支持 mTLS；状态文件仍为明文。
