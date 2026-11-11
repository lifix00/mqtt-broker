# MQTT Broker 项目进度与计划

> 更新时间：2026-11-10
> 代码规模：C 源码约 2085 行，Python 测试约 1485 行
> 当前测试：72 个用例，71 passed，1 skipped（retained 消息尚未实现）
> 构建状态：`make` 零警告；全量测试在 ASan/UBSan 下通过

## 1. 项目概览

这是一个从零实现的 MQTT 3.1.1 Broker，目标是理解并实践：

- Linux 非阻塞网络编程：`epoll`、`accept4`、读写缓冲、背压
- MQTT 3.1.1 报文编解码与协议状态机
- 主题订阅、通配符匹配与消息路由
- QoS 0 / QoS 1 的投递语义
- 持久会话、离线消息与断线恢复

技术上使用 C11 实现 Broker，Python + pytest 编写端到端集成测试，测试通过裸 socket 直接验证线上报文，不依赖第三方 MQTT 客户端库。

**当前阶段结论：QoS 1 已完整落地**，包括入站 PUBACK、出站 QoS 1、packet id 管理、inflight 表、PUBACK 清理、DUP 超时重传、持久会话离线队列和重连恢复。下一阶段的主线是 Retained、Will、Keepalive 和 QoS 2。

## 2. 进度总览

| 模块 | 状态 | 说明 |
| --- | --- | --- |
| epoll 网络层 | ✅ 完成 | 单线程事件循环、非阻塞读写、EPOLLOUT 动态注册 |
| MQTT 固定头/变长长度 | ✅ 完成 | 半包、粘包、非法剩余长度处理 |
| CONNECT / CONNACK | ✅ 完成 | 协议级别校验、Client ID 校验、空 Client ID 自动分配 |
| PINGREQ / PINGRESP | ✅ 完成 | — |
| DISCONNECT | ✅ 完成 | 正常断开与协议违规断开 |
| SUBSCRIBE / SUBACK | ✅ 完成 | 多 filter、QoS 授予、非法 filter 返回 0x80 |
| UNSUBSCRIBE / UNSUBACK | ✅ 完成 | 按 filter 精确删除 |
| 主题匹配 | ✅ 完成 | `+`、`#`、空层级、边界用例 |
| PUBLISH QoS 0 | ✅ 完成 | 广播、二进制 payload、无订阅者不报错 |
| PUBLISH QoS 1（入站） | ✅ 完成 | 解码 packet id、回 PUBACK、无订阅者也确认 |
| PUBLISH QoS 1（出站） | ✅ 完成 | 按 `min(pub_qos, sub_qos)` 投递、per-session packet id |
| QoS 1 确认与重传 | ✅ 完成 | inflight、PUBACK 清理、2s 超时 DUP=1 重传 |
| 持久会话 clean_session=0 | ✅ 完成 | 会话表、离线队列、重连恢复、旧连接接管 |
| Retained 消息 | ❌ 未实现 | 测试当前 skip |
| Will 消息 | ❌ 未实现 | CONNECT 已解析，尚未存储和触发 |
| Keepalive 超时 | ❌ 未实现 | keep_alive 已解析，未启动超时踢人 |
| QoS 2 | ❌ 未实现 | 入站 QoS 2 直接断开；订阅 QoS 2 降级授予 0 |
| 认证 / TLS | ❌ 未实现 | username/password 已解析，未校验 |

## 3. 已完成能力

### 3.1 网络层

- 全局 `epoll` 同时监听 listen fd 和所有客户端 fd
- `accept4` 使用 `SOCK_NONBLOCK | SOCK_CLOEXEC`
- 每个连接维护读缓冲 `rbuf` 和写缓冲 `wbuf`
- 读侧循环读到 `EAGAIN`，再交给协议层拆包
- 写侧优先直接写，内核缓冲满时注册 `EPOLLOUT`，后续 flush
- 支持半包、粘包、一次事件处理多条报文
- `SIGPIPE` 忽略，避免写已关闭 socket 导致进程退出

### 3.2 协议层

- 固定头解析：包类型、flags、Remaining Length 变长编码
- CONNECT 解码：协议名/级别、Clean Session、Keep Alive、Will、用户名密码
- 协议状态机：CONNECT 之前只允许 CONNECT；重复 CONNECT 断开
- CONNACK 返回码：接受、协议版本错误、Client ID 非法、服务不可用
- 非法包类型、非法 flags、非法长度均断开连接，不崩溃、不 hang
- 空 Client ID + `clean_session=1` 时由 Broker 生成 `$auto-N` 唯一 ID

### 3.3 订阅与路由

- 全局订阅链表，订阅项归属于**会话**而不是某一次连接
- 同一会话对同一 filter 重复订阅是幂等的，只更新 QoS
- `+` 匹配单层，`#` 匹配剩余所有层，支持空层级
- 订阅 filter 合法性校验，非法时 SUBACK 返回 `0x80`
- 投递 QoS 计算：`min(publish_qos, subscription_qos)`
- 实时转发时 RETAIN 置 0（符合 MQTT 3.1.1）
- 会话销毁时自动清理其全部订阅

### 3.4 QoS 1 与持久会话

**入站 QoS 1**

- 正确解码 16 位 packet id（含高字节，不会被截断）
- 先回 PUBACK，再向订阅者转发
- 无订阅者时也回 PUBACK，发布者连接保持正常

**出站 QoS 1**

- 每个会话独立维护 packet id 空间，1..65535 循环并跳过 inflight 占用项
- 发送前把 topic/payload 深拷贝进 inflight 表
- 订阅者回 PUBACK 后按 packet id 精确清理
- 2 秒未确认则重传：packet id 不变、DUP=1
- 重传由主循环每 200ms 扫描一次驱动
- 连接关闭时：clean session 释放全部 inflight；持久会话保留，供重连恢复

**持久会话（clean_session=0）**

- 会话表按 Client ID 索引，保存订阅、inflight、离线队列、packet id 状态
- 离线期间匹配的 QoS 1 消息进入队列（QoS 0 不保存）
- 重连时 CONNACK 的 `session_present=1`
- 重连后先以原 packet id + DUP=1 重发未确认消息，再按 FIFO 补发离线队列
- 重连时带 `clean_session=1` 会丢弃旧会话和旧订阅，`session_present=0`
- 同一 Client ID 的新连接会接管旧连接：旧连接被关闭，会话状态保留给新连接
- 旧连接采用延迟回收，避免同一批 epoll 事件中的悬空指针

## 4. 代码结构

```text
src/
├── main.c            # 启动参数、listen socket、epoll 事件循环、定时重传驱动
├── connection.c/.h   # 连接对象、读写缓冲、EPOLLOUT、连接生命周期
├── mqtt.c/.h         # 报文编解码、协议状态机、CONNECT/PUBLISH/SUBSCRIBE/PUBACK
├── session.c/.h      # 会话表、clean session、inflight、离线队列、packet id
└── subscription.c/.h # 订阅链表、主题匹配、消息路由与离线入队

tests/
├── conftest.py       # 每个测试启动/停止 Broker，随机端口
├── mqtt_client.py    # 最小 MQTT 3.1.1 测试客户端
├── _helpers.py       # 裸 socket 报文读取工具
├── test_connect.py
├── test_pubsub.py
├── test_wildcards.py
├── test_qos1.py
├── test_session.py
├── test_lifecycle.py
├── test_errors.py
├── test_half_packet.py
├── test_client_qos1.py
└── test_retained.py  # 暂时 skip
```

### 关键数据结构

```text
session_t
├── client_id / clean_session / keep_alive
├── conn                  # 当前在线连接，NULL 表示离线
├── next_packet_id
├── inflight[1024]        # 已发送、等待 PUBACK 的 QoS1 消息
└── queue_head/queue_tail # 离线期间排队的 QoS1 消息
```

`inflight` 和离线队列都保存在会话层，因此连接断开不会丢失 QoS 1 状态；这是 `clean_session=0` 能恢复的基础。

## 5. 测试与验证

### 运行方式

```bash
make                 # 编译 Broker
make test            # 运行全部 pytest（使用 uv）
# 或
.venv/bin/python -m pytest tests/ -q
```

### 当前结果

```text
collected 72 items
71 passed, 1 skipped in 18.69s
```

跳过项：`test_retained.py::test_retained_message`，原因是 Retained 消息尚未实现。

### 覆盖重点

- 正常连接、错误协议级别、空 Client ID、错误 flags
- 半包、粘包、非法包类型、非法 DISCONNECT
- 精确主题、多订阅者、二进制 payload、通配符边界
- QoS 0 / QoS 1 投递、PUBACK、packet id 高字节
- 多条 QoS1 的 packet id 唯一性
- 未确认消息的 DUP 重传、PUBACK 后停止重传
- 持久会话离线队列、重连恢复、clean session 丢弃、旧连接接管
- 断连后订阅清理、二次 CONNECT、CONNECT 前 PING

### 额外验证

- `-Wall -Wextra` 编译零警告
- 全量测试在 `-fsanitize=address,undefined` 下通过
- 手工验证了：双订阅者独立 packet id、只确认其中一条时另一条继续重传、同 Client ID 接管

## 6. 已知限制

1. **会话仅存内存**：Broker 进程重启后，`clean_session=0` 的会话、inflight 和离线队列全部丢失，尚未做磁盘持久化。
2. **没有会话过期**：MQTT 3.1.1 本身没有 Session Expiry，离线会话会一直保留；后续可以增加管理上限或自定义过期策略。
3. **单包受读缓冲限制**：`READ_BUF_SIZE=4096`，超过约 4KB 的 PUBLISH 会因读缓冲满而断开；写缓冲上限 64KB。
4. **队列和 inflight 有上限**：每会话最多 1024 条 inflight、1024 条离线消息，满了会丢弃并打日志。
5. **重传定时器固定 2 秒**：`QOS1_RETRY_TIMEOUT_MS` 是编译期常量，尚未做成配置项。
6. **Keepalive 未生效**：`keep_alive` 已解析，但没有超时检测和断开逻辑。
7. **Will / Retained 未实现**：CONNECT 中解析了 Will 字段，但未存储、未在异常断开时发布；Retained 消息未保存，实时转发时 RETAIN 一律置 0。
8. **QoS 2 未实现**：收到 QoS 2 PUBLISH 会断开；SUBSCRIBE 请求 QoS 2 时按 0 授予。
9. **无认证与加密**：username/password 只解析不校验，不支持 TLS。
10. **无优雅退出**：Broker 收到 SIGTERM 时直接退出，未做连接和会话清理。

## 7. 下一阶段计划

### P0：补齐 MQTT 3.1.1 常用能力

| 任务 | 说明 | 验收标准 |
| --- | --- | --- |
| Retained 消息 | 保存每个 topic 的最后一条 retained PUBLISH；新订阅时投递并置 RETAIN=1；空 payload 删除 retained | 取消 `test_retained.py` 的 skip，补充新增/覆盖/删除用例 |
| Will 消息 | CONNECT 时保存 Will topic/payload/QoS/retain；异常断开时发布，正常 DISCONNECT 时丢弃 | 异常断线（直接 close）后订阅者收到 Will；正常 DISCONNECT 不收到 |
| Keepalive | 每个连接记录最后收包时间；超过 1.5 × keep_alive 未收到任何报文则断开 | 客户端不发 PING 超时被断开；正常 PING 保持连接 |
| 统一异常断开路径 | 区分正常 DISCONNECT 与异常断开，为 Will 和日志提供依据 | 正常/异常断开行为可区分并可测试 |

### P1：QoS 2 与会话可靠性

| 任务 | 说明 | 验收标准 |
| --- | --- | --- |
| QoS 2 入站 | PUBLISH → PUBREC → PUBREL → PUBCOMP，去重 | 重复 packet id 不重复投递；完整握手可测试 |
| QoS 2 出站 | 维护 QoS2 inflight 状态，处理 PUBREC/PUBREL/PUBCOMP | 订阅者完成握手后状态清理 |
| 会话磁盘持久化 | 将 session、订阅、inflight、离线队列落盘，启动时恢复 | 重启 Broker 后 `clean_session=0` 会话可恢复 |
| 会话数量/内存上限 | 限制离线会话数量，必要时淘汰最旧会话 | 大量离线 Client 不会耗尽内存 |

### P2：工程化与可运维性

| 任务 | 说明 | 验收标准 |
| --- | --- | --- |
| 动态读缓冲 | 支持大于 4KB 的 PUBLISH，按 Remaining Length 扩容 | 64KB payload 可正常收发 |
| 配置化 | 端口、重传间隔、队列上限、日志级别从配置/环境变量读取 | 不改代码即可调整 |
| 单元测试 | 为 topic 匹配、长度编解码、session 表写纯 C 或 Python 单测 | 边界用例覆盖 |
| CI | `make` + pytest + sanitizer 自动执行 | 每次提交自动跑 |
| 日志与指标 | 连接数、订阅数、inflight 数、丢弃计数 | 可观测 |
| 认证/TLS | 用户名密码校验与 TLS 接入 | 可选启用 |

## 8. 建议的推进顺序

1. **先做 P0 的 Retained + Will + Keepalive**：这三项是 MQTT 3.1.1 最常用、也最容易在真实客户端上暴露问题的能力。
2. **再做 QoS 2**：在现有 QoS1 inflight 基础上扩展状态机，避免过早引入会话落盘复杂度。
3. **然后做会话落盘和上限**：让 `clean_session=0` 在 Broker 重启后仍然成立。
4. **最后做工程化**：动态缓冲、配置化、CI、日志指标、认证/TLS。

## 9. 里程碑

| 里程碑 | 内容 | 状态 |
| --- | --- | --- |
| M1 | epoll 网络层 + MQTT 骨架 + CONNECT/PING/DISCONNECT | ✅ |
| M2 | 订阅/通配符 + QoS 0 发布订阅 | ✅ |
| M3 | QoS 1 完整链路 + 持久会话与断线恢复 | ✅ |
| M4 | Retained + Will + Keepalive | ⏳ 下一步 |
| M5 | QoS 2 + 会话磁盘持久化 | 计划中 |
| M6 | 工程化与可运维性 | 计划中 |
