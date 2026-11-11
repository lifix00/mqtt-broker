# MQTT Broker 项目进度与计划

> 更新时间：2026-11-11
> 代码规模：C 源码约 3574 行，Python 测试约 2579 行
> 当前测试：104 个用例，全部通过（0 skipped）
> 构建状态：`make` 零警告；全量测试在 ASan/UBSan 下通过

## 1. 项目概览

这是一个从零实现的 MQTT 3.1.1 Broker，目标是理解并实践：

- Linux 非阻塞网络编程：`epoll`、`accept4`、读写缓冲、背压
- MQTT 3.1.1 报文编解码与协议状态机
- 主题订阅、通配符匹配与消息路由
- QoS 0 / QoS 1 的投递语义
- 持久会话、离线消息与断线恢复

技术上使用 C11 实现 Broker，Python + pytest 编写端到端集成测试，测试通过裸 socket 直接验证线上报文，不依赖第三方 MQTT 客户端库。

**当前阶段结论：M6 核心已完成**。动态读写缓冲、会话/retained/订阅上限与 LRU 淘汰、SIGTERM/SIGINT 优雅退出、CLI 配置都已落地。下一阶段 M7 聚焦工程化收尾：CI、日志指标、认证/TLS、fuzz、持久化事务性与增量快照。

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
| Retained 消息 | ✅ 完成 | 按 topic 保存最后一条；新订阅时 RETAIN=1 投递；空 payload 删除 |
| Will 消息 | ✅ 完成 | 异常断开/协议错误/超时/接管时发布；正常 DISCONNECT 丢弃 |
| Keepalive 超时 | ✅ 完成 | 1.5 × keep_alive 未收包则断开，并走异常断开路径发布 Will |
| PUBLISH QoS 2（入站） | ✅ 完成 | PUBLISH → PUBREC → PUBREL → PUBCOMP，重复包不重复投递 |
| PUBLISH QoS 2（出站） | ✅ 完成 | 订阅者 QoS2、inflight 状态、PUBREL/PUBCOMP、超时重传 |
| 状态持久化 | ✅ 完成 | retained、持久会话、订阅、inflight、离线队列、QoS2 状态跨重启恢复 |
| 资源上限与淘汰 | ✅ 完成 | 会话/retained/订阅上限，LRU 淘汰；加载状态时跳过上限 |
| 动态读写缓冲 | ✅ 完成 | 初始 4KB 按需扩容，支持 128KB+ 报文 |
| 优雅退出 | ✅ 完成 | SIGTERM/SIGINT：停 accept、发布 Will、flush、落盘、退出 0 |
| CLI 配置 | ✅ 完成 | 端口、状态文件、上限、缓冲上限、重传间隔 |
| CI / 指标 | ❌ 未实现 | 尚无自动化 CI 和 metrics 输出 |
| 持久化事务性 | ❌ 未实现 | 快照级一致性，QoS2 极端崩溃窗口未做事务 |
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


### 3.5 Retained 消息

- 全局 retained 表按**精确 topic** 保存最后一条消息，包含 topic、payload、QoS
- PUBLISH 带 RETAIN=1 时保存/替换；payload 为空时删除该 topic 的 retained 消息
- 实时转发给已有订阅者时 RETAIN 一律置 0，只有新订阅触发的投递才置 1
- SUBSCRIBE 先回 SUBACK，再按每个成功授予的 filter 投递匹配的 retained 消息
- retained 投递 QoS = `min(retained_qos, granted_qos)`
- QoS1 retained 投递会登记 inflight，订阅者断开后可在重连时以 DUP=1 重发
- retained 表常驻内存，Broker 重启后丢失；MQTT 3.1.1 本身没有 retained 过期机制

### 3.6 Will 消息

- CONNECT 解码时提取 Will Flag、Will QoS、Will Retain、Will Topic、Will Payload
- CONNECT 成功时把 Will 深拷贝到**连接对象**，不放进持久会话，因为 Will 属于本次连接
- 校验 Will Topic 非空且不含 `+` / `#`；当前只支持 Will QoS 0/1
- 以下情况发布 Will：
  - 客户端 socket 直接关闭 / EOF
  - 读写错误、EPOLLERR / EPOLLHUP
  - 协议错误导致断开
  - Keepalive 超时
  - 同 Client ID 新连接接管旧连接
- 正常 DISCONNECT 会把 `will_suppress` 置 1，不发布 Will
- Will 带 Retain 时会同时写入 retained 表；新订阅者能收到 retained 版本
- Will 发布 QoS1 时按普通 QoS1 投递处理，离线持久会话可入队

### 3.7 Keepalive 超时

- 每个连接记录 `keep_alive`（秒）和 `last_rx_ms`（最后一次收到任何字节的时间）
- 服务端每 200ms 扫描一次，超过 `1.5 × keep_alive` 没有收到数据就断开
- `keep_alive=0` 表示关闭超时检测
- 超时连接标记为 dead，复用统一的延迟回收路径，因此会正常触发 Will
- 收到 PINGREQ 或其他任意报文都会刷新 `last_rx_ms`


### 3.8 QoS 2

**入站（发布者 → Broker）**

- 收到 QoS2 PUBLISH 后先保存到 `incoming_qos2` 表，回 PUBREC，此时不投递
- 收到 PUBREL 后才调用 `broker_publish` 真正投递，并回 PUBCOMP
- 重复 PUBLISH（同 packet id）：只重发 PUBREC，不覆盖、不重复投递
- 重复 PUBREL：仍回 PUBCOMP，但不会再次投递
- retained 存储在 PUBREL 阶段完成，保证“只写一次”
- 入站 QoS2 状态属于会话，`clean_session=0` 时随会话持久化

**出站（Broker → 订阅者）**

- 订阅可授予 QoS2，投递 QoS 继续按 `min(pub_qos, sub_qos)` 计算
- 出站 inflight 记录三阶段状态：
  - `INFLIGHT_WAIT_PUBACK`：QoS1 等 PUBACK
  - `INFLIGHT_WAIT_PUBREC`：QoS2 已发 PUBLISH，等 PUBREC
  - `INFLIGHT_WAIT_PUBCOMP`：QoS2 已发 PUBREL，等 PUBCOMP
- 收到 PUBREC 后转 `WAIT_PUBCOMP` 并发送 PUBREL
- 收到 PUBCOMP 后清理 inflight
- 超时重传：
  - `WAIT_PUBREC` 阶段重发 PUBLISH，DUP=1、packet id 不变
  - `WAIT_PUBCOMP` 阶段重发 PUBREL
- 重连恢复：
  - 未完成 PUBLISH 阶段的消息以 DUP=1 重发
  - 已进入 PUBREL 阶段的消息直接重发 PUBREL

### 3.9 状态持久化

- 新增 `persistence.c/.h`，使用二进制快照文件保存 Broker 状态
- 默认文件：`mqtt-broker.state`，可通过 `--state-file PATH` 指定
- 保存内容：
  - retained 表（topic、payload、QoS）
  - `clean_session=0` 会话：client id、keep_alive、next packet id
  - 会话订阅（filter、QoS）
  - 出站 inflight（packet id、QoS、握手状态、retain、topic、payload）
  - 离线队列（topic、payload、QoS、retain）
  - 入站 QoS2 表（packet id、retain、topic、payload）
- 写入策略：每次状态变更后写临时文件 → `fflush` + `fsync` → `rename` 原子替换
- 启动时加载状态；文件不存在则从空状态开始；文件损坏则清空并写回合法空状态
- `clean_session=1` 的会话不落盘，符合 MQTT 语义
- 重传时间戳 `sent_at_ms` 不落盘，重启后按当前时间重新计时


### 3.10 资源上限与淘汰

- 默认上限（均可通过 CLI 调整，0 表示不限制）：
  - 持久会话：1024
  - retained：4096
  - 每会话订阅：256
- 持久会话满时，优先淘汰**最久未活动的离线持久会话**；全部在线时拒绝新持久会话，返回 CONNACK 服务不可用
- retained 满时淘汰最久未更新的 retained topic
- 订阅满时 SUBACK 返回 `0x80`，不影响已有订阅
- LRU 使用单调递增序号而不是墙上时钟，避免同一毫秒内多次操作导致顺序不确定
- 从状态文件加载时跳过上限检查，避免把刚恢复的数据立即淘汰

### 3.11 动态读写缓冲

- `connection_t` 的 `rbuf` / `wbuf` 从固定数组改为动态指针：
  - 初始容量 4KB
  - 读缓冲按 2 倍扩容，默认上限 1MiB
  - 写缓冲按需扩容，默认上限 2MiB
  - 写缓冲在扩容前先尝试搬移已发送部分，减少内存增长
- 读满但还能扩容时继续读；扩容到上限仍无法组成完整报文才断开
- 128KB payload 的 PUBLISH 已有端到端测试
- 上限可通过 `--max-read-buffer` / `--max-write-buffer` 调整

### 3.12 优雅退出与配置

- 安装 SIGTERM / SIGINT 处理器，只设置 `sig_atomic_t` 标志
- 主循环检测到标志后：
  1. 停止 accept，从 epoll 移除并关闭 listen fd
  2. 对所有在线连接发布 Will（正常 DISCONNECT 过的连接已抑制）
  3. 最多 1 秒尝试 flush 所有写缓冲
  4. 销毁连接：clean session 清理，持久会话保留
  5. 最后再写一次状态文件并退出 0
- 新增 `config.c/.h`，所有上限和重传间隔集中管理
- 新增 CLI：
  - `--max-sessions` / `--max-retained` / `--max-subscriptions`
  - `--max-read-buffer` / `--max-write-buffer`
  - `--qos-retry-ms`
- 状态文件仍然是写穿（write-through），优雅退出只是兜底保存，不依赖退出时落盘

## 4. 代码结构

```text
src/
├── main.c            # 启动参数、listen socket、epoll 事件循环、优雅退出
├── config.c/.h       # 全局配置：资源上限、缓冲上限、重传间隔
├── connection.c/.h   # 连接对象、读写缓冲、EPOLLOUT、连接生命周期
├── mqtt.c/.h         # 报文编解码、协议状态机、CONNECT/PUBLISH/SUBSCRIBE/PUBACK/Will
├── session.c/.h      # 会话表、clean session、inflight、入站 QoS2、离线队列
├── subscription.c/.h # 订阅链表、主题匹配、单会话投递、消息路由与离线入队
├── retained.c/.h     # retained 消息表、保存/替换/删除、新订阅投递
└── persistence.c/.h  # 二进制状态快照、原子落盘、启动加载

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
├── test_retained.py
├── test_will.py
├── test_keepalive.py
├── test_qos2.py
├── test_persistence.py
├── test_limits.py
└── test_shutdown.py
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

```text
connection_t
├── session / state
├── keep_alive / last_rx_ms
└── will_topic / will_payload / will_qos / will_retain / will_suppress

session_t
├── inflight[]           # 出站 QoS1/QoS2，含握手状态
├── incoming_qos2[]      # 入站 QoS2，等 PUBREL
├── queue_head/queue_tail# 离线 QoS1/QoS2
└── next_packet_id
```

Will 属于连接而不是会话；连接异常关闭时发布，正常 DISCONNECT 时丢弃。

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
collected 104 items
104 passed in 41.06s
```

没有 skipped 用例：Retained、Will、Keepalive、QoS2、持久化、资源上限、动态缓冲和优雅退出都有端到端测试。

### 覆盖重点

- 正常连接、错误协议级别、空 Client ID、错误 flags
- 半包、粘包、非法包类型、非法 DISCONNECT
- 精确主题、多订阅者、二进制 payload、通配符边界
- QoS 0 / QoS 1 投递、PUBACK、packet id 高字节
- 多条 QoS1 的 packet id 唯一性
- 未确认消息的 DUP 重传、PUBACK 后停止重传
- 持久会话离线队列、重连恢复、clean session 丢弃、旧连接接管
- Retained 保存、替换、空 payload 删除、QoS1 retained、实时转发 RETAIN=0
- Will 异常断开发布、正常 DISCONNECT 不发布、协议错误发布、Will Retain、QoS1 Will
- Keepalive 超时断开、PINGREQ 续命、超时后发布 Will
- QoS2 入站/出站握手、重复 PUBLISH、重复 PUBREL、PUBLISH/PUBREL 重传
- QoS2 retained
- retained/订阅/离线队列/出站 QoS2/incoming QoS2 跨 Broker 重启恢复
- clean session 不落盘、损坏状态文件回退
- 会话/retained/订阅上限与 LRU 淘汰
- 128KB 大 payload 的动态读写缓冲
- SIGTERM 优雅退出、退出前 Will 发布
- 断连后订阅清理、二次 CONNECT、CONNECT 前 PING

### 额外验证

- `-Wall -Wextra` 编译零警告
- 全量测试在 `-fsanitize=address,undefined` 下通过
- 手工验证了：双订阅者独立 packet id、只确认其中一条时另一条继续重传、同 Client ID 接管
- 手工验证了：retained 替换/删除、Will Retain、Keepalive 超时触发 Will
- 手工验证了：QoS2 重传、incoming/outgoing 状态跨重启、接管时 Will 发布
- 手工验证了：128KB payload、三类资源上限、SIGTERM 发布 Will 并退出 0

## 6. 已知限制

1. **持久化是整文件快照**：每次状态变更都会重写整个状态文件并 `fsync`，状态大时写放大会明显；后续可改为 journal / 增量快照。
2. **崩溃一致性是快照级，不是事务级**：极端情况下在 QoS2 PUBREL 处理中途崩溃，重启后可能出现重复投递或确认丢失；正常停机/重启路径已测试。
3. **没有会话/retained 过期**：MQTT 3.1.1 本身没有 Session Expiry 和 retained 过期，离线会话与 retained 会一直保留到被 LRU 淘汰或手动清理。
4. **有硬性缓冲上限**：默认读 1MiB、写 2MiB；超过上限的单包会被断开，可通过 CLI 调大，但不是无限支持。
5. **队列和 inflight 仍固定为 1024**：每会话最多 1024 条 inflight、1024 条离线消息、1024 条入站 QoS2，满了会丢弃/断开并打日志。
6. **Keepalive 是字节级计时**：收到任意字节都会刷新 `last_rx_ms`，不是严格的“完整控制包”级计时；扫描周期 200ms。
7. **没有 CONNECT 超时**：客户端连上后一直不发 CONNECT 不会被主动清理。
8. **Will / Retained 大 topic 限制**：实时转发路径仍使用 256 字节 topic 缓冲，超过该长度的 topic 在实时投递时会被丢弃（retained 新订阅路径不受此限制）。
9. **优雅退出最多等 1 秒 flush**：对端极慢时，部分 Will/QoS 消息可能未发出；持久会话的 inflight 会在下次重连时重发。
10. **无认证与加密**：username/password 只解析不校验，不支持 TLS；状态文件是明文。
11. **没有 CI / 指标 / fuzz**：目前靠本地 pytest + ASan/UBSan，尚未接入自动化 CI 和运行指标。
12. **状态文件无压缩/迁移**：格式升级需要手动兼容，旧版本状态文件可能被当作损坏文件清空。

## 7. 下一阶段计划

### P0：工程化收尾

| 任务 | 说明 | 验收标准 |
| --- | --- | --- |
| CI | `make` + pytest + sanitizer 在 CI 自动执行 | 每次提交自动跑 |
| 日志与指标 | 连接数、订阅数、inflight、retained、丢弃计数，支持日志级别 | 可通过日志/指标观察运行状态 |
| 持久化事务性 | 解决 QoS2 PUBREL 中途崩溃的重复/丢失窗口 | 崩溃注入测试通过 |
| 状态文件演进 | 版本迁移、增量快照或 journal，降低写放大 | 大状态写入可接受 |

### P1：协议与运维能力

| 任务 | 说明 | 验收标准 |
| --- | --- | --- |
| 认证/TLS | 用户名密码校验与 TLS 接入 | 可选启用 |
| CONNECT 超时与大 topic | 半开连接回收；动态 topic 缓冲 | 长 topic/半开连接可测试 |
| 队列上限配置化 | 把 inflight/离线队列/QoS2 表上限也纳入配置 | 不改代码即可调整 |
| 配置文件中文化 | CLI 之外的配置文件支持 | 适合容器/服务化部署 |

### P2：健壮性验证

| 任务 | 说明 | 验收标准 |
| --- | --- | --- |
| 模糊测试 | 固定头、Remaining Length、CONNECT/PUBLISH、状态文件 | 不崩溃、不越界 |
| 崩溃注入 | 在 PUBREL/落盘关键点注入崩溃 | 状态可恢复或明确失败 |
| 长稳测试 | 大量连接、订阅、retained 循环 | 内存/句柄不泄漏 |
| 文档与示例 | 部署、配置、限制说明 | 新用户可独立跑通 |

## 8. 建议的推进顺序

1. **先做 CI + 日志指标**：把已有 104 个测试和 sanitizer 固化成流水线。
2. **再做持久化事务性与状态文件演进**：解决写放大和 QoS2 崩溃窗口。
3. **然后做认证/TLS、CONNECT 超时、大 topic、队列上限配置化**。
4. **最后做 fuzz、崩溃注入、长稳测试**。

## 9. 里程碑

| 里程碑 | 内容 | 状态 |
| --- | --- | --- |
| M1 | epoll 网络层 + MQTT 骨架 + CONNECT/PING/DISCONNECT | ✅ |
| M2 | 订阅/通配符 + QoS 0 发布订阅 | ✅ |
| M3 | QoS 1 完整链路 + 持久会话与断线恢复 | ✅ |
| M4 | Retained + Will + Keepalive | ✅ |
| M5 | QoS 2 + 会话/Retained 磁盘持久化 | ✅ |
| M6 | 资源上限、动态缓冲、优雅退出、CLI 配置 | ✅ |
| M7 | CI、日志指标、认证/TLS、fuzz、持久化事务性 | ⏳ 下一步 |
