#include "acl.h"
#include "config.h"
#include "metrics.h"
#include "mqtt.h"
#include "persistence.h"
#include "retained.h"
#include "subscription.h"
#include "connection.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>

// 工具：剩余长度变长编码解码
// 返回: 1 成功, 0 数据不足, -1 非法
static int decode_remaining_length(const uint8_t *buf, size_t len,
                                   uint32_t *out_value, size_t *out_consumed) {
    uint32_t multiplier = 1;
    uint32_t value = 0;
    size_t i = 0;

    for (;;) {
        if (i >= len)
            return 0; // 数据不够
        if (i >= 4)
            return -1; // 最多 4 字节，非法

        uint8_t byte = buf[i];
        value += (uint32_t)(byte & 0x7F) * multiplier;

        if ((byte & 0x80) == 0) {
            // 最后一个字节
            *out_value = value;
            *out_consumed = i + 1;
            return 1;
        }
        multiplier *= 128;
        i++;
    }
}

int mqtt_decode_fixed_header(const uint8_t *buf, size_t len,
                             mqtt_fixed_header_t *hdr) {
    if (len < 1)
        return 0;

    uint8_t b0 = buf[0];
    uint8_t type = (b0 >> 4) & 0x0F;
    uint8_t flags = b0 & 0x0F;

    if (type == 0 || type == 15)
        return -1; // 保留/非法

    uint32_t rl = 0;
    size_t rl_len = 0;
    int rc = decode_remaining_length(buf + 1, len - 1, &rl, &rl_len);
    if (rc <= 0)
        return rc;

    hdr->type = type;
    hdr->flags = flags;
    hdr->remaining_length = rl;
    hdr->header_len = 1 + rl_len;
    hdr->total_len = hdr->header_len + rl;
    return 1;
}

// ============ 工具：UTF-8 字符串读取 ============
// MQTT 字符串格式：2 字节长度（大端） + 内容；out_str指向字符串开头
// 返回: 1 成功, 0 数据不够, -1 格式错误
static int read_utf8_string(const uint8_t *buf, size_t len,
                            const char **out_str, size_t *out_str_len,
                            size_t *out_consumed) {
    if (len < 2)
        return 0;
    uint16_t slen = (uint16_t)((buf[0] << 8) | buf[1]);
    if (len < 2 + (size_t)slen)
        return 0;

    *out_str = (const char *)(buf + 2);
    *out_str_len = slen;
    *out_consumed = 2 + slen;
    return 1;
}

// ============ CONNECT 解码 ============
int mqtt_decode_connect(const uint8_t *body, size_t body_len,
                        mqtt_connect_t *out) {
    memset(out, 0, sizeof(*out));

    const uint8_t *p = body;
    size_t remain = body_len;
    size_t consumed;
    int rc;

    // 1. protocol name (UTF-8 string)
    const char *proto_name;
    size_t proto_name_len;
    rc = read_utf8_string(p, remain, &proto_name, &proto_name_len, &consumed);
    if (rc <= 0)
        return rc;
    if (proto_name_len != 4 || memcmp(proto_name, "MQTT", 4) != 0)
        return -1;
    p += consumed;
    remain -= consumed;

    // 2. protocol level (1 byte)
    if (remain < 1)
        return 0;
    out->proto_level = p[0];
    p += 1;
    remain -= 1;
    // 校验

    // 3. connect flags
    if (remain < 1)
        return 0;
    uint8_t connect_flags = p[0];
    p += 1;
    remain -= 1;
    // 检验：bit 0 必须为 0（保留位，MQTT 3.1.1 规范要求）
    if (connect_flags & 0x01)
        return -1;
    out->clean_session = (connect_flags >> 1) & 0x01;
    out->has_will = (connect_flags >> 2) & 0x01;
    out->will_qos = (connect_flags >> 3) & 0x03;
    out->will_retain = (connect_flags >> 5) & 0x01;
    out->has_username = (connect_flags >> 7) & 0x01;
    out->has_password = (connect_flags >> 6) & 0x01;

    // 没有 Will 时 Will QoS / Will Retain 必须为 0
    if (!out->has_will && (out->will_qos != 0 || out->will_retain != 0))
        return -1;
    if (out->will_qos == 3)
        return -1; // 保留值，非法
    if (out->has_password && !out->has_username)
        return -1; // MQTT 3.1.1：没有用户名时不能有密码

    // 4. Keep Alive（2 字节，大端）
    if (remain < 2)
        return 0;
    out->keep_alive = (uint16_t)((p[0] << 8) | p[1]);
    p += 2;
    remain -= 2;

    // ==== payload ====

    // 5. Client ID
    rc = read_utf8_string(p, remain, &out->client_id, &out->client_id_len,
                          &consumed);
    if (rc <= 0)
        return rc;
    p += consumed;
    remain -= consumed;

    // 6. Will Topic + Will Payload（如有）
    if (out->has_will) {
        rc = read_utf8_string(p, remain, &out->will_topic, &out->will_topic_len,
                              &consumed);
        if (rc <= 0)
            return rc;
        p += consumed;
        remain -= consumed;

        // Will Payload 也是 UTF-8 string 格式（2 字节长度前缀）
        rc = read_utf8_string(p, remain, (const char **)&out->will_payload,
                              &out->will_payload_len, &consumed);
        if (rc <= 0)
            return rc;
        p += consumed;
        remain -= consumed;
    }

    // 7. Username
    if (out->has_username) {
        rc = read_utf8_string(p, remain, &out->username, &out->username_len,
                              &consumed);
        if (rc <= 0)
            return rc;
        p += consumed;
        remain -= consumed;
    }

    // 8. Password（也是 UTF-8 string 格式，虽然叫 password 但其实是 binary
    // data）
    if (out->has_password) {
        rc = read_utf8_string(p, remain, (const char **)&out->password,
                              &out->password_len, &consumed);
        if (rc <= 0)
            return rc;
        p += consumed;
        remain -= consumed;
    }

    return 1;
}

// ============ 编码 CONNACK ============
// CONNACK 固定为 4 字节：0x20 0x02 <session_present> <return_code>
static int send_connack(connection_t *c, uint8_t session_present,
                        uint8_t return_code) {
    uint8_t pkt[4];
    pkt[0] = 0x20; // type=2 (CONNACK), flags=0
    pkt[1] = 0x02; // remaining_length = 2
    pkt[2] = session_present;
    pkt[3] = return_code;
    return connection_send(c, pkt, sizeof(pkt));
}

// ======== PUBLISH 解码 ========
int mqtt_decode_publish(const uint8_t *body, size_t body_len, uint8_t flags,
                        mqtt_publish_t *out) {
    memset(out, 0, sizeof(*out));
    out->dup = (flags >> 3) & 0x01;
    out->qos = (flags >> 1) & 0x03;
    out->retain = flags & 0x01;

    // 检查标志
    if (out->qos > 2)
        return -1;
    if (out->qos == 0 && out->dup)
        return -1; // QoS0不能带 DUP

    // topic
    const char *topic;
    size_t topic_len;
    size_t consumed;
    int rc = read_utf8_string(body, body_len, &topic, &topic_len, &consumed);
    if (rc <= 0)
        return rc;
    if (topic_len == 0)
        return -1;
    if (memchr(topic, '+', topic_len) || memchr(topic, '#', topic_len))
        return -1; // topic 不能有通配符

    out->topic = topic;
    out->topic_len = topic_len;
    body += consumed;
    body_len -= consumed;

    // packet ID (QoS > 0)
    if (out->qos > 0) {
        if (body_len < 2)
            return 0;
        out->packet_id = (uint16_t)((body[0] << 8) | body[1]);
        if (out->packet_id == 0)
            return -1;
        body += 2;
        body_len -= 2;
    }

    out->payload = body;
    out->payload_len = body_len;
    return 1;
}

// ==== PUBLISH 编码） ====
static int mqtt_send_publish_packet(connection_t *c, const char *topic,
                                    size_t topic_len, const uint8_t *payload,
                                    size_t payload_len, uint8_t qos,
                                    uint8_t retain, uint8_t dup,
                                    uint16_t packet_id) {
    // 固定头
    uint8_t flags = (uint8_t)(((dup & 1) << 3) | ((qos & 0x03) << 1) |
                              (retain & 1));
    uint8_t hdr[1 + 4]; // remaining length 最多4个字节表示
    size_t i = 0;
    hdr[i++] = (uint8_t)((MQTT_PUBLISH << 4) | flags);

    uint32_t remaining = (uint32_t)(2 + topic_len + payload_len);
    if (qos > 0)
        remaining += 2; // packet id

    do {
        uint8_t b = (uint8_t)(remaining % 128);
        remaining /= 128;
        if (remaining > 0)
            b |= 0x80;
        hdr[i++] = b;
    } while (remaining > 0);

    // 发送固定头，i 通常是小于 1 + 4的
    if (connection_send(c, hdr, i) < 0)
        return -1;

    // topic
    uint8_t tlen_be[2] = {(uint8_t)(topic_len >> 8), (uint8_t)topic_len & 0xff};
    if (connection_send(c, tlen_be, 2) < 0)
        return -1;
    if (connection_send(c, topic, topic_len) < 0)
        return -1;

    // QoS > 0 需要 packet id
    if (qos > 0) {
        uint8_t pid_be[2] = {(uint8_t)(packet_id >> 8),
                             (uint8_t)(packet_id & 0xff)};
        if (connection_send(c, pid_be, 2) < 0)
            return -1;
    }

    // payload
    if (payload_len > 0 && connection_send(c, payload, payload_len) < 0) {
        return -1;
    }
    return 0;
}

int mqtt_send_publish_qos0(connection_t *c, const char *topic, size_t topic_len,
                           const uint8_t *payload, size_t payload_len,
                           uint8_t retain, uint8_t dup) {
    return mqtt_send_publish_packet(c, topic, topic_len, payload, payload_len, 0,
                                    retain, dup, 0);
}

int mqtt_send_publish_qos1(connection_t *c, const char *topic, size_t topic_len,
                           const uint8_t *payload, size_t payload_len,
                           uint8_t retain, uint8_t dup, uint16_t packet_id) {
    return mqtt_send_publish_packet(c, topic, topic_len, payload, payload_len, 1,
                                    retain, dup, packet_id);
}

int mqtt_send_publish_qos2(connection_t *c, const char *topic, size_t topic_len,
                           const uint8_t *payload, size_t payload_len,
                           uint8_t retain, uint8_t dup, uint16_t packet_id) {
    return mqtt_send_publish_packet(c, topic, topic_len, payload, payload_len, 2,
                                    retain, dup, packet_id);
}

// QoS2 握手的三种 4 字节确认包：PUBREC / PUBREL / PUBCOMP
static int send_pid_ack(connection_t *c, uint8_t first_byte,
                        uint16_t packet_id) {
    uint8_t pkt[4] = {first_byte, 0x02, (uint8_t)(packet_id >> 8),
                      (uint8_t)(packet_id & 0xff)};
    return connection_send(c, pkt, sizeof(pkt));
}

static int send_pubrec(connection_t *c, uint16_t packet_id) {
    return send_pid_ack(c, 0x50, packet_id);
}

static int send_pubrel(connection_t *c, uint16_t packet_id) {
    return send_pid_ack(c, 0x62, packet_id);
}

static int send_pubcomp(connection_t *c, uint16_t packet_id) {
    return send_pid_ack(c, 0x70, packet_id);
}

// 统一入口：先处理 retained 存储，再向现有订阅者实时转发。
// 实时转发的 RETAIN 必须为 0；retained 消息只在新订阅时以 RETAIN=1 投递。
static void broker_publish(const char *topic, size_t topic_len,
                           const uint8_t *payload, size_t payload_len,
                           uint8_t qos, uint8_t retain) {
    if (retain) {
        if (retained_store(topic, topic_len, payload, payload_len, qos) < 0) {
            fprintf(stderr, "failed to store retained message for '%.*s'\n",
                    (int)topic_len, topic);
        }
    }
    subscription_deliver(topic, topic_len, payload, payload_len, qos, 0);
}

void mqtt_publish_will(connection_t *c) {
    if (!c || !c->will.topic || c->state != CONN_STATE_CONNECTED)
        return;

    printf("fd=%d publish Will topic='%.*s' qos=%u retain=%u\n", c->fd,
           (int)c->will.topic_len, c->will.topic, c->will.qos,
           c->will.retain);
    broker_publish(c->will.topic, c->will.topic_len, c->will.payload,
                   c->will.payload_len, c->will.qos, c->will.retain);
    // 发布后立即释放，避免 shutdown / destroy 重复发布
    connection_clear_will(c);
}

// ======== SUBSCRIBE / UNSUBSCRIBE ========
static int send_suback(connection_t *c, uint16_t packet_id,
                       const uint8_t *codes, size_t n) {
    uint8_t pkt[1 + 4 + 2 + 64];
    size_t i = 0;
    pkt[i++] = (uint8_t)((MQTT_SUBACK << 4) | 0x00);

    uint32_t remaining = (uint32_t)(2 + n);
    do {
        uint8_t b = (uint8_t)(remaining % 128);
        remaining /= 128;
        if (remaining > 0)
            b |= 0x80;
        pkt[i++] = b;
    } while (remaining > 0);

    pkt[i++] = (uint8_t)(packet_id >> 8);
    pkt[i++] = (uint8_t)(packet_id & 0xff);
    memcpy(pkt + i, codes, n);
    i += n;
    return connection_send(c, pkt, i);
}

static int send_unsuback(connection_t *c, uint16_t packet_id) {
    uint8_t pkt[4];
    pkt[0] = (uint8_t)((MQTT_UNSUBACK << 4) | 0x00);
    pkt[1] = 0x02;
    pkt[2] = (uint8_t)(packet_id >> 8);
    pkt[3] = (uint8_t)(packet_id & 0xff);
    return connection_send(c, pkt, 4);
}
// ======== handler ============

// ============ 处理 CONNECT ============

static unsigned g_auto_client_seq = 0;

// 把会话离线队列里的消息搬进 inflight 并发送
static void session_flush_queue(session_t *s, connection_t *c) {
    while (s->queue_head) {
        if (s->inflight_count >= MAX_INFLIGHT)
            break;

        queued_message_t *q = s->queue_head;
        uint16_t packet_id = session_alloc_packet_id(s);
        if (packet_id == 0)
            break;

        inflight_entry_t *e =
            session_inflight_add(s, packet_id, q->qos, q->retain, q->topic,
                                 q->topic_len, q->payload, q->payload_len);
        if (!e)
            break;

        // 先把“队列 -> inflight”落盘，再从网络发送。
        // 这样即使崩溃，消息也不会既留在队列又留在 inflight（QoS2 会因此重复）。
        session_queue_pop(s);

        int rc;
        if (e->qos == 2) {
            rc = mqtt_send_publish_qos2(c, e->topic, e->topic_len, e->payload,
                                        e->payload_len, e->retain, 0,
                                        e->packet_id);
        } else {
            rc = mqtt_send_publish_qos1(c, e->topic, e->topic_len, e->payload,
                                        e->payload_len, e->retain, 0,
                                        e->packet_id);
        }
        if (rc < 0)
            break; // 发送失败没关系，inflight 已落盘，后续会重传
    }
}

// 持久会话重连：先原 packet id + DUP=1 重发未确认消息，再补发离线队列
static void session_resume(session_t *s, connection_t *c) {
    uint64_t now = connection_now_ms();
    for (int i = 0; i < MAX_INFLIGHT; i++) {
        inflight_entry_t *e = &s->inflight[i];
        if (!e->in_use)
            continue;

        // 新连接上重新开始计算 ACK 超时和重试次数；
        // 离线期间不把旧连接上的等待时间算到新连接头上。
        e->first_sent_ms = now;
        e->retry_count = 0;

        if (e->qos == 2 && e->state == INFLIGHT_WAIT_PUBCOMP) {
            // 已经进入 PUBREL 阶段，重连后重发 PUBREL
            if (send_pubrel(c, e->packet_id) == 0)
                e->last_sent_ms = now;
        } else if (mqtt_send_publish_packet(c, e->topic, e->topic_len,
                                            e->payload, e->payload_len, e->qos,
                                            e->retain, 1,
                                            e->packet_id) == 0) {
            // QoS1 或 QoS2 的 PUBLISH 阶段：DUP=1 重发
            e->last_sent_ms = now;
        }
    }

    // 入站 QoS2：已经回过 PUBREC 但还没等到 PUBREL 的消息，重连后补发 PUBREC
    for (int i = 0; i < MAX_INCOMING_QOS2; i++) {
        incoming_qos2_entry_t *in = &s->incoming_qos2[i];
        if (in->in_use)
            send_pubrec(c, in->packet_id);
    }

    session_flush_queue(s, c);
}

// 把 CONNECT 中的 Will 复制到连接；失败返回 -1
static int connection_store_will(connection_t *c,
                                 const mqtt_connect_t *mc) {
    if (!mc->has_will)
        return 0;

    if (mc->will_qos > 2) {
        fprintf(stderr, "fd=%d invalid Will QoS %u\n", c->fd, mc->will_qos);
        return -1;
    }
    if (mc->will_topic_len == 0 ||
        memchr(mc->will_topic, '+', mc->will_topic_len) ||
        memchr(mc->will_topic, '#', mc->will_topic_len)) {
        fprintf(stderr, "fd=%d invalid Will topic\n", c->fd);
        return -1;
    }

    char *topic = malloc(mc->will_topic_len + 1);
    uint8_t *payload = malloc(mc->will_payload_len ? mc->will_payload_len : 1);
    if (!topic || !payload) {
        free(topic);
        free(payload);
        return -1;
    }

    memcpy(topic, mc->will_topic, mc->will_topic_len);
    topic[mc->will_topic_len] = '\0';
    if (mc->will_payload_len)
        memcpy(payload, mc->will_payload, mc->will_payload_len);

    c->will.topic = topic;
    c->will.topic_len = mc->will_topic_len;
    c->will.payload = payload;
    c->will.payload_len = mc->will_payload_len;
    c->will.qos = mc->will_qos;
    c->will.retain = mc->will_retain;
    return 0;
}

// 返回: 0 继续, -1 关闭连接
static int handle_connect(connection_t *c, const uint8_t *body,
                          size_t body_len) {
    // 状态检查：必须处于 CONNECTING
    if (c->state != CONN_STATE_CONNECTING) {
        fprintf(stderr, "fd=%d protocol violation: CONNECT after CONNECT\n",
                c->fd);
        return -1;
    }

    mqtt_connect_t mc;
    int rc = mqtt_decode_connect(body, body_len, &mc);
    if (rc <= 0) {
        fprintf(stderr, "fd=%d malformed CONNECT\n", c->fd);
        return -1;
    }

    // 协议级别校验
    if (mc.proto_level != 4) {
        fprintf(stderr, "fd=%d unsupported protocol level %u\n", c->fd,
                mc.proto_level);
        send_connack(c, 0, MQTT_CONNACK_BAD_PROTO_VERSION);
        connection_request_close(c);
        return -1; // 发完就关
    }

    // 用户名密码认证（可选）
    if (g_config.auth_username) {
        size_t user_len = strlen(g_config.auth_username);
        int ok = mc.has_username && mc.username_len == user_len &&
                 memcmp(mc.username, g_config.auth_username, user_len) == 0;

        if (ok && g_config.auth_password) {
            size_t pass_len = strlen(g_config.auth_password);
            ok = mc.has_password && mc.password_len == pass_len &&
                 memcmp(mc.password, g_config.auth_password, pass_len) == 0;
        }

        if (!ok) {
            fprintf(stderr, "fd=%d authentication failed\n", c->fd);
            send_connack(c, 0, MQTT_CONNACK_BAD_CREDENTIALS);
            connection_request_close(c);
            return -1;
        }
    }

    // 授权身份：启用认证时用配置的用户；未启用认证时一律视为 anonymous，
    // 不信任 CONNECT 里客户端自报的 username，避免 ACL 被冒充。
    const char *identity = g_config.auth_username ? g_config.auth_username
                                                   : "anonymous";
    snprintf(c->username, sizeof(c->username), "%s", identity);

    // Will 本质上是发布，CONNECT 阶段先做写权限检查。
    if (mc.has_will &&
        !acl_check_publish(c->username, mc.will_topic, mc.will_topic_len)) {
        fprintf(stderr, "fd=%d Will topic denied\n", c->fd);
        send_connack(c, 0, MQTT_CONNACK_NOT_AUTHORIZED);
        connection_request_close(c);
        return -1;
    }

    // Client ID 校验
    if (mc.client_id_len == 0 && !mc.clean_session) {
        fprintf(stderr, "fd=%d empty client id with clean_session=0\n", c->fd);
        send_connack(c, 0, MQTT_CONNACK_IDENTIFIER_REJECTED);
        connection_request_close(c);
        return -1;
    }

    if (mc.client_id_len >= MAX_CLIENT_ID_LEN) {
        fprintf(stderr, "fd=%d client id too long (%zu bytes)\n", c->fd,
                mc.client_id_len);
        send_connack(c, 0, MQTT_CONNACK_IDENTIFIER_REJECTED);
        connection_request_close(c);
        return -1;
    }

    if (connection_store_will(c, &mc) < 0)
        return -1;
    c->keep_alive = mc.keep_alive;

    char client_id[MAX_CLIENT_ID_LEN];
    if (mc.client_id_len == 0) {
        // 空 client id + clean_session=1：由 broker 生成唯一 id，避免会话冲突
        snprintf(client_id, sizeof(client_id), "$auto-%u",
                 ++g_auto_client_seq);
    } else {
        memcpy(client_id, mc.client_id, mc.client_id_len);
        client_id[mc.client_id_len] = '\0';
    }

    session_t *s = session_find(client_id);
    uint8_t session_present = 0;

    if (s) {
        // 同一个 client id 的旧连接必须断开（会话状态保留给新连接）
        if (s->conn) {
            connection_t *old = s->conn;
            s->conn = NULL;
            old->session = NULL;
            printf("fd=%d takeover client_id='%s' from fd=%d\n", c->fd,
                   client_id, old->fd);
            connection_mark_dead(old);
        }

        // clean session 的状态不可恢复：新连接带 clean=1，或旧会话本身是 clean
        if (s->clean_session || mc.clean_session) {
            session_destroy(s);
            s = NULL;
        } else {
            session_present = 1;
        }
    }

    if (!s) {
        s = session_create(client_id, mc.clean_session, mc.keep_alive);
        if (!s) {
            fprintf(stderr, "fd=%d out of memory for session\n", c->fd);
            send_connack(c, 0, MQTT_CONNACK_SERVER_UNAVAILABLE);
            connection_request_close(c);
            return -1;
        }
    } else {
        s->keep_alive = mc.keep_alive;
        s->clean_session = mc.clean_session;
    }

    session_attach(s, c);

    printf("fd=%d CONNECT: client_id='%s', clean=%u, keepalive=%u, proto=%u, "
           "session_present=%u\n",
           c->fd, s->client_id, mc.clean_session, mc.keep_alive,
           mc.proto_level, session_present);

    // 发送 CONNACK 接受
    if (send_connack(c, session_present, MQTT_CONNACK_ACCEPTED) < 0)
        return -1;

    if (session_present)
        session_resume(s, c);

    return 0;
}

// ======= handle PUBLISH ========
static int handle_publish(connection_t *c, const uint8_t *body, size_t body_len,
                          uint8_t flags) {
    mqtt_publish_t p;
    int rc = mqtt_decode_publish(body, body_len, flags, &p);
    if (rc <= 0) {
        fprintf(stderr, "fd=%d malformed PUBLISH\n", c->fd);
        return -1;
    }

    if (!acl_check_publish(c->username, p.topic, p.topic_len)) {
        fprintf(stderr, "fd=%d PUBLISH denied topic='%.*s'\n", c->fd,
                (int)p.topic_len, p.topic);
        return -1;
    }

    metrics_message_received();
    printf("fd=%d PUBLISH topic='%.*s' payload=%zu bytes qos=%u retain=%u\n",
           c->fd, (int)p.topic_len, p.topic, p.payload_len, p.qos, p.retain);

    session_t *sess = c->session;
    if (!sess)
        return -1;

    // QoS2 第一阶段：保存消息，回 PUBREC，等 PUBREL 后再投递
    if (p.qos == 2) {
        if (session_incoming_qos2_find(sess, p.packet_id)) {
            // 重复 PUBLISH：消息已在等 PUBREL，只重发 PUBREC
            printf("fd=%d duplicate QoS2 PUBLISH mid=%u, resend PUBREC\n",
                   c->fd, p.packet_id);
            return send_pubrec(c, p.packet_id) < 0 ? -1 : 0;
        }

        if (!session_incoming_qos2_add(sess, p.packet_id, p.retain, p.topic,
                                       p.topic_len, p.payload, p.payload_len)) {
            fprintf(stderr, "fd=%d incoming QoS2 table full\n", c->fd);
            return -1;
        }

        if (send_pubrec(c, p.packet_id) < 0)
            return -1;
        printf("fd=%d PUBREC sent for mid=%u (wait PUBREL)\n", c->fd,
               p.packet_id);
        return 0;
    }

    // QoS0 或 QoS1：先完成 retained 存储 / 离线入队 / 转发，
    // 再回 PUBACK，确保客户端收到确认时持久化状态已经落盘。
    broker_publish(p.topic, p.topic_len, p.payload, p.payload_len, p.qos,
                   p.retain);

    if (p.qos == 1) {
        uint8_t puback[4] = {
            0x40, 0x02,
            (uint8_t)(p.packet_id >> 8),
            (uint8_t)(p.packet_id & 0xFF)
        };
        if (connection_send(c, puback, sizeof(puback)) < 0)
            return -1;
        printf("fd=%d PUBACK sent for mid=%u\n", c->fd, p.packet_id);
    }

    printf("fd=%d PUBLISH handled (retain=%u)\n", c->fd, p.retain);
    return 0;
}

// ======== handle PUBACK ========
static int handle_puback(connection_t *c, const uint8_t *body,
                         size_t body_len) {
    if (body_len != 2) {
        fprintf(stderr, "fd=%d invalid PUBACK length %zu\n", c->fd, body_len);
        return -1;
    }

    uint16_t packet_id = (uint16_t)((body[0] << 8) | body[1]);
    if (packet_id == 0) {
        fprintf(stderr, "fd=%d invalid PUBACK packet id 0\n", c->fd);
        return -1;
    }

    session_t *sess = c->session;
    inflight_entry_t *e =
        sess ? session_inflight_find(sess, packet_id) : NULL;

    if (e && e->qos == 1) {
        session_inflight_remove(sess, packet_id);
        printf("fd=%d PUBACK mid=%u, inflight=%d\n", c->fd, packet_id,
               sess->inflight_count);
        session_flush_queue(sess, c);
    } else if (e) {
        printf("fd=%d PUBACK mid=%u for QoS2 message, ignored\n", c->fd,
               packet_id);
    } else {
        printf("fd=%d PUBACK mid=%u (unknown, ignored)\n", c->fd, packet_id);
    }
    return 0;
}

// ======== handle PUBREC ========
// 订阅者收到 QoS2 PUBLISH 后的第一阶段确认：broker 回 PUBREL
static int handle_pubrec(connection_t *c, const uint8_t *body,
                         size_t body_len) {
    if (body_len != 2) {
        fprintf(stderr, "fd=%d invalid PUBREC length %zu\n", c->fd, body_len);
        return -1;
    }

    uint16_t packet_id = (uint16_t)((body[0] << 8) | body[1]);
    if (packet_id == 0)
        return -1;

    session_t *sess = c->session;
    inflight_entry_t *e =
        sess ? session_inflight_find(sess, packet_id) : NULL;
    if (!e || e->qos != 2) {
        printf("fd=%d PUBREC mid=%u (unknown, ignored)\n", c->fd, packet_id);
        return 0;
    }

    if (e->state != INFLIGHT_WAIT_PUBCOMP) {
        e->state = INFLIGHT_WAIT_PUBCOMP;
        e->last_sent_ms = connection_now_ms();
        persistence_save();
    }
    if (send_pubrel(c, packet_id) < 0)
        return -1;

    printf("fd=%d PUBREL sent for mid=%u\n", c->fd, packet_id);
    return 0;
}

// ======== handle PUBREL ========
// 发布者完成 QoS2 第二阶段：此时才真正投递消息，并回 PUBCOMP
static int handle_pubrel(connection_t *c, const uint8_t *body,
                         size_t body_len) {
    if (body_len != 2) {
        fprintf(stderr, "fd=%d invalid PUBREL length %zu\n", c->fd, body_len);
        return -1;
    }

    uint16_t packet_id = (uint16_t)((body[0] << 8) | body[1]);
    if (packet_id == 0)
        return -1;

    session_t *sess = c->session;
    incoming_qos2_entry_t *in =
        sess ? session_incoming_qos2_find(sess, packet_id) : NULL;

    if (in) {
        printf("fd=%d PUBREL mid=%u -> deliver exactly once\n", c->fd,
               packet_id);
        broker_publish(in->topic, in->topic_len, in->payload, in->payload_len,
                       2, in->retain);
        session_incoming_qos2_remove(sess, packet_id);
    } else {
        // 重复 PUBREL 或未知 id：仍然回 PUBCOMP，避免发布者卡住
        printf("fd=%d PUBREL mid=%u (already completed, ignored)\n", c->fd,
               packet_id);
    }

    return send_pubcomp(c, packet_id) < 0 ? -1 : 0;
}

// ======== handle PUBCOMP ========
// 订阅者完成 QoS2 第三阶段：清理出站 inflight
static int handle_pubcomp(connection_t *c, const uint8_t *body,
                          size_t body_len) {
    if (body_len != 2) {
        fprintf(stderr, "fd=%d invalid PUBCOMP length %zu\n", c->fd,
                body_len);
        return -1;
    }

    uint16_t packet_id = (uint16_t)((body[0] << 8) | body[1]);
    if (packet_id == 0)
        return -1;

    session_t *sess = c->session;
    inflight_entry_t *e =
        sess ? session_inflight_find(sess, packet_id) : NULL;
    if (e && e->qos == 2 && e->state == INFLIGHT_WAIT_PUBCOMP) {
        session_inflight_remove(sess, packet_id);
        printf("fd=%d PUBCOMP mid=%u, inflight=%d\n", c->fd, packet_id,
               sess->inflight_count);
        session_flush_queue(sess, c);
    } else {
        printf("fd=%d PUBCOMP mid=%u (unknown, ignored)\n", c->fd, packet_id);
    }
    return 0;
}

// ============ handle PINGREQ / DISCONNECT ============

// 返回: 0 继续, -1 关闭
static int handle_pingreq(connection_t *c, const uint8_t *body,
                          size_t body_len) {
    (void)body;
    if (body_len != 0) {
        fprintf(stderr, "fd=%d invalid PINGREQ (len=%zu)\n", c->fd, body_len);
        return -1;
    }
    // PINGRESP: type=13(0xD0), remaining=0
    const uint8_t pkt[2] = {0xD0, 0x00};
    return connection_send(c, pkt, sizeof(pkt)) < 0 ? -1 : 0;
}

static int handle_disconnect(connection_t *c, const uint8_t *body,
                             size_t body_len) {
    (void)body;
    if (body_len != 0) {
        fprintf(stderr, "fd=%d invalid DISCONNECT (len=%zu)\n", c->fd,
                body_len);
        return -1;
    }
    printf("fd=%d DISCONNECT: client_id='%s' (clean exit)\n", c->fd,
           c->session ? c->session->client_id : "");
    connection_clear_will(c); // 正常断开不发布 Will
    return -1;            // 关闭连接
}

// ======== handle SUBSCRIBE =======
static int handle_subscribe(connection_t *c, const uint8_t *body,
                            size_t body_len, uint8_t flags) {
    if (flags != MQTT_SUBSCRIBE_FLAGS) {
        fprintf(stderr, "fd=%d invalid SUBSCRIBE flags 0x%x\n", c->fd, flags);
        return -1;
    }
    if (body_len < 2)
        return -1;

    uint16_t packet_id = (uint16_t)((body[0] << 8) | body[1]);
    if (packet_id == 0)
        return -1;
    body += 2;
    body_len -= 2;

    if (body_len == 0) {
        fprintf(stderr, "fd=%d SUBSCRIBE without filters\n", c->fd);
        return -1;
    }

    uint8_t codes[64];
    char granted_filters[64][MAX_FILTER_LEN];
    uint8_t granted_qos_arr[64];
    size_t n = 0;
    size_t granted_count = 0;
    while (body_len > 0) {
        if (n >= sizeof(codes)) {
            fprintf(stderr, "fd=%d too many filters in one SUBSCRIBE\n", c->fd);
            return -1;
        }

        const char *filter;
        size_t filter_len;
        size_t consumed;
        int rc =
            read_utf8_string(body, body_len, &filter, &filter_len, &consumed);
        if (rc <= 0)
            return -1;
        body += consumed;
        body_len -= consumed;

        if (body_len < 1)
            return -1;
        uint8_t req_qos = body[0] & 0x03;
        body += 1;
        body_len -= 1;

        char filter_buf[MAX_FILTER_LEN];
        if (filter_len >= sizeof(filter_buf) || req_qos > 2 ||
            !topic_filter_valid_len(filter, filter_len)) {
            codes[n++] = MQTT_SUBACK_FAILURE;
            continue;
        }
        memcpy(filter_buf, filter, filter_len);
        filter_buf[filter_len] = '\0';

        if (!acl_check_subscribe(c->username, filter_buf)) {
            fprintf(stderr, "fd=%d SUBSCRIBE denied '%s'\n", c->fd,
                    filter_buf);
            codes[n++] = MQTT_SUBACK_FAILURE;
            continue;
        }

        uint8_t granted_qos = req_qos; // 已支持 QoS 0/1/2
        if (subscription_add(c->session, filter_buf, granted_qos) < 0) {
            codes[n++] = MQTT_SUBACK_FAILURE;
            continue;
        }

        printf("fd=%d SUBSCRIBE '%.*s' (req_qos=%u)\n", c->fd,
               (uint8_t)filter_len, filter, req_qos);
        codes[n++] = granted_qos;

        memcpy(granted_filters[granted_count], filter_buf, filter_len + 1);
        granted_qos_arr[granted_count] = granted_qos;
        granted_count++;
    }

    // MQTT 3.1.1：必须先发 SUBACK，再发由新订阅触发的 retained 消息
    if (send_suback(c, packet_id, codes, n) < 0)
        return -1;

    for (size_t i = 0; i < granted_count; i++) {
        int delivered = retained_deliver(c->session, granted_filters[i],
                                         granted_qos_arr[i]);
        if (delivered > 0) {
            printf("fd=%d delivered %d retained message(s) for '%s'\n",
                   c->fd, delivered, granted_filters[i]);
        }
    }
    return 0;
}

// ======== handle UNSUBSCRIBE ========
static int handle_unsubscribe(connection_t *c, const uint8_t *body,
                              size_t body_len, uint8_t flags) {
    if (flags != MQTT_UNSUBSCRIBE_FLAGS) {
        fprintf(stderr, "fd=%d invalid UNSUBSCRIBE flags 0x%X\n", c->fd, flags);
        return -1;
    }
    if (body_len < 2)
        return -1;

    uint16_t packet_id = (uint16_t)((body[0] << 8) | body[1]);
    if (packet_id == 0)
        return -1;
    body += 2;
    body_len -= 2;

    while (body_len > 0) {
        const char *filter;
        size_t filter_len;
        size_t consumed;
        int rc =
            read_utf8_string(body, body_len, &filter, &filter_len, &consumed);
        if (rc <= 0)
            return -1;
        body += consumed;
        body_len -= consumed;

        char filter_buf[MAX_FILTER_LEN];
        if (filter_len >= sizeof(filter_buf))
            continue;

        memcpy(filter_buf, filter, filter_len);
        filter_buf[filter_len] = '\0';

        if (subscription_remove(c->session, filter_buf)) {
            printf("fd=%d UNSUBSCRIBE '%s'\n", c->fd, filter_buf);
        }
    }

    return send_unsuback(c, packet_id);
}

// 把报文类型转字符串，日志用
static const char *packet_type_name(uint8_t type) {
    switch (type) {
    case MQTT_CONNECT:
        return "CONNECT";
    case MQTT_CONNACK:
        return "CONNACK";
    case MQTT_PUBLISH:
        return "PUBLISH";
    case MQTT_PUBACK:
        return "PUBACK";
    case MQTT_PUBREC:
        return "PUBREC";
    case MQTT_PUBREL:
        return "PUBREL";
    case MQTT_PUBCOMP:
        return "PUBCOMP";
    case MQTT_SUBSCRIBE:
        return "SUBSCRIBE";
    case MQTT_SUBACK:
        return "SUBACK";
    case MQTT_UNSUBSCRIBE:
        return "UNSUBSCRIBE";
    case MQTT_UNSUBACK:
        return "UNSUBACK";
    case MQTT_PINGREQ:
        return "PINGREQ";
    case MQTT_PINGRESP:
        return "PINGRESP";
    case MQTT_DISCONNECT:
        return "DISCONNECT";
    default:
        return "UNKNOWN";
    }
}

// 校验固定头 flags
static bool fixed_header_flags_valid(uint8_t type, uint8_t flags) {
    switch (type) {
    case MQTT_PUBLISH:
        // PUBLISH 的 flags 由 DUP/QoS/RETAIN 决定，不在这里校验
        return true;
    case MQTT_SUBSCRIBE:
    case MQTT_UNSUBSCRIBE:
    case MQTT_PUBREL:
        return flags == 0x02;
    default:
        return flags == 0x00;
    }
}

// 分派一条完整报文；返回 0 继续, -1 关闭
static int dispatch_packet(connection_t *c, const mqtt_fixed_header_t *hdr,
                           const uint8_t *body, size_t body_len) {
    // ---- 状态机： CONNECTING 状态只允许 CONNECT ----
    if (c->state == CONN_STATE_CONNECTING) {
        if (hdr->type != MQTT_CONNECT) {
            fprintf(stderr, "fd=%d protocol violation got %s before CONNECT\n",
                    c->fd, packet_type_name(hdr->type));
            return -1;
        }

        return handle_connect(c, body, body_len);
    }

    switch (hdr->type) {
    case MQTT_CONNECT:
        fprintf(stderr, "fd=%d protocol violation: second CONNECT\n", c->fd);
        return -1;
    case MQTT_PINGREQ:
        return handle_pingreq(c, body, body_len);
    case MQTT_DISCONNECT:
        return handle_disconnect(c, body, body_len);
    case MQTT_PUBLISH:
        return handle_publish(c, body, body_len, hdr->flags);
    case MQTT_PUBACK:
        return handle_puback(c, body, body_len);
    case MQTT_PUBREC:
        return handle_pubrec(c, body, body_len);
    case MQTT_PUBREL:
        return handle_pubrel(c, body, body_len);
    case MQTT_PUBCOMP:
        return handle_pubcomp(c, body, body_len);
    case MQTT_SUBSCRIBE:
        return handle_subscribe(c, body, body_len, hdr->flags);
    case MQTT_UNSUBSCRIBE:
        return handle_unsubscribe(c, body, body_len, hdr->flags);
    default:
        fprintf(stderr, "fd=%d unhandled packet type %s(%u)\n", c->fd,
                packet_type_name(hdr->type), hdr->type);
        return -1; // 暂时不支持，断开
    }
}

// 判断一条 inflight 是否应该放弃。
// first_sent_ms 在首次发送/重连时设置，不会随重传刷新。
static int inflight_should_drop(const inflight_entry_t *e, uint64_t now_ms,
                                const char **reason) {
    if (g_config.inflight_timeout_ms > 0 &&
        now_ms - e->first_sent_ms >=
            (uint64_t)g_config.inflight_timeout_ms) {
        *reason = "timeout";
        return 1;
    }
    if (g_config.max_inflight_retries > 0 &&
        e->retry_count >= (uint32_t)g_config.max_inflight_retries) {
        *reason = "retry limit";
        return 1;
    }
    return 0;
}

// 遍历所有会话，重传超时未确认的 QoS1 消息，并补发离线队列
void mqtt_retransmit_all(uint64_t now_ms) {
    for (session_t *s = session_first(); s; s = session_next(s)) {
        connection_t *c = s->conn;
        if (!c || c->state != CONN_STATE_CONNECTED || c->want_close ||
            c->dead)
            continue;

        for (int i = 0; i < MAX_INFLIGHT; i++) {
            inflight_entry_t *e = &s->inflight[i];
            if (!e->in_use)
                continue;

            const char *reason = NULL;
            if (inflight_should_drop(e, now_ms, &reason)) {
                // 不删除 inflight：QoS1/QoS2 的事务状态要留给重连恢复。
                // 关闭当前连接；clean session 会随连接销毁，持久会话会保留。
                printf("fd=%d inflight mid=%u qos=%u %s, closing connection "
                       "and keeping state (retries=%u)\n",
                       c->fd, e->packet_id, e->qos, reason, e->retry_count);
                connection_mark_dead(c);
                break;
            }

            if (now_ms - e->last_sent_ms < (uint64_t)g_config.qos_retry_ms)
                continue;

            if (e->qos == 2 && e->state == INFLIGHT_WAIT_PUBCOMP) {
                // 已进入 PUBREL 阶段：重发 PUBREL，而不是 PUBLISH
                if (send_pubrel(c, e->packet_id) == 0) {
                    e->last_sent_ms = now_ms;
                    e->retry_count++;
                    printf("fd=%d retransmit PUBREL mid=%u\n", c->fd,
                           e->packet_id);
                }
                continue;
            }

            // QoS1 / QoS2 PUBLISH 阶段：packet id 不变，DUP=1 重发
            if (mqtt_send_publish_packet(c, e->topic, e->topic_len, e->payload,
                                         e->payload_len, e->qos, e->retain, 1,
                                         e->packet_id) == 0) {
                e->last_sent_ms = now_ms;
                e->retry_count++;
                printf("fd=%d retransmit PUBLISH mid=%u qos=%u (DUP=1)\n",
                       c->fd, e->packet_id, e->qos);
            }
            // 发送失败（写缓冲满）就保持旧时间，下一轮再试
        }

        if (!c->dead && s->queue_head)
            session_flush_queue(s, c);
    }
}

int mqtt_process(connection_t *c) {
    for (;;) {
        size_t avail = c->rlen - c->rpos;
        if (avail == 0) {
            c->rpos = 0;
            c->rlen = 0;
            return 0;
        }

        // 1. 固定头解包
        const uint8_t *buf = (const uint8_t *)c->rbuf + c->rpos;
        mqtt_fixed_header_t hdr;
        int rc = mqtt_decode_fixed_header(buf, avail, &hdr);
        if (rc == 0)
            return 0;
        if (rc < 0) {
            fprintf(stderr, "fd=%d protocol error: invalid fixed header\n",
                    c->fd);
            return -1;
        }

        // 2. flags 校验
        if (!fixed_header_flags_valid(hdr.type, hdr.flags)) {
            fprintf(stderr, "fd=%d invalid flags 0x%X on %s\n", c->fd,
                    hdr.flags, packet_type_name(hdr.type));
            return -1;
        }

        if (avail < hdr.total_len)
            return 0;

        // 缓冲区里已经有一条完整报文
        const uint8_t *body = buf + hdr.header_len;
        size_t body_len = hdr.remaining_length;

        printf("fd=%d packet: type=%s(%u), flags=0x%X, remaining_length=%u, "
               "total=%zu\n",
               c->fd, packet_type_name(hdr.type), hdr.type, hdr.flags,
               hdr.remaining_length, hdr.total_len);

        if (dispatch_packet(c, &hdr, body, body_len) < 0)
            return -1;

        c->rpos += hdr.total_len;
        if (c->rpos == c->rlen) {
            c->rpos = 0;
            c->rlen = 0;
        }
    }
}
