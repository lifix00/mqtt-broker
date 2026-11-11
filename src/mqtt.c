#include "mqtt.h"
#include "subscription.h"
#include "connection.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>

// QoS1 消息超过这个时间还没收到 PUBACK 就重传（DUP=1）
#define QOS1_RETRY_TIMEOUT_MS 2000

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
    out->has_username = (connect_flags >> 7) & 0x01;
    out->has_password = (connect_flags >> 6) & 0x01;

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

// ==== PUBLISH 编码（QoS 0/1 共用） ====
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

        if (mqtt_send_publish_qos1(c, q->topic, q->topic_len, q->payload,
                                   q->payload_len, q->retain, 0,
                                   packet_id) < 0) {
            session_inflight_remove(s, packet_id);
            break;
        }
        session_queue_pop(s);
    }
}

// 持久会话重连：先原 packet id + DUP=1 重发未确认消息，再补发离线队列
static void session_resume(session_t *s, connection_t *c) {
    uint64_t now = connection_now_ms();
    for (int i = 0; i < MAX_INFLIGHT; i++) {
        inflight_entry_t *e = &s->inflight[i];
        if (!e->in_use)
            continue;
        if (mqtt_send_publish_qos1(c, e->topic, e->topic_len, e->payload,
                                   e->payload_len, e->retain, 1,
                                   e->packet_id) == 0) {
            e->sent_at_ms = now;
        }
    }
    session_flush_queue(s, c);
}

// 标记旧连接待回收：不立即 free，避免同一批 epoll 事件里出现悬空指针
static void connection_mark_dead(connection_t *old) {
    old->dead = 1;
    old->want_close = 1;
    epoll_ctl(g_epfd, EPOLL_CTL_DEL, old->fd, NULL);
}

// 返回: 0 继续, -1 关闭连接
static int handle_connect(connection_t *c, const uint8_t *body,
                          size_t body_len) {
    // 状态检查：必须处于 CONNECTING
    if (c->state != SESSION_STATE_CONNECTING) {
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
        fprintf(stderr, "fd=%d malformed\n", c->fd);
        return -1;
    }
    if (p.qos > 1) {
        fprintf(stderr, "fd=%d Qos %u not supported yet\n", c->fd, p.qos);
        return -1;
    }

    printf("fd=%d PUBLISH topic='%s' payload=%zu bytes retain=%u\n", c->fd,
           p.topic, p.payload_len, p.retain);

    // QoS 1：先回 PUBACK（无论有没有订阅者）
    if (p.qos == 1) {
        uint8_t puback[4] = {
            0x40, 0x02,
            (uint8_t)(p.packet_id >> 8),
            (uint8_t)(p.packet_id & 0xFF)
        };
        if (connection_send(c, puback, sizeof(puback)) < 0) {
            return -1;
        }
        printf("fd=%d PUBACK sent for mid=%u\n", c->fd, p.packet_id);
    }

    // 转发（按 min(发布 QoS, 订阅 QoS) 决定投递 QoS）
    int delivered = subscription_deliver(p.topic, p.topic_len,
                                         p.payload, p.payload_len,
                                         p.qos, p.retain);
    printf("fd=%d PUBLISH delivered to %d subscriber(s)\n", c->fd, delivered);
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
    if (sess && session_inflight_remove(sess, packet_id)) {
        printf("fd=%d PUBACK mid=%u, inflight=%d\n", c->fd, packet_id,
               sess->inflight_count);
        // inflight 腾出空间后，继续发送离线期间排队的消息
        session_flush_queue(sess, c);
    } else {
        // 可能是已经清理过的重复 PUBACK，忽略即可
        printf("fd=%d PUBACK mid=%u (unknown, ignored)\n", c->fd, packet_id);
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
    // TODO(Will): 清除遗嘱消息 —— 但现在还没有 Will 存储，先不管
    return -1; // 关闭连接
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
    size_t n = 0;
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

        uint8_t granted_qos = req_qos <= 1 ? req_qos : 0; // 只支持 0/1，2暂时降级成0
        if (subscription_add(c->session, filter_buf, granted_qos) < 0) {
            codes[n++] = MQTT_SUBACK_FAILURE;
            continue;
        }

        printf("fd=%d SUBSCRIBE '%.*s' (req_qos=%u)\n", c->fd,
               (uint8_t)filter_len, filter, req_qos);
        codes[n++] = granted_qos;
    }

    return send_suback(c, packet_id, codes, n);
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

// 从 rbuf 里把已消费的头部字节挪掉
// consumed = 从 rbuf 开头要丢弃的字节数
static void rbuf_consume(connection_t *c, size_t consumed) {
    if (consumed >= c->rlen) {
        c->rlen = 0;
        return;
    }
    memmove(c->rbuf, c->rbuf + consumed, c->rlen - consumed);
    c->rlen -= consumed;
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
    if (c->state == SESSION_STATE_CONNECTING) {
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

// 遍历所有会话，重传超时未确认的 QoS1 消息，并补发离线队列
void mqtt_retransmit_all(uint64_t now_ms) {
    for (session_t *s = session_first(); s; s = session_next(s)) {
        connection_t *c = s->conn;
        if (!c || c->state != SESSION_STATE_CONNECTED || c->want_close ||
            c->dead)
            continue;

        for (int i = 0; i < MAX_INFLIGHT; i++) {
            inflight_entry_t *e = &s->inflight[i];
            if (!e->in_use)
                continue;
            if (now_ms - e->sent_at_ms < QOS1_RETRY_TIMEOUT_MS)
                continue;

            // 重传必须带 DUP=1，packet id 保持原值
            if (mqtt_send_publish_packet(c, e->topic, e->topic_len, e->payload,
                                         e->payload_len, 1, e->retain, 1,
                                         e->packet_id) == 0) {
                e->sent_at_ms = now_ms;
                printf("fd=%d retransmit PUBLISH mid=%u (DUP=1)\n", c->fd,
                       e->packet_id);
            }
            // 发送失败（写缓冲满）就保持旧时间，下一轮再试
        }

        if (s->queue_head)
            session_flush_queue(s, c);
    }
}

int mqtt_process(connection_t *c) {
    for (;;) {
        if (c->rlen == 0)
            return 0;

        // 1. 固定头解包
        mqtt_fixed_header_t hdr;
        int rc =
            mqtt_decode_fixed_header((const uint8_t *)c->rbuf, c->rlen, &hdr);
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

        if (c->rlen < hdr.total_len)
            return 0;

        // 缓冲区里已经有一条完整报文
        const uint8_t *body = (const uint8_t *)c->rbuf + hdr.header_len;
        size_t body_len = hdr.remaining_length;

        printf("fd=%d packet: type=%s(%u), flags=0x%X, remaining_length=%u, "
               "total=%zu\n",
               c->fd, packet_type_name(hdr.type), hdr.type, hdr.flags,
               hdr.remaining_length, hdr.total_len);

        if (dispatch_packet(c, &hdr, body, body_len) < 0)
            return -1;

        rbuf_consume(c, hdr.total_len);
    }
}
