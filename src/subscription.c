#include "subscription.h"
#include "config.h"
#include "connection.h"
#include "mqtt.h"
#include "persistence.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static subscription_t *g_head = NULL;

void subscription_init(void) { g_head = NULL; }

subscription_t *subscription_first(void) { return g_head; }

subscription_t *subscription_next(subscription_t *s) {
    return s ? s->next : NULL;
}

int subscription_count(void) {
    int n = 0;
    for (subscription_t *s = g_head; s; s = s->next)
        n++;
    return n;
}

int subscription_add(session_t *sess, const char *filter, uint8_t qos) {
    // 幂等，先做查找，如果有相同（session, filter）存在，更新 qos
    for (subscription_t *s = g_head; s; s = s->next) {
        if (s->session == sess && strcmp(s->topic_filter, filter) == 0) {
            s->qos = qos;
            session_touch(sess);
            persistence_save();
            return 0;
        }
    }

    if (!persistence_is_loading() && g_config.max_subscriptions > 0) {
        int count = 0;
        for (subscription_t *s = g_head; s; s = s->next) {
            if (s->session == sess)
                count++;
        }
        if (count >= g_config.max_subscriptions) {
            fprintf(stderr,
                    "session '%s' subscription limit reached (%d)\n",
                    sess->client_id, g_config.max_subscriptions);
            return -1;
        }
    }

    subscription_t *s = calloc(1, sizeof(*s));
    if (!s) return -1;

    size_t len = strlen(filter);
    if (len >= sizeof(s->topic_filter)) {
        free(s);
        return -1;
    }

    memcpy(s->topic_filter, filter, len + 1);
    s->session = sess;
    s->qos = qos;
    s->next = g_head;
    g_head = s;
    session_touch(sess);
    persistence_save();
    return 0;
}

void subscription_remove_all(session_t *sess) {
    subscription_t **pp = &g_head;
    while (*pp) {
        if ((*pp)->session == sess) {
            subscription_t *victim = *pp;
            *pp = victim->next;
            free(victim);
        } else {
            pp = &(*pp)->next;
        }
    }
    persistence_save();
}

int subscription_remove(session_t *sess, const char *filter) {
    subscription_t **pp = &g_head;
    while (*pp) {
        if ((*pp)->session == sess && strcmp((*pp)->topic_filter, filter) == 0) {
            subscription_t *victim = *pp;
            *pp = victim->next;
            free(victim);
            session_touch(sess);
            persistence_save();
            return 1;
        } else {
            pp = &(*pp)->next;
        }
    }
    return 0;
}

// filter 是C字符串
bool topic_filter_valid(const char *filter) {
    if (!filter || !*filter)
        return false;

    size_t len = strlen(filter);
    for (size_t i = 0; i < len; i++) {
        if (filter[i] == '#') {
            // # 必须是最后一个字符
            if (i != len - 1)
                return false;
            // # 独占一层， 前一个符号是 '/' 或者不存在
            if (i > 0 && filter[i - 1] != '/')
                return false;
        } else if (filter[i] == '+') {
            // + 必须独占一层，前一个字符是'/'或开头，后一个字符是'/'或结尾
            if (i > 0 && filter[i - 1] != '/')
                return false;
            if (i + 1 < len && filter[i + 1] != '/')
                return false;
        }
    }
    return true;
}

bool topic_filter_valid_len(const char *filter, size_t len) {
    if (!filter || len == 0)
        return false;
    for (size_t i = 0; i < len; i++) {
        if (filter[i] == '#') {
            if (i != len - 1)
                return false;
            if (i > 0 && filter[i - 1] != '/')
                return false;
        } else if (filter[i] == '+') {
            if (i > 0 && filter[i - 1] != '/')
                return false;
            if (i + 1 < len && filter[i + 1] != '/')
                return false;
        }
    }
    return true;
}

bool topic_matches(const char *filter, const char *topic) {
    const char *f = filter;
    const char *t = topic;

    while (*f) {
        // # 必须是最后一层
        if (*f == '#') {
            return f[1] == '\0'; // # 后面不能再有内容
        }

        const char *f_end = strchr(f, '/');
        const char *t_end = strchr(t, '/');
        size_t f_len = f_end ? (size_t)(f_end - f) : strlen(f);
        size_t t_len = t_end ? (size_t)(t_end - t) : strlen(t);

        if (f_len == 1 && *f == '+') {
            // + 匹配任意一层，但 topic 必须有这一层（t 不能已结束）
            // 如果 t 已经结束（*t == '\0' 且没有 t_end），+ 不匹配
            if (*t == '\0' && !t_end)
                return false;
        } else {
            if (f_len != t_len || memcmp(f, t, f_len) != 0)
                return false;
        }

        // 移动 f
        if (f_end) {
            f = f_end + 1;
            // 如果下一层是 #，当前层匹配完就整体成功
            if (*f == '#' && f[1] == '\0')
                return true;
        } else {
            f += f_len;
        }

        // 移动 t
        if (t_end)
            t = t_end + 1;
        else
            t += t_len;
    }

    return *t == '\0';
}

int subscription_deliver_to_session(session_t *sess, const char *topic,
                                    size_t topic_len, const uint8_t *payload,
                                    size_t payload_len, uint8_t qos,
                                    uint8_t retain) {
    connection_t *c = sess->conn;

    // 离线（或已被接管、等待回收）的会话：QoS0 丢弃，QoS1/QoS2 持久会话入队
    if (!c || c->dead) {
        if (qos >= 1 && !sess->clean_session) {
            if (session_queue_push(sess, topic, topic_len, payload, payload_len,
                                   qos, retain) == 0) {
                return 1;
            }
            fprintf(stderr, "session '%s' offline queue full, drop message\n",
                    sess->client_id);
        }
        return 0;
    }

    if (qos == 0) {
        return mqtt_send_publish_qos0(c, topic, topic_len, payload, payload_len,
                                      retain, 0) == 0
                   ? 1
                   : 0;
    }

    // QoS1/QoS2：为会话分配 packet id，登记 inflight 后再发送。
    // QoS1 等 PUBACK；QoS2 等 PUBREC/PUBCOMP。超时由 mqtt_retransmit_all 重传。
    uint16_t packet_id = session_alloc_packet_id(sess);
    if (packet_id == 0) {
        fprintf(stderr, "fd=%d inflight table full, drop qos%u msg\n", c->fd,
                qos);
        return 0;
    }

    inflight_entry_t *e =
        session_inflight_add(sess, packet_id, qos, retain, topic, topic_len,
                             payload, payload_len);
    if (!e) {
        fprintf(stderr, "fd=%d out of memory for inflight msg\n", c->fd);
        return 0;
    }

    int rc;
    if (qos == 2) {
        rc = mqtt_send_publish_qos2(c, topic, topic_len, payload, payload_len,
                                    retain, 0, packet_id);
    } else {
        rc = mqtt_send_publish_qos1(c, topic, topic_len, payload, payload_len,
                                    retain, 0, packet_id);
    }
    if (rc < 0) {
        session_inflight_remove(sess, packet_id);
        return 0;
    }
    return 1;
}

int subscription_deliver(const char *topic, size_t topic_len,
                          const uint8_t *payload, size_t payload_len,
                          uint8_t pub_qos, uint8_t retain) {
    (void)retain; // 转发给已有订阅者时，RETAIN 一律为 0（MQTT 3.1.1）

    char topic_buf[MAX_FILTER_LEN];
    if (topic_len >= sizeof(topic_buf)) {
        fprintf(stderr, "topic too long in delivery (%zu bytes)\n", topic_len);
        return 0;
    }
    memcpy(topic_buf, topic, topic_len);
    topic_buf[topic_len] = '\0';

    int delivered = 0;
    for (subscription_t *s = g_head; s; s = s->next) {
        if (!topic_matches(s->topic_filter, topic_buf))
            continue;

        // 投递 QoS = min(pub_qos, sub_qos)
        uint8_t deliver_qos = pub_qos < s->qos ? pub_qos : s->qos;
        delivered += subscription_deliver_to_session(
            s->session, topic, topic_len, payload, payload_len, deliver_qos, 0);
    }
    return delivered;
}
