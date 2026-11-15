#include "subscription.h"
#include "config.h"
#include "connection.h"
#include "metrics.h"
#include "mqtt.h"
#include "persistence.h"
#include "topic_trie.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// 全部订阅的全局链表，供统计/调试遍历；删除时 O(1) 摘除
static subscription_t *g_head = NULL;
static subscription_t *g_tail = NULL;
static int g_count = 0;

// ==== 初始化/遍历 ====

void subscription_init(void) {
    topic_trie_init();
    g_head = NULL;
    g_tail = NULL;
    g_count = 0;
}

subscription_t *subscription_first(void) { return g_head; }

subscription_t *subscription_next(subscription_t *s) {
    return s ? s->next : NULL;
}

subscription_t *subscription_session_first(session_t *s) {
    return s ? s->subscriptions : NULL;
}

subscription_t *subscription_session_next(subscription_t *s) {
    return s ? s->session_next : NULL;
}

int subscription_count(void) { return g_count; }

// ==== 添加/删除 ====

int subscription_add(session_t *sess, const char *filter, uint8_t qos) {
    if (!sess || !filter)
        return -1;

    size_t len = strlen(filter);
    if (len == 0 || len >= MAX_FILTER_LEN)
        return -1;

    // 幂等：同一（session, filter）只更新 QoS
    topic_node_t *node = topic_trie_find(filter);
    if (node) {
        for (subscription_t *s = node->subscribers; s; s = s->node_next) {
            if (s->session == sess) {
                s->qos = qos;
                session_touch(sess);
                persistence_save();
                return 0;
            }
        }
    }

    if (!persistence_is_loading() && g_config.max_subscriptions > 0 &&
        sess->subscription_count >= (size_t)g_config.max_subscriptions) {
        fprintf(stderr, "session '%s' subscription limit reached (%d)\n",
                sess->client_id, g_config.max_subscriptions);
        return -1;
    }

    subscription_t *s = calloc(1, sizeof(*s));
    if (!s)
        return -1;
    memcpy(s->topic_filter, filter, len + 1);
    s->session = sess;
    s->qos = qos;

    if (!node) {
        node = topic_trie_find_or_create(filter);
        if (!node) {
            free(s);
            return -1;
        }
    }
    s->node = node;

    // 挂到 trie 节点的订阅者链表
    s->node_next = node->subscribers;
    node->subscribers = s;

    // 挂到全局链表尾部（顺序稳定，便于持久化）
    s->prev = g_tail;
    s->next = NULL;
    if (g_tail)
        g_tail->next = s;
    else
        g_head = s;
    g_tail = s;

    // 挂到会话的订阅链表头部
    s->session_next = sess->subscriptions;
    s->session_prev = NULL;
    if (sess->subscriptions)
        sess->subscriptions->session_prev = s;
    sess->subscriptions = s;
    sess->subscription_count++;

    g_count++;
    session_touch(sess);
    persistence_save();
    return 0;
}

// 从三个链表和 trie 中摘除一条订阅，并回收空节点；不 free victim
static void detach_subscription(subscription_t *victim) {
    session_t *sess = victim->session;
    topic_node_t *node = victim->node;

    if (node) {
        subscription_t **pp = &node->subscribers;
        while (*pp && *pp != victim)
            pp = &(*pp)->node_next;
        if (*pp)
            *pp = victim->node_next;
    }

    if (victim->prev)
        victim->prev->next = victim->next;
    else
        g_head = victim->next;
    if (victim->next)
        victim->next->prev = victim->prev;
    else
        g_tail = victim->prev;

    if (sess) {
        if (victim->session_prev)
            victim->session_prev->session_next = victim->session_next;
        else
            sess->subscriptions = victim->session_next;
        if (victim->session_next)
            victim->session_next->session_prev = victim->session_prev;
        if (sess->subscription_count > 0)
            sess->subscription_count--;
    }

    if (g_count > 0)
        g_count--;
    if (node)
        topic_trie_prune(node);
}

void subscription_remove_all(session_t *sess) {
    if (!sess)
        return;

    while (sess->subscriptions) {
        subscription_t *victim = sess->subscriptions;
        detach_subscription(victim);
        free(victim);
    }
    sess->subscription_count = 0;
    persistence_save();
}

int subscription_remove(session_t *sess, const char *filter) {
    if (!sess || !filter)
        return 0;

    topic_node_t *node = topic_trie_find(filter);
    if (!node)
        return 0;

    subscription_t *victim = NULL;
    for (subscription_t *s = node->subscribers; s; s = s->node_next) {
        if (s->session == sess) {
            victim = s;
            break;
        }
    }
    if (!victim)
        return 0;

    detach_subscription(victim);
    free(victim);
    session_touch(sess);
    persistence_save();
    return 1;
}

// ==== filter 校验与字符串匹配（retained 仍在使用）====

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

// ==== 投递 ====

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
        metrics_message_dropped();
        return 0;
    }

    if (qos == 0) {
        if (mqtt_send_publish_qos0(c, topic, topic_len, payload, payload_len,
                                   retain, 0) < 0) {
            metrics_message_dropped();
            return 0;
        }
        metrics_message_sent();
        return 1;
    }

    // QoS1/QoS2：为会话分配 packet id，登记 inflight 后发送。
    // 本函数只负责发送，不等待确认；PUBACK/PUBREC/PUBCOMP 由 mqtt.c
    // 的 handle_* 处理，超时重传由 mqtt_retransmit_all 负责。
    uint16_t packet_id = session_alloc_packet_id(sess);
    if (packet_id == 0) {
        fprintf(stderr, "fd=%d inflight table full, drop qos%u msg\n", c->fd,
                qos);
        metrics_message_dropped();
        return 0;
    }

    inflight_entry_t *e =
        session_inflight_add(sess, packet_id, qos, retain, topic, topic_len,
                             payload, payload_len);
    if (!e) {
        fprintf(stderr, "fd=%d out of memory for inflight msg\n", c->fd);
        metrics_message_dropped();
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
        metrics_message_dropped();
        return 0;
    }
    metrics_message_sent();
    return 1;
}

typedef struct {
    const char *topic; // 完整 topic，投递时使用（不能用匹配游标）
    size_t topic_len;
    const uint8_t *payload;
    size_t payload_len;
    uint8_t qos;
    uint8_t retain;
} deliver_ctx_t;

static int deliver_subscribers(subscription_t *subs,
                               const deliver_ctx_t *ctx) {
    int delivered = 0;
    for (subscription_t *sub = subs; sub; sub = sub->node_next) {
        // 投递 QoS = min(pub_qos, sub_qos)
        uint8_t deliver_qos = ctx->qos < sub->qos ? ctx->qos : sub->qos;
        delivered += subscription_deliver_to_session(
            sub->session, ctx->topic, ctx->topic_len, ctx->payload,
            ctx->payload_len, deliver_qos, ctx->retain);
    }
    return delivered;
}

static int deliver_subscribers_cb(struct subscription *subs, void *ctx) {
    const deliver_ctx_t *dctx = ctx;
    return deliver_subscribers(subs, dctx);
}

int subscription_deliver(const char *topic, size_t topic_len,
                         const uint8_t *payload, size_t payload_len,
                         uint8_t pub_qos, uint8_t retain) {
    (void)retain; // 转发给已有订阅者时，RETAIN 一律为 0（MQTT 3.1.1）
    if (!topic)
        return 0;

    deliver_ctx_t ctx = {
        .topic = topic,
        .topic_len = topic_len,
        .payload = payload,
        .payload_len = payload_len,
        .qos = pub_qos,
        .retain = 0,
    };
    return topic_trie_match(topic, topic_len, deliver_subscribers_cb, &ctx);
}
