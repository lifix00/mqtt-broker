#include "config.h"
#include "persistence.h"
#include "retained.h"
#include "subscription.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static retained_message_t *g_head = NULL;
static uint64_t g_retained_seq = 0;

static void retained_free(retained_message_t *m);

static int retained_evict_oldest(void) {
    retained_message_t **pp = &g_head;
    retained_message_t **victim_pp = NULL;
    uint64_t oldest = 0;

    while (*pp) {
        if (!victim_pp || (*pp)->last_updated_seq < oldest) {
            victim_pp = pp;
            oldest = (*pp)->last_updated_seq;
        }
        pp = &(*pp)->next;
    }
    if (!victim_pp)
        return -1;

    retained_message_t *victim = *victim_pp;
    *victim_pp = victim->next;
    printf("evict retained topic '%.*s' (max_retained=%d)\n",
           (int)victim->topic_len, victim->topic, g_config.max_retained);
    retained_free(victim);
    return 0;
}

void retained_init(void) { g_head = NULL; }

retained_message_t *retained_first(void) { return g_head; }

retained_message_t *retained_next(retained_message_t *m) {
    return m ? m->next : NULL;
}

int retained_count(void) {
    int n = 0;
    for (retained_message_t *m = g_head; m; m = m->next)
        n++;
    return n;
}

void retained_clear_all(void) {
    retained_message_t *m = g_head;
    while (m) {
        retained_message_t *next = m->next;
        retained_free(m);
        m = next;
    }
    g_head = NULL;
}

static void retained_free(retained_message_t *m) {
    free(m->topic);
    free(m->payload);
    free(m);
}

int retained_store(const char *topic, size_t topic_len, const uint8_t *payload,
                   size_t payload_len, uint8_t qos) {
    retained_message_t **pp = &g_head;
    while (*pp) {
        if ((*pp)->topic_len == topic_len &&
            memcmp((*pp)->topic, topic, topic_len) == 0) {
            break;
        }
        pp = &(*pp)->next;
    }

    retained_message_t *old = *pp;

    // 空 payload 表示删除 retained 消息，不重新插入
    if (payload_len == 0) {
        if (old) {
            *pp = old->next;
            retained_free(old);
        }
        persistence_save();
        return 0;
    }

    if (!persistence_is_loading() && g_config.max_retained > 0) {
        while (retained_count() >= g_config.max_retained) {
            if (retained_evict_oldest() < 0)
                return -1;
        }
    }

    retained_message_t *m = calloc(1, sizeof(*m));
    if (!m)
        return -1;

    // topic 需要 NUL 结尾，retained_deliver 里会用 topic_matches（按 C 字符串）
    m->topic = malloc(topic_len + 1);
    m->payload = malloc(payload_len);
    if (!m->topic || !m->payload) {
        free(m->topic);
        free(m->payload);
        free(m);
        return -1;
    }

    memcpy(m->topic, topic, topic_len);
    m->topic[topic_len] = '\0';
    memcpy(m->payload, payload, payload_len);
    m->topic_len = topic_len;
    m->payload_len = payload_len;
    m->qos = qos;
    m->last_updated_seq = ++g_retained_seq;

    if (old) {
        m->next = old->next;
        *pp = m;
        retained_free(old);
    } else {
        m->next = g_head;
        g_head = m;
    }
    persistence_save();
    return 0;
}

int retained_deliver(session_t *sess, const char *filter, uint8_t granted_qos) {
    int delivered = 0;
    for (retained_message_t *m = g_head; m; m = m->next) {
        if (!topic_matches(filter, m->topic))
            continue;

        uint8_t qos = m->qos < granted_qos ? m->qos : granted_qos;
        delivered += subscription_deliver_to_session(
            sess, m->topic, m->topic_len, m->payload, m->payload_len, qos, 1);
    }
    return delivered;
}
