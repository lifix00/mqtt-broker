#include "session.h"
#include "config.h"
#include "connection.h"
#include "persistence.h"
#include "subscription.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define SESSION_BUCKET_COUNT 64

static session_t *g_buckets[SESSION_BUCKET_COUNT];
static uint64_t g_session_seq = 0;

static unsigned session_hash(const char *client_id) {
    unsigned h = 5381;
    while (*client_id)
        h = ((h << 5) + h) ^ (unsigned char)*client_id++;
    return h % SESSION_BUCKET_COUNT;
}

void session_store_init(void) {
    memset(g_buckets, 0, sizeof(g_buckets));
}

session_t *session_find(const char *client_id) {
    if (!client_id)
        return NULL;
    unsigned b = session_hash(client_id);
    for (session_t *s = g_buckets[b]; s; s = s->hash_next) {
        if (strcmp(s->client_id, client_id) == 0)
            return s;
    }
    return NULL;
}

void session_touch(session_t *s) {
    if (s)
        s->last_active_seq = ++g_session_seq;
}

int session_persistent_count(void) {
    int n = 0;
    for (session_t *s = session_first(); s; s = session_next(s)) {
        if (!s->clean_session)
            n++;
    }
    return n;
}

static int evict_oldest_offline_session(void) {
    session_t *victim = NULL;
    for (session_t *s = session_first(); s; s = session_next(s)) {
        if (s->clean_session || s->conn)
            continue;
        if (!victim || s->last_active_seq < victim->last_active_seq)
            victim = s;
    }
    if (!victim)
        return -1;

    printf("evict offline session '%s' (max_sessions=%d)\n",
           victim->client_id, g_config.max_sessions);
    session_destroy(victim);
    return 0;
}

session_t *session_create(const char *client_id, uint8_t clean_session,
                          uint16_t keep_alive) {
    if (!client_id || strlen(client_id) >= MAX_CLIENT_ID_LEN)
        return NULL;

    if (clean_session == 0 && !persistence_is_loading() &&
        g_config.max_sessions > 0) {
        while (session_persistent_count() >= g_config.max_sessions) {
            if (evict_oldest_offline_session() < 0) {
                fprintf(stderr,
                        "session limit reached (%d), reject '%s'\n",
                        g_config.max_sessions, client_id);
                return NULL;
            }
        }
    }

    session_t *s = calloc(1, sizeof(*s));
    if (!s)
        return NULL;

    snprintf(s->client_id, sizeof(s->client_id), "%s", client_id);
    s->clean_session = clean_session;
    s->keep_alive = keep_alive;
    s->next_packet_id = 1;
    s->last_active_seq = ++g_session_seq;

    unsigned b = session_hash(s->client_id);
    s->hash_next = g_buckets[b];
    g_buckets[b] = s;
    persistence_save();
    return s;
}

void session_attach(session_t *s, connection_t *c) {
    s->conn = c;
    c->session = s;
    c->state = SESSION_STATE_CONNECTED;
    session_touch(s);
    persistence_save();
}

void session_detach(session_t *s) {
    if (!s || !s->conn)
        return;
    connection_t *c = s->conn;
    s->conn = NULL;
    c->session = NULL;
    c->state = SESSION_STATE_CONNECTING;
    session_touch(s);
}

void session_destroy(session_t *s) {
    if (!s)
        return;

    session_detach(s);
    subscription_remove_all(s);
    session_inflight_clear(s);
    session_queue_clear(s);
    session_incoming_qos2_clear(s);

    unsigned b = session_hash(s->client_id);
    session_t **pp = &g_buckets[b];
    while (*pp) {
        if (*pp == s) {
            *pp = s->hash_next;
            break;
        }
        pp = &(*pp)->hash_next;
    }
    free(s);
    persistence_save();
}

session_t *session_first(void) {
    for (int i = 0; i < SESSION_BUCKET_COUNT; i++) {
        if (g_buckets[i])
            return g_buckets[i];
    }
    return NULL;
}

session_t *session_next(session_t *s) {
    if (!s)
        return NULL;
    if (s->hash_next)
        return s->hash_next;

    unsigned b = session_hash(s->client_id);
    for (unsigned i = b + 1; i < SESSION_BUCKET_COUNT; i++) {
        if (g_buckets[i])
            return g_buckets[i];
    }
    return NULL;
}

void session_clear_all(void) {
    session_t *s = session_first();
    while (s) {
        session_t *next = session_next(s);
        session_destroy(s);
        s = next;
    }
}

// ==== packet id / inflight ====

uint16_t session_alloc_packet_id(session_t *s) {
    if (s->inflight_count >= MAX_INFLIGHT)
        return 0;

    // packet id 1..65535 循环，跳过仍被 inflight 占用的编号
    for (uint32_t i = 0; i < 65535; i++) {
        if (s->next_packet_id == 0)
            s->next_packet_id = 1;

        uint16_t pid = s->next_packet_id;
        s->next_packet_id = (pid == 65535) ? 1 : (uint16_t)(pid + 1);

        if (!session_inflight_find(s, pid))
            return pid;
    }
    return 0;
}

inflight_entry_t *session_inflight_find(session_t *s, uint16_t packet_id) {
    for (int i = 0; i < MAX_INFLIGHT; i++) {
        if (s->inflight[i].in_use && s->inflight[i].packet_id == packet_id)
            return &s->inflight[i];
    }
    return NULL;
}

inflight_entry_t *session_inflight_add(session_t *s, uint16_t packet_id,
                                       uint8_t qos, uint8_t retain,
                                       const char *topic, size_t topic_len,
                                       const uint8_t *payload,
                                       size_t payload_len) {
    if (s->inflight_count >= MAX_INFLIGHT)
        return NULL;

    inflight_entry_t *slot = NULL;
    for (int i = 0; i < MAX_INFLIGHT; i++) {
        if (!s->inflight[i].in_use) {
            slot = &s->inflight[i];
            break;
        }
    }
    if (!slot)
        return NULL;

    char *topic_copy = malloc(topic_len ? topic_len : 1);
    uint8_t *payload_copy = malloc(payload_len ? payload_len : 1);
    if (!topic_copy || !payload_copy) {
        free(topic_copy);
        free(payload_copy);
        return NULL;
    }
    if (topic_len)
        memcpy(topic_copy, topic, topic_len);
    if (payload_len)
        memcpy(payload_copy, payload, payload_len);

    slot->packet_id = packet_id;
    slot->in_use = 1;
    slot->qos = qos;
    slot->state = (qos == 2) ? INFLIGHT_WAIT_PUBREC : INFLIGHT_WAIT_PUBACK;
    slot->retain = retain;
    slot->sent_at_ms = connection_now_ms();
    slot->topic = topic_copy;
    slot->topic_len = topic_len;
    slot->payload = payload_copy;
    slot->payload_len = payload_len;
    s->inflight_count++;
    session_touch(s);
    persistence_save();
    return slot;
}

int session_inflight_remove(session_t *s, uint16_t packet_id) {
    inflight_entry_t *e = session_inflight_find(s, packet_id);
    if (!e)
        return 0;

    free(e->topic);
    free(e->payload);
    memset(e, 0, sizeof(*e));
    s->inflight_count--;
    session_touch(s);
    persistence_save();
    return 1;
}

void session_inflight_clear(session_t *s) {
    for (int i = 0; i < MAX_INFLIGHT; i++) {
        if (s->inflight[i].in_use) {
            free(s->inflight[i].topic);
            free(s->inflight[i].payload);
            memset(&s->inflight[i], 0, sizeof(s->inflight[i]));
        }
    }
    s->inflight_count = 0;
}

// ==== 入站 QoS2 ====

incoming_qos2_entry_t *session_incoming_qos2_find(session_t *s,
                                                  uint16_t packet_id) {
    for (int i = 0; i < MAX_INCOMING_QOS2; i++) {
        if (s->incoming_qos2[i].in_use &&
            s->incoming_qos2[i].packet_id == packet_id)
            return &s->incoming_qos2[i];
    }
    return NULL;
}

incoming_qos2_entry_t *session_incoming_qos2_add(
    session_t *s, uint16_t packet_id, uint8_t retain, const char *topic,
    size_t topic_len, const uint8_t *payload, size_t payload_len) {
    if (s->incoming_qos2_count >= MAX_INCOMING_QOS2)
        return NULL;

    incoming_qos2_entry_t *slot = NULL;
    for (int i = 0; i < MAX_INCOMING_QOS2; i++) {
        if (!s->incoming_qos2[i].in_use) {
            slot = &s->incoming_qos2[i];
            break;
        }
    }
    if (!slot)
        return NULL;

    char *topic_copy = malloc(topic_len ? topic_len : 1);
    uint8_t *payload_copy = malloc(payload_len ? payload_len : 1);
    if (!topic_copy || !payload_copy) {
        free(topic_copy);
        free(payload_copy);
        return NULL;
    }
    if (topic_len)
        memcpy(topic_copy, topic, topic_len);
    if (payload_len)
        memcpy(payload_copy, payload, payload_len);

    slot->packet_id = packet_id;
    slot->in_use = 1;
    slot->retain = retain;
    slot->topic = topic_copy;
    slot->topic_len = topic_len;
    slot->payload = payload_copy;
    slot->payload_len = payload_len;
    s->incoming_qos2_count++;
    session_touch(s);
    persistence_save();
    return slot;
}

int session_incoming_qos2_remove(session_t *s, uint16_t packet_id) {
    incoming_qos2_entry_t *e = session_incoming_qos2_find(s, packet_id);
    if (!e)
        return 0;

    free(e->topic);
    free(e->payload);
    memset(e, 0, sizeof(*e));
    s->incoming_qos2_count--;
    session_touch(s);
    persistence_save();
    return 1;
}

void session_incoming_qos2_clear(session_t *s) {
    for (int i = 0; i < MAX_INCOMING_QOS2; i++) {
        if (s->incoming_qos2[i].in_use) {
            free(s->incoming_qos2[i].topic);
            free(s->incoming_qos2[i].payload);
            memset(&s->incoming_qos2[i], 0, sizeof(s->incoming_qos2[i]));
        }
    }
    s->incoming_qos2_count = 0;
}

// ==== 离线队列 ====

int session_queue_push(session_t *s, const char *topic, size_t topic_len,
                       const uint8_t *payload, size_t payload_len, uint8_t qos,
                       uint8_t retain) {
    if (s->queue_count >= MAX_QUEUED_MESSAGES)
        return -1;

    queued_message_t *q = calloc(1, sizeof(*q));
    if (!q)
        return -1;

    q->topic = malloc(topic_len ? topic_len : 1);
    q->payload = malloc(payload_len ? payload_len : 1);
    if (!q->topic || !q->payload) {
        free(q->topic);
        free(q->payload);
        free(q);
        return -1;
    }
    if (topic_len)
        memcpy(q->topic, topic, topic_len);
    if (payload_len)
        memcpy(q->payload, payload, payload_len);

    q->topic_len = topic_len;
    q->payload_len = payload_len;
    q->qos = qos;
    q->retain = retain;
    q->next = NULL;

    if (s->queue_tail)
        s->queue_tail->next = q;
    else
        s->queue_head = q;
    s->queue_tail = q;
    s->queue_count++;
    session_touch(s);
    persistence_save();
    return 0;
}

queued_message_t *session_queue_peek(session_t *s) { return s->queue_head; }

void session_queue_pop(session_t *s) {
    queued_message_t *q = s->queue_head;
    if (!q)
        return;

    s->queue_head = q->next;
    if (!s->queue_head)
        s->queue_tail = NULL;
    s->queue_count--;

    free(q->topic);
    free(q->payload);
    free(q);

    session_touch(s);
    persistence_save();
}

void session_queue_clear(session_t *s) {
    queued_message_t *q = s->queue_head;
    while (q) {
        queued_message_t *next = q->next;
        free(q->topic);
        free(q->payload);
        free(q);
        q = next;
    }
    s->queue_head = NULL;
    s->queue_tail = NULL;
    s->queue_count = 0;
}
