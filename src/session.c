#include "session.h"
#include "connection.h"
#include "subscription.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define SESSION_BUCKET_COUNT 64

static session_t *g_buckets[SESSION_BUCKET_COUNT];

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

session_t *session_create(const char *client_id, uint8_t clean_session,
                          uint16_t keep_alive) {
    if (!client_id || strlen(client_id) >= MAX_CLIENT_ID_LEN)
        return NULL;

    session_t *s = calloc(1, sizeof(*s));
    if (!s)
        return NULL;

    snprintf(s->client_id, sizeof(s->client_id), "%s", client_id);
    s->clean_session = clean_session;
    s->keep_alive = keep_alive;
    s->next_packet_id = 1;

    unsigned b = session_hash(s->client_id);
    s->hash_next = g_buckets[b];
    g_buckets[b] = s;
    return s;
}

void session_attach(session_t *s, connection_t *c) {
    s->conn = c;
    c->session = s;
    c->state = SESSION_STATE_CONNECTED;
}

void session_detach(session_t *s) {
    if (!s || !s->conn)
        return;
    connection_t *c = s->conn;
    s->conn = NULL;
    c->session = NULL;
    c->state = SESSION_STATE_CONNECTING;
}

void session_destroy(session_t *s) {
    if (!s)
        return;

    session_detach(s);
    subscription_remove_all(s);
    session_inflight_clear(s);
    session_queue_clear(s);

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
    slot->retain = retain;
    slot->sent_at_ms = connection_now_ms();
    slot->topic = topic_copy;
    slot->topic_len = topic_len;
    slot->payload = payload_copy;
    slot->payload_len = payload_len;
    s->inflight_count++;
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
}

void session_queue_clear(session_t *s) {
    while (s->queue_head)
        session_queue_pop(s);
}
