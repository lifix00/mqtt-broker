#include "persistence.h"
#include "retained.h"
#include "session.h"
#include "subscription.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define STATE_MAGIC     "MQTTSN1\n"
#define STATE_VERSION   1u
#define MAX_BLOB_LEN    (16u * 1024u * 1024u)
#define MAX_RECORDS     1000000u
#define MAX_PATH_LEN    4096

static char g_path[MAX_PATH_LEN];
static int g_enabled = 0;
static int g_loading = 0;

// ==== 基础读写 ====

static int write_u8(FILE *f, uint8_t v) {
    return fputc((int)v, f) == EOF ? -1 : 0;
}

static int write_u16(FILE *f, uint16_t v) {
    uint8_t b[2] = {(uint8_t)(v >> 8), (uint8_t)(v & 0xff)};
    return fwrite(b, 1, sizeof(b), f) == sizeof(b) ? 0 : -1;
}

static int write_u32(FILE *f, uint32_t v) {
    uint8_t b[4] = {(uint8_t)(v >> 24), (uint8_t)(v >> 16),
                    (uint8_t)(v >> 8), (uint8_t)(v & 0xff)};
    return fwrite(b, 1, sizeof(b), f) == sizeof(b) ? 0 : -1;
}

static int write_bytes(FILE *f, const void *data, size_t len) {
    if (len == 0)
        return 0;
    return fwrite(data, 1, len, f) == len ? 0 : -1;
}

static int write_blob(FILE *f, const void *data, size_t len) {
    if (len > MAX_BLOB_LEN)
        return -1;
    if (write_u32(f, (uint32_t)len) < 0)
        return -1;
    return write_bytes(f, data, len);
}

static int read_u8(FILE *f, uint8_t *v) {
    int c = fgetc(f);
    if (c == EOF)
        return -1;
    *v = (uint8_t)c;
    return 0;
}

static int read_u16(FILE *f, uint16_t *v) {
    uint8_t b[2];
    if (fread(b, 1, sizeof(b), f) != sizeof(b))
        return -1;
    *v = (uint16_t)((b[0] << 8) | b[1]);
    return 0;
}

static int read_u32(FILE *f, uint32_t *v) {
    uint8_t b[4];
    if (fread(b, 1, sizeof(b), f) != sizeof(b))
        return -1;
    *v = ((uint32_t)b[0] << 24) | ((uint32_t)b[1] << 16) |
         ((uint32_t)b[2] << 8) | (uint32_t)b[3];
    return 0;
}

static int read_blob(FILE *f, uint8_t **out, size_t *out_len) {
    uint32_t len;
    if (read_u32(f, &len) < 0 || len > MAX_BLOB_LEN)
        return -1;

    uint8_t *p = malloc(len ? len : 1);
    if (!p)
        return -1;
    if (len && fread(p, 1, len, f) != len) {
        free(p);
        return -1;
    }
    *out = p;
    *out_len = len;
    return 0;
}

static int read_string(FILE *f, char *buf, size_t cap) {
    uint8_t *p = NULL;
    size_t len = 0;
    if (read_blob(f, &p, &len) < 0)
        return -1;
    if (len >= cap || memchr(p, '\0', len)) {
        free(p);
        return -1;
    }
    memcpy(buf, p, len);
    buf[len] = '\0';
    free(p);
    return 0;
}

// ==== 统计 ====

static uint32_t count_persistent_sessions(void) {
    uint32_t n = 0;
    for (session_t *s = session_first(); s; s = session_next(s)) {
        if (!s->clean_session)
            n++;
    }
    return n;
}

static uint32_t count_session_subscriptions(session_t *s) {
    uint32_t n = 0;
    for (subscription_t *sub = subscription_first(); sub;
         sub = subscription_next(sub)) {
        if (sub->session == s)
            n++;
    }
    return n;
}

// ==== 保存 ====

static int save_retained(FILE *f) {
    if (write_u32(f, (uint32_t)retained_count()) < 0)
        return -1;
    for (retained_message_t *m = retained_first(); m; m = retained_next(m)) {
        if (write_blob(f, m->topic, m->topic_len) < 0 ||
            write_blob(f, m->payload, m->payload_len) < 0 ||
            write_u8(f, m->qos) < 0)
            return -1;
    }
    return 0;
}

static int save_sessions(FILE *f) {
    if (write_u32(f, count_persistent_sessions()) < 0)
        return -1;

    for (session_t *s = session_first(); s; s = session_next(s)) {
        if (s->clean_session)
            continue;

        size_t cid_len = strlen(s->client_id);
        if (write_blob(f, s->client_id, cid_len) < 0 ||
            write_u16(f, s->keep_alive) < 0 ||
            write_u16(f, s->next_packet_id) < 0)
            return -1;

        if (write_u32(f, count_session_subscriptions(s)) < 0)
            return -1;
        for (subscription_t *sub = subscription_first(); sub;
             sub = subscription_next(sub)) {
            if (sub->session != s)
                continue;
            if (write_blob(f, sub->topic_filter, strlen(sub->topic_filter)) <
                    0 ||
                write_u8(f, sub->qos) < 0)
                return -1;
        }

        if (write_u32(f, (uint32_t)s->inflight_count) < 0)
            return -1;
        for (int i = 0; i < MAX_INFLIGHT; i++) {
            inflight_entry_t *e = &s->inflight[i];
            if (!e->in_use)
                continue;
            if (write_u16(f, e->packet_id) < 0 || write_u8(f, e->qos) < 0 ||
                write_u8(f, e->state) < 0 || write_u8(f, e->retain) < 0 ||
                write_blob(f, e->topic, e->topic_len) < 0 ||
                write_blob(f, e->payload, e->payload_len) < 0)
                return -1;
        }

        if (write_u32(f, (uint32_t)s->queue_count) < 0)
            return -1;
        for (queued_message_t *q = s->queue_head; q; q = q->next) {
            if (write_blob(f, q->topic, q->topic_len) < 0 ||
                write_blob(f, q->payload, q->payload_len) < 0 ||
                write_u8(f, q->qos) < 0 || write_u8(f, q->retain) < 0)
                return -1;
        }

        if (write_u32(f, (uint32_t)s->incoming_qos2_count) < 0)
            return -1;
        for (int i = 0; i < MAX_INCOMING_QOS2; i++) {
            incoming_qos2_entry_t *in = &s->incoming_qos2[i];
            if (!in->in_use)
                continue;
            if (write_u16(f, in->packet_id) < 0 ||
                write_u8(f, in->retain) < 0 ||
                write_blob(f, in->topic, in->topic_len) < 0 ||
                write_blob(f, in->payload, in->payload_len) < 0)
                return -1;
        }
    }
    return 0;
}

int persistence_is_loading(void) { return g_loading; }

void persistence_save(void) {
    if (!g_enabled || g_loading)
        return;

    char tmp[MAX_PATH_LEN];
    int n = snprintf(tmp, sizeof(tmp), "%s.tmp", g_path);
    if (n < 0 || (size_t)n >= sizeof(tmp)) {
        fprintf(stderr, "state path too long\n");
        return;
    }

    FILE *f = fopen(tmp, "wb");
    if (!f) {
        perror("open state tmp");
        return;
    }

    int ok = 1;
    if (write_bytes(f, STATE_MAGIC, 8) < 0 ||
        write_u32(f, STATE_VERSION) < 0 || save_retained(f) < 0 ||
        save_sessions(f) < 0)
        ok = 0;

    if (ok && (fflush(f) != 0 || fsync(fileno(f)) != 0))
        ok = 0;
    if (fclose(f) != 0)
        ok = 0;

    if (ok) {
        if (rename(tmp, g_path) < 0) {
            perror("rename state");
            unlink(tmp);
        }
    } else {
        fprintf(stderr, "failed to write state file '%s'\n", g_path);
        unlink(tmp);
    }
}

// ==== 加载 ====

static int load_retained(FILE *f) {
    uint32_t count;
    if (read_u32(f, &count) < 0 || count > MAX_RECORDS)
        return -1;

    for (uint32_t i = 0; i < count; i++) {
        uint8_t *topic = NULL;
        uint8_t *payload = NULL;
        size_t topic_len = 0;
        size_t payload_len = 0;
        uint8_t qos;

        if (read_blob(f, &topic, &topic_len) < 0 ||
            read_blob(f, &payload, &payload_len) < 0 || read_u8(f, &qos) < 0 ||
            qos > 2) {
            free(topic);
            free(payload);
            return -1;
        }

        int rc = retained_store((const char *)topic, topic_len, payload,
                                payload_len, qos);
        free(topic);
        free(payload);
        if (rc < 0)
            return -1;
    }
    return 0;
}

static int load_sessions(FILE *f) {
    uint32_t count;
    if (read_u32(f, &count) < 0 || count > MAX_RECORDS)
        return -1;

    for (uint32_t i = 0; i < count; i++) {
        char client_id[MAX_CLIENT_ID_LEN];
        uint16_t keep_alive;
        uint16_t next_packet_id;

        if (read_string(f, client_id, sizeof(client_id)) < 0 ||
            read_u16(f, &keep_alive) < 0 ||
            read_u16(f, &next_packet_id) < 0)
            return -1;

        session_t *s = session_create(client_id, 0, keep_alive);
        if (!s)
            return -1;
        s->next_packet_id = next_packet_id ? next_packet_id : 1;

        uint32_t sub_count;
        if (read_u32(f, &sub_count) < 0 || sub_count > MAX_RECORDS)
            return -1;
        for (uint32_t j = 0; j < sub_count; j++) {
            char filter[MAX_FILTER_LEN];
            uint8_t qos;
            if (read_string(f, filter, sizeof(filter)) < 0 ||
                read_u8(f, &qos) < 0 || qos > 2)
                return -1;
            if (subscription_add(s, filter, qos) < 0)
                return -1;
        }

        uint32_t inflight_count;
        if (read_u32(f, &inflight_count) < 0 ||
            inflight_count > MAX_INFLIGHT)
            return -1;
        for (uint32_t j = 0; j < inflight_count; j++) {
            uint16_t packet_id;
            uint8_t qos, state, retain;
            uint8_t *topic = NULL;
            uint8_t *payload = NULL;
            size_t topic_len = 0;
            size_t payload_len = 0;

            if (read_u16(f, &packet_id) < 0 || read_u8(f, &qos) < 0 ||
                read_u8(f, &state) < 0 || read_u8(f, &retain) < 0 ||
                read_blob(f, &topic, &topic_len) < 0 ||
                read_blob(f, &payload, &payload_len) < 0) {
                free(topic);
                free(payload);
                return -1;
            }

            int state_ok = (qos == 1 && state == INFLIGHT_WAIT_PUBACK) ||
                           (qos == 2 && (state == INFLIGHT_WAIT_PUBREC ||
                                         state == INFLIGHT_WAIT_PUBCOMP));
            if (packet_id == 0 || !state_ok) {
                free(topic);
                free(payload);
                return -1;
            }

            inflight_entry_t *e =
                session_inflight_add(s, packet_id, qos, retain,
                                     (const char *)topic, topic_len, payload,
                                     payload_len);
            free(topic);
            free(payload);
            if (!e)
                return -1;
            e->state = state;
        }

        uint32_t queue_count;
        if (read_u32(f, &queue_count) < 0 || queue_count > MAX_QUEUED_MESSAGES)
            return -1;
        for (uint32_t j = 0; j < queue_count; j++) {
            uint8_t *topic = NULL;
            uint8_t *payload = NULL;
            size_t topic_len = 0;
            size_t payload_len = 0;
            uint8_t qos, retain;

            if (read_blob(f, &topic, &topic_len) < 0 ||
                read_blob(f, &payload, &payload_len) < 0 ||
                read_u8(f, &qos) < 0 || read_u8(f, &retain) < 0 ||
                qos < 1 || qos > 2) {
                free(topic);
                free(payload);
                return -1;
            }

            int rc = session_queue_push(s, (const char *)topic, topic_len,
                                        payload, payload_len, qos, retain);
            free(topic);
            free(payload);
            if (rc < 0)
                return -1;
        }

        uint32_t incoming_count;
        if (read_u32(f, &incoming_count) < 0 ||
            incoming_count > MAX_INCOMING_QOS2)
            return -1;
        for (uint32_t j = 0; j < incoming_count; j++) {
            uint16_t packet_id;
            uint8_t retain;
            uint8_t *topic = NULL;
            uint8_t *payload = NULL;
            size_t topic_len = 0;
            size_t payload_len = 0;

            if (read_u16(f, &packet_id) < 0 || read_u8(f, &retain) < 0 ||
                read_blob(f, &topic, &topic_len) < 0 ||
                read_blob(f, &payload, &payload_len) < 0) {
                free(topic);
                free(payload);
                return -1;
            }

            incoming_qos2_entry_t *in = session_incoming_qos2_add(
                s, packet_id, retain, (const char *)topic, topic_len, payload,
                payload_len);
            free(topic);
            free(payload);
            if (!in || packet_id == 0)
                return -1;
        }
    }
    return 0;
}

static int load_state(FILE *f) {
    char magic[8];
    uint32_t version;
    if (fread(magic, 1, sizeof(magic), f) != sizeof(magic) ||
        memcmp(magic, STATE_MAGIC, sizeof(magic)) != 0)
        return -1;
    if (read_u32(f, &version) < 0 || version != STATE_VERSION)
        return -1;
    if (load_retained(f) < 0)
        return -1;
    if (load_sessions(f) < 0)
        return -1;
    return 0;
}

void persistence_init(const char *path) {
    g_enabled = 0;
    g_loading = 0;
    g_path[0] = '\0';

    if (!path || !*path)
        return;
    if (strlen(path) >= sizeof(g_path) - 5) {
        fprintf(stderr, "state path too long, persistence disabled\n");
        return;
    }
    snprintf(g_path, sizeof(g_path), "%s", path);
    g_enabled = 1;
    g_loading = 1;

    FILE *f = fopen(g_path, "rb");
    if (!f) {
        g_loading = 0;
        if (errno != ENOENT)
            perror("open state");
        return;
    }

    int rc = load_state(f);
    fclose(f);

    if (rc < 0) {
        fprintf(stderr, "state file '%s' is corrupt, starting empty\n",
                g_path);
        session_clear_all();
        retained_clear_all();
        g_loading = 0;
        persistence_save(); // 写回一个合法的空状态
    } else {
        g_loading = 0;
    }
}
