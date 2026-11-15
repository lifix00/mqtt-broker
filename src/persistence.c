#include "persistence.h"
#include "retained.h"
#include "session.h"
#include "subscription.h"

#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define STATE_MAGIC     "MQTTSN1\n"
#define STATE_VERSION   2u
#define MAX_BLOB_LEN    (16u * 1024u * 1024u)
#define MAX_RECORDS     1000000u
#define MAX_PATH_LEN    4096
// 合并窗口：短时间内的多次状态修改只生成一份快照。
#define PERSISTENCE_COALESCE_MS 50u

static char g_path[MAX_PATH_LEN];
static int g_enabled = 0;
static int g_loading = 0;

// 主线程只设置 dirty；后台线程只消费已经序列化好的内存快照。
static atomic_int g_dirty = 0;
static uint64_t g_last_snapshot_ms = 0;

static pthread_t g_writer_thread;
static pthread_mutex_t g_writer_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_writer_cond = PTHREAD_COND_INITIALIZER;
static uint8_t *g_pending_buf = NULL;
static size_t g_pending_len = 0;
static int g_writer_started = 0;
static int g_writer_stop = 0;

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

static int write_u64(FILE *f, uint64_t v) {
    uint8_t b[8];
    for (int i = 0; i < 8; i++)
        b[i] = (uint8_t)(v >> (56 - 8 * i));
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

static int read_u64(FILE *f, uint64_t *v) {
    uint8_t b[8];
    if (fread(b, 1, sizeof(b), f) != sizeof(b))
        return -1;
    uint64_t value = 0;
    for (int i = 0; i < 8; i++)
        value = (value << 8) | b[i];
    *v = value;
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
    for (subscription_t *sub = subscription_session_first(s); sub;
         sub = subscription_session_next(sub)) {
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
            write_u16(f, s->next_packet_id) < 0 ||
            write_u64(f, s->offline_since_ms) < 0)
            return -1;

        if (write_u32(f, count_session_subscriptions(s)) < 0)
            return -1;
        for (subscription_t *sub = subscription_session_first(s); sub;
             sub = subscription_session_next(sub)) {
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

// 把当前状态序列化到一块连续内存。这个函数只在主线程调用，
// 因此读取 session/trie 时不需要额外加锁；后台线程只接触这块
// 已经构建完成的只读快照。
static int build_snapshot(uint8_t **out, size_t *out_len) {
    char *buf = NULL;
    size_t len = 0;
    FILE *f = open_memstream(&buf, &len);
    if (!f)
        return -1;

    int ok = 1;
    if (write_bytes(f, STATE_MAGIC, 8) < 0 ||
        write_u32(f, STATE_VERSION) < 0 || save_retained(f) < 0 ||
        save_sessions(f) < 0)
        ok = 0;
    if (fclose(f) != 0)
        ok = 0;

    if (!ok) {
        free(buf);
        return -1;
    }
    *out = (uint8_t *)buf;
    *out_len = len;
    return 0;
}

// 原子写入一块已经序列化好的状态。只做磁盘 I/O，不读取全局状态，
// 因此可以被后台线程安全调用。
static int write_snapshot_file(const uint8_t *data, size_t len) {
    char tmp[MAX_PATH_LEN];
    int n = snprintf(tmp, sizeof(tmp), "%s.tmp", g_path);
    if (n < 0 || (size_t)n >= sizeof(tmp)) {
        fprintf(stderr, "state path too long\n");
        return -1;
    }

    FILE *f = fopen(tmp, "wb");
    if (!f) {
        perror("open state tmp");
        return -1;
    }

    int ok = fwrite(data, 1, len, f) == len;
    if (ok && fflush(f) != 0)
        ok = 0;
    if (ok && fsync(fileno(f)) != 0)
        ok = 0;
    if (fclose(f) != 0)
        ok = 0;

    if (!ok) {
        fprintf(stderr, "failed to write state file '%s'\n", g_path);
        unlink(tmp);
        return -1;
    }
    if (rename(tmp, g_path) < 0) {
        perror("rename state");
        unlink(tmp);
        return -1;
    }
    return 0;
}

static void persistence_write_now(void) {
    uint8_t *buf = NULL;
    size_t len = 0;
    if (build_snapshot(&buf, &len) < 0) {
        fprintf(stderr, "failed to serialize state\n");
        return;
    }
    write_snapshot_file(buf, len);
    free(buf);
}

static void *persistence_writer_main(void *arg) {
    (void)arg;

    for (;;) {
        pthread_mutex_lock(&g_writer_lock);
        while (!g_writer_stop && !g_pending_buf)
            pthread_cond_wait(&g_writer_cond, &g_writer_lock);
        if (g_writer_stop) {
            pthread_mutex_unlock(&g_writer_lock);
            break;
        }

        uint8_t *buf = g_pending_buf;
        size_t len = g_pending_len;
        g_pending_buf = NULL;
        g_pending_len = 0;
        pthread_mutex_unlock(&g_writer_lock);

        if (write_snapshot_file(buf, len) < 0) {
            // 写失败时保持 dirty，下一次 tick 会重新生成快照。
            atomic_store_explicit(&g_dirty, 1, memory_order_relaxed);
        }
        free(buf);
    }
    return NULL;
}

// 标记状态已变化。真正的序列化和磁盘写入由主循环的
// persistence_tick() 与后台 writer 合并完成。
void persistence_save(void) {
    if (!g_enabled || g_loading)
        return;
    atomic_store_explicit(&g_dirty, 1, memory_order_relaxed);
}

int persistence_is_dirty(void) {
    return g_enabled && !g_loading &&
           atomic_load_explicit(&g_dirty, memory_order_relaxed);
}

// 主循环定期调用：合并短时间内的多次修改，生成一份快照并交给
// 后台线程。序列化仍在主线程完成，保证读到的是一致状态；fsync
// 和文件写入不在主线程执行。
void persistence_tick(uint64_t now_ms) {
    if (!g_enabled || g_loading)
        return;
    if (!atomic_load_explicit(&g_dirty, memory_order_relaxed))
        return;
    if (g_last_snapshot_ms != 0 &&
        now_ms - g_last_snapshot_ms < PERSISTENCE_COALESCE_MS)
        return;

    uint8_t *buf = NULL;
    size_t len = 0;
    if (build_snapshot(&buf, &len) < 0) {
        fprintf(stderr, "failed to serialize state\n");
        return;
    }
    g_last_snapshot_ms = now_ms;
    atomic_store_explicit(&g_dirty, 0, memory_order_relaxed);

    if (!g_writer_started) {
        // pthread_create 失败时的降级路径：同步写，但至少保证正确性。
        if (write_snapshot_file(buf, len) < 0)
            atomic_store_explicit(&g_dirty, 1, memory_order_relaxed);
        free(buf);
        return;
    }

    pthread_mutex_lock(&g_writer_lock);
    if (g_writer_stop) {
        pthread_mutex_unlock(&g_writer_lock);
        atomic_store_explicit(&g_dirty, 1, memory_order_relaxed);
        free(buf);
        return;
    }
    // 只保留最新快照；后台线程正在写旧快照时，新快照覆盖 pending。
    free(g_pending_buf);
    g_pending_buf = buf;
    g_pending_len = len;
    pthread_cond_signal(&g_writer_cond);
    pthread_mutex_unlock(&g_writer_lock);
}

// 停止后台线程，并同步写入最终状态。只在 Broker 正常退出时调用。
void persistence_shutdown(void) {
    if (!g_enabled)
        return;

    if (g_writer_started) {
        pthread_mutex_lock(&g_writer_lock);
        g_writer_stop = 1;
        pthread_cond_broadcast(&g_writer_cond);
        pthread_mutex_unlock(&g_writer_lock);
        pthread_join(g_writer_thread, NULL);
        g_writer_started = 0;
    }

    pthread_mutex_lock(&g_writer_lock);
    free(g_pending_buf);
    g_pending_buf = NULL;
    g_pending_len = 0;
    pthread_mutex_unlock(&g_writer_lock);

    // 后台线程已经停止，主线程直接写最后一份完整状态。
    persistence_write_now();
    atomic_store_explicit(&g_dirty, 0, memory_order_relaxed);
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

static int load_sessions(FILE *f, uint32_t version) {
    uint32_t count;
    if (read_u32(f, &count) < 0 || count > MAX_RECORDS)
        return -1;

    for (uint32_t i = 0; i < count; i++) {
        char client_id[MAX_CLIENT_ID_LEN];
        uint16_t keep_alive;
        uint16_t next_packet_id;
        uint64_t offline_since_ms = 0;

        if (read_string(f, client_id, sizeof(client_id)) < 0 ||
            read_u16(f, &keep_alive) < 0 ||
            read_u16(f, &next_packet_id) < 0)
            return -1;
        if (version >= 2 && read_u64(f, &offline_since_ms) < 0)
            return -1;

        session_t *s = session_create(client_id, 0, keep_alive);
        if (!s)
            return -1;
        s->next_packet_id = next_packet_id ? next_packet_id : 1;
        // 旧格式没有这个字段；保存时在线（0）的会话重启后从当前时刻开始计时。
        s->offline_since_ms =
            offline_since_ms ? offline_since_ms : session_wall_now_ms();

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
    if (read_u32(f, &version) < 0 || (version != 1 && version != STATE_VERSION))
        return -1;
    if (load_retained(f) < 0)
        return -1;
    if (load_sessions(f, version) < 0)
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
    } else {
        int rc = load_state(f);
        fclose(f);

        if (rc < 0) {
            fprintf(stderr, "state file '%s' is corrupt, starting empty\n",
                    g_path);
            session_clear_all();
            retained_clear_all();
            g_loading = 0;
            // 启动后台线程前先写回一个合法的空状态。
            persistence_write_now();
        } else {
            g_loading = 0;
        }
    }

    g_writer_stop = 0;
    if (pthread_create(&g_writer_thread, NULL, persistence_writer_main, NULL) !=
        0) {
        // 创建失败时降级为主线程同步写，功能仍然正确。
        perror("pthread_create persistence writer");
        g_writer_started = 0;
    } else {
        g_writer_started = 1;
    }
}
