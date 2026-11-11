#ifndef SESSION_H
#define SESSION_H

#include <stddef.h>
#include <stdint.h>

#define MAX_INFLIGHT         1024
#define MAX_QUEUED_MESSAGES  1024
#define MAX_CLIENT_ID_LEN    128

struct connection;
typedef struct connection connection_t;

typedef enum {
    SESSION_STATE_CONNECTING = 0,
    SESSION_STATE_CONNECTED,
} session_state_t;

// 一条已发送、等待 PUBACK 的 QoS1 消息
typedef struct inflight_entry {
    uint16_t packet_id;
    uint8_t  in_use;
    uint8_t  qos;
    uint8_t  retain;
    uint64_t sent_at_ms; // 上次发送时间，用于超时重传

    // 保存消息副本，PUBACK 丢失或重连时才能重传
    char    *topic;
    size_t   topic_len;
    uint8_t *payload;
    size_t   payload_len;
} inflight_entry_t;

// 离线会话排队等待发送的 QoS1 消息
typedef struct queued_message {
    char    *topic;
    size_t   topic_len;
    uint8_t *payload;
    size_t   payload_len;
    uint8_t  qos;
    uint8_t  retain;
    struct queued_message *next;
} queued_message_t;

// 会话：clean_session=0 时在连接断开后继续存在。
// 订阅、inflight、离线队列都归属于会话，而不是某一次连接。
typedef struct session {
    char     client_id[MAX_CLIENT_ID_LEN];
    uint8_t  clean_session;
    uint16_t keep_alive;

    connection_t *conn; // NULL 表示当前离线

    uint16_t next_packet_id;
    inflight_entry_t inflight[MAX_INFLIGHT];
    int inflight_count;

    queued_message_t *queue_head;
    queued_message_t *queue_tail;
    int queue_count;

    struct session *hash_next; // 会话表桶内链
} session_t;

// ==== 会话表 ====
void session_store_init(void);
session_t *session_find(const char *client_id);
session_t *session_create(const char *client_id, uint8_t clean_session,
                          uint16_t keep_alive);
void session_destroy(session_t *s);

void session_attach(session_t *s, connection_t *c);
void session_detach(session_t *s);

session_t *session_first(void);
session_t *session_next(session_t *s);

// ==== 出站 QoS1：packet id 与 inflight ====
uint16_t session_alloc_packet_id(session_t *s);
inflight_entry_t *session_inflight_find(session_t *s, uint16_t packet_id);
inflight_entry_t *session_inflight_add(session_t *s, uint16_t packet_id,
                                       uint8_t qos, uint8_t retain,
                                       const char *topic, size_t topic_len,
                                       const uint8_t *payload,
                                       size_t payload_len);
int session_inflight_remove(session_t *s, uint16_t packet_id);
void session_inflight_clear(session_t *s);

// ==== 离线队列 ====
int session_queue_push(session_t *s, const char *topic, size_t topic_len,
                       const uint8_t *payload, size_t payload_len, uint8_t qos,
                       uint8_t retain);
queued_message_t *session_queue_peek(session_t *s);
void session_queue_pop(session_t *s);
void session_queue_clear(session_t *s);

#endif
