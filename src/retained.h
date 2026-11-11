#ifndef RETAINED_H
#define RETAINED_H

#include <stddef.h>
#include <stdint.h>

#include "session.h"

typedef struct retained_message {
    char    *topic;
    size_t   topic_len;
    uint8_t *payload;
    size_t   payload_len;
    uint8_t  qos;
    uint64_t last_updated_seq;
    struct retained_message *next;
} retained_message_t;

// 初始化 retained 消息表
void retained_init(void);

// 保存/替换一条 retained 消息；payload_len==0 表示删除该 topic 的 retained 消息。
// 返回 0 成功，-1 失败
int retained_store(const char *topic, size_t topic_len, const uint8_t *payload,
                   size_t payload_len, uint8_t qos);

// 把匹配 filter 的 retained 消息投递给指定会话。
// 投递时 RETAIN=1，QoS = min(retained_qos, granted_qos)。
// 返回成功投递的条数
int retained_deliver(session_t *sess, const char *filter, uint8_t granted_qos);

// 遍历 retained 表（持久化用）
retained_message_t *retained_first(void);
retained_message_t *retained_next(retained_message_t *m);
int retained_count(void);

// 清空 retained 表（加载失败时回滚）
void retained_clear_all(void);

#endif
