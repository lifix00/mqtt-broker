#ifndef SUBSCRIPTION_H
#define SUBSCRIPTION_H

#include <stddef.h>
#include <stdbool.h>

#include "session.h"

#define MAX_FILTER_LEN 256

typedef struct subscription {
    char topic_filter[MAX_FILTER_LEN];
    session_t *session;
    uint8_t qos;
    struct subscription *next;
} subscription_t;

// 初始化链表，进程启动时调用一次
void subscription_init(void);

// 添加订阅（幂等）。filter合法性由调用方检查
// 返回 0：成功，-1 内存不足
int subscription_add(session_t *s, const char *filter, uint8_t qos);

// 移除某个会话的所有订阅（会话销毁时调用）
void subscription_remove_all(session_t *s);

// 移除某个会话的某个filter（UNSUBSCRIBE用）
// 返回 1：找到并移除，0表示没找到
int subscription_remove(session_t *s, const char *filter);

// 把一条消息投递给单个会话。
// 在线直接发送；离线且 clean_session=0 的会话在 QoS1 时入队。
// 返回 1 成功，0 丢弃/失败
int subscription_deliver_to_session(session_t *sess, const char *topic,
                                    size_t topic_len, const uint8_t *payload,
                                    size_t payload_len, uint8_t qos,
                                    uint8_t retain);

// 把一条消息分发给所有匹配的订阅者。
// 返回成功投递（含入队）的订阅者数量
int subscription_deliver(const char *topic, size_t topic_len,
                          const uint8_t *payload, size_t payload_len,
                          uint8_t qos, uint8_t retain);

// ==== 工具函数 ====

// 主题匹配
bool topic_matches(const char *filter, const char *topic);

// filter合法性检查（订阅的时候调用）
bool topic_filter_valid(const char *filter);
bool topic_filter_valid_len(const char *filter, size_t len);

// 获取订阅链表长度（调试用的）
int subscription_count(void);

// 遍历全部订阅（持久化用）
subscription_t *subscription_first(void);
subscription_t *subscription_next(subscription_t *s);

#endif
