#ifndef SUBSCRIPTION_H
#define SUBSCRIPTION_H

#include <stddef.h>
#include <stdbool.h>

#include "session.h"

#define MAX_FILTER_LEN 256

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

// 把一条消息分发给所有匹配的订阅者。
// 在线会话直接投递；离线且 clean_session=0 的会话在 QoS1 时入队。
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

#endif
