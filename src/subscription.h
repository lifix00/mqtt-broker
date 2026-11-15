#ifndef SUBSCRIPTION_H
#define SUBSCRIPTION_H

#include <stddef.h>
#include <stdbool.h>

#include "session.h"

#define MAX_FILTER_LEN 256

struct topic_node;

// 一条订阅 = (session, filter, qos)。
// 同时挂在三个链表上，避免为不同操作做全表扫描：
//   - next/prev：全局遍历（调试、统计）
//   - node_next：同一 trie 节点上的订阅者
//   - session_next/session_prev：同一会话的订阅（断连清理、持久化）
typedef struct subscription {
    char topic_filter[MAX_FILTER_LEN];
    session_t *session;
    uint8_t qos;
    struct topic_node *node; // 所属 trie 节点，删除时无需重新查找路径

    struct subscription *next;
    struct subscription *prev;
    struct subscription *node_next;
    struct subscription *session_next;
    struct subscription *session_prev;
} subscription_t;

// 初始化 trie，进程启动时调用一次
void subscription_init(void);

// 添加订阅（幂等）。filter合法性由调用方检查
// 返回 0：成功，-1 内存不足/超过上限
int subscription_add(session_t *s, const char *filter, uint8_t qos);

// 移除某个会话的所有订阅（会话销毁时调用）
void subscription_remove_all(session_t *s);

// 移除某个会话的某个filter（UNSUBSCRIBE用）
// 返回 1：找到并移除，0表示没找到
int subscription_remove(session_t *s, const char *filter);

// 把一条消息投递给单个会话。
// 在线直接发送；离线且 clean_session=0 的会话在 QoS1/QoS2 时入队。
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

// 主题匹配（保留给 retained 等仍使用 C 字符串的路径）
bool topic_matches(const char *filter, const char *topic);

// filter合法性检查（订阅的时候调用）
bool topic_filter_valid(const char *filter);
bool topic_filter_valid_len(const char *filter, size_t len);

// 获取订阅总数（调试/指标用）
int subscription_count(void);

// 遍历全部订阅（调试用）
subscription_t *subscription_first(void);
subscription_t *subscription_next(subscription_t *s);

// 按会话遍历订阅（持久化用）
subscription_t *subscription_session_first(session_t *s);
subscription_t *subscription_session_next(subscription_t *s);

#endif
