#ifndef TOPIC_TRIE_H
#define TOPIC_TRIE_H

#include <stddef.h>

// trie 只负责树结构和遍历。
// 订阅者链表（subscribers / subscription.node / node_next）由 subscription.c 维护；
// 这里只保存链表头，不关心 subscription_t 的内部字段。
struct subscription;

typedef struct topic_node {
    char *segment; // 本层字符串，如 "sport"、"+"、"#"；root 为 NULL
    size_t segment_len;

    struct topic_node *parent;
    struct topic_node *children;  // 子节点链表头（遍历/释放用）
    struct topic_node *sibling;   // 同级下一个节点
    struct topic_node *hash_next; // 全局边哈希桶内链

    struct topic_node *plus;       // 缓存的 '+' 子节点
    struct topic_node *hash_child; // 缓存的 '#' 子节点

    struct subscription *subscribers; // 订阅了“root 到本节点”这条路径的会话
} topic_node_t;

// 匹配到节点时，把该节点的订阅者链表交给 callback。
// 返回本次成功投递的数量，供上层累加。
typedef int (*topic_match_cb_t)(struct subscription *subs, void *ctx);

void topic_trie_init(void);

// 只查找 filter 对应路径，不创建节点
topic_node_t *topic_trie_find(const char *filter);

// 查找路径，缺失节点补建；失败返回 NULL（会回滚本次新建的节点）
topic_node_t *topic_trie_find_or_create(const char *filter);

// 节点没有订阅者也没有子节点时，向上回收空路径
void topic_trie_prune(topic_node_t *node);

// 沿 topic 层级匹配，命中节点时调用 cb(subs, ctx)
int topic_trie_match(const char *topic, size_t topic_len,
                     topic_match_cb_t cb, void *ctx);

#endif
