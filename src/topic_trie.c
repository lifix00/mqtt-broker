#include "topic_trie.h"

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

// 每个节点代表主题的一层，root 的 segment 为 NULL。
// children 用链表组织（省内存、方便遍历/释放），同时维护一个全局
// (parent, segment) -> node 的哈希索引，使“找某一层的精确子节点”是 O(1)，
// 不会再因为某一层子节点很多而退化成线性扫描。
// '+' 和 '#' 在父节点上单独缓存，匹配时不需要查哈希。

#define EDGE_BUCKET_COUNT 16384u

static topic_node_t g_root;
static topic_node_t *g_edges[EDGE_BUCKET_COUNT];

// ==== 边的哈希索引 ====

static uint64_t edge_hash(const topic_node_t *parent, const char *segment,
                          size_t len) {
    // FNV-1a，先混入父节点地址，再混入本层字符串
    uint64_t h = 1469598103934665603ULL;
    uintptr_t p = (uintptr_t)parent;
    for (size_t i = 0; i < sizeof(p); i++) {
        h ^= (uint8_t)(p >> (8 * i));
        h *= 1099511628211ULL;
    }
    for (size_t i = 0; i < len; i++) {
        h ^= (unsigned char)segment[i];
        h *= 1099511628211ULL;
    }
    return h;
}

static size_t edge_bucket(const topic_node_t *parent, const char *segment,
                          size_t len) {
    return (size_t)(edge_hash(parent, segment, len) % EDGE_BUCKET_COUNT);
}

static topic_node_t *edge_find(topic_node_t *parent, const char *segment,
                               size_t len) {
    size_t b = edge_bucket(parent, segment, len);
    for (topic_node_t *n = g_edges[b]; n; n = n->hash_next) {
        if (n->parent == parent && n->segment_len == len &&
            (len == 0 || memcmp(n->segment, segment, len) == 0))
            return n;
    }
    return NULL;
}

static void edge_add(topic_node_t *n) {
    size_t b = edge_bucket(n->parent, n->segment, n->segment_len);
    n->hash_next = g_edges[b];
    g_edges[b] = n;
}

static void edge_remove(topic_node_t *n) {
    size_t b = edge_bucket(n->parent, n->segment, n->segment_len);
    topic_node_t **pp = &g_edges[b];
    while (*pp) {
        if (*pp == n) {
            *pp = n->hash_next;
            return;
        }
        pp = &(*pp)->hash_next;
    }
}

// ==== 节点生命周期 ====

static topic_node_t *node_create(topic_node_t *parent, const char *segment,
                                 size_t len) {
    topic_node_t *n = calloc(1, sizeof(*n));
    if (!n)
        return NULL;

    n->segment = malloc(len + 1);
    if (!n->segment) {
        free(n);
        return NULL;
    }
    if (len)
        memcpy(n->segment, segment, len);
    n->segment[len] = '\0';
    n->segment_len = len;
    n->parent = parent;

    n->sibling = parent->children;
    parent->children = n;
    if (len == 1 && segment[0] == '+')
        parent->plus = n;
    else if (len == 1 && segment[0] == '#')
        parent->hash_child = n;

    edge_add(n);
    return n;
}

static void node_unlink_from_parent(topic_node_t *n) {
    topic_node_t *parent = n->parent;
    if (!parent)
        return;

    topic_node_t **pp = &parent->children;
    while (*pp && *pp != n)
        pp = &(*pp)->sibling;
    if (*pp)
        *pp = n->sibling;

    if (parent->plus == n)
        parent->plus = NULL;
    if (parent->hash_child == n)
        parent->hash_child = NULL;
}

// 释放一条“刚创建、还未挂订阅者”的直线链
static void node_free_chain(topic_node_t *n) {
    while (n) {
        topic_node_t *next = n->children;
        free(n->segment);
        free(n);
        n = next;
    }
}

static void rollback_new_chain(topic_node_t *first) {
    // 先把所有新节点从哈希索引摘掉，再统一释放
    for (topic_node_t *n = first; n; n = n->children)
        edge_remove(n);
    node_unlink_from_parent(first);
    node_free_chain(first);
}

static void node_free_recursive(topic_node_t *n) {
    while (n) {
        topic_node_t *sibling = n->sibling;
        node_free_recursive(n->children);
        free(n->segment);
        free(n);
        n = sibling;
    }
}

void topic_trie_init(void) {
    node_free_recursive(g_root.children);
    memset(&g_root, 0, sizeof(g_root));
    memset(g_edges, 0, sizeof(g_edges));
}

void topic_trie_prune(topic_node_t *node) {
    while (node && node != &g_root && !node->children && !node->subscribers) {
        topic_node_t *parent = node->parent;
        edge_remove(node);
        node_unlink_from_parent(node);
        free(node->segment);
        free(node);
        node = parent;
    }
}

// ==== 路径查找/创建 ====

topic_node_t *topic_trie_find(const char *filter) {
    if (!filter)
        return NULL;

    topic_node_t *cur = &g_root;
    const char *p = filter;

    for (;;) {
        const char *slash = strchr(p, '/');
        size_t len = slash ? (size_t)(slash - p) : strlen(p);

        topic_node_t *child = edge_find(cur, p, len);
        if (!child)
            return NULL;
        cur = child;

        if (!slash)
            return cur;
        p = slash + 1;
    }
}

// 找到路径，缺失的节点补建；失败时回滚本次新建的整条链
topic_node_t *topic_trie_find_or_create(const char *filter) {
    if (!filter)
        return NULL;

    topic_node_t *cur = &g_root;
    topic_node_t *first_new = NULL;
    const char *p = filter;

    for (;;) {
        const char *slash = strchr(p, '/');
        size_t len = slash ? (size_t)(slash - p) : strlen(p);

        topic_node_t *child = edge_find(cur, p, len);
        if (!child) {
            child = node_create(cur, p, len);
            if (!child) {
                if (first_new)
                    rollback_new_chain(first_new);
                return NULL;
            }
            if (!first_new)
                first_new = child;
        }
        cur = child;

        if (!slash)
            break;
        p = slash + 1;
    }

    return cur;
}

// ==== 匹配 ====

// 沿 trie 下行匹配一个主题层级。
// cursor/remaining 只描述“还没匹配完的 topic 后缀”；
// has_level 区分“已经没有下一层”和“下一层是空字符串”，
// 这样 "sport/" 和 "sport" 都能被正确处理。
static int trie_match_deliver(topic_node_t *node, const char *cursor,
                              size_t remaining, int has_level,
                              topic_match_cb_t cb, void *ctx) {
    int delivered = 0;

    if (!has_level) {
        // 路径在这里结束：精确 filter，以及 "xxx/#" 匹配零层的情况
        delivered += cb(node->subscribers, ctx);
        if (node->hash_child)
            delivered += cb(node->hash_child->subscribers, ctx);
        return delivered;
    }

    // "#" 匹配当前层及其后所有层
    if (node->hash_child)
        delivered += cb(node->hash_child->subscribers, ctx);

    const char *slash = memchr(cursor, '/', remaining);
    size_t level_len = slash ? (size_t)(slash - cursor) : remaining;
    int next_has_level = slash != NULL;
    const char *next_cursor = next_has_level ? slash + 1 : NULL;
    size_t next_remaining = next_has_level ? remaining - level_len - 1 : 0;

    if (node->plus) {
        delivered += trie_match_deliver(node->plus, next_cursor, next_remaining,
                                        next_has_level, cb, ctx);
    }

    topic_node_t *exact = edge_find(node, cursor, level_len);
    // 发布 topic 里出现字面量 '+'/'#' 时，edge_find 可能返回通配符节点；
    // 它已经在上面处理过，避免重复投递。
    if (exact && exact != node->plus && exact != node->hash_child) {
        delivered += trie_match_deliver(exact, next_cursor, next_remaining,
                                        next_has_level, cb, ctx);
    }

    return delivered;
}

int topic_trie_match(const char *topic, size_t topic_len,
                     topic_match_cb_t cb, void *ctx) {
    if (!topic || !cb)
        return 0;
    return trie_match_deliver(&g_root, topic, topic_len, 1, cb, ctx);
}
