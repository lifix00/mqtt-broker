#include "acl.h"
#include "subscription.h"

#include <ctype.h>
#include <stdio.h>
#include <string.h>

#define ACL_MAX_USERS  64
#define ACL_MAX_RULES  512
#define ACL_MAX_NAME   128
#define ACL_READ       1u
#define ACL_WRITE      2u

typedef struct {
    char name[ACL_MAX_NAME];
} acl_user_t;

typedef struct {
    int user;
    unsigned actions;
    char filter[MAX_FILTER_LEN];
} acl_rule_t;

static int g_enabled;
static acl_user_t g_users[ACL_MAX_USERS];
static int g_user_count;
static acl_rule_t g_rules[ACL_MAX_RULES];
static int g_rule_count;

static void acl_reset(void) {
    g_enabled = 0;
    g_user_count = 0;
    g_rule_count = 0;
}

static char *trim(char *s) {
    while (*s && isspace((unsigned char)*s))
        s++;
    char *end = s + strlen(s);
    while (end > s && isspace((unsigned char)end[-1]))
        *--end = '\0';
    return s;
}

static int find_user(const char *name) {
    for (int i = 0; i < g_user_count; i++) {
        if (strcmp(g_users[i].name, name) == 0)
            return i;
    }
    return -1;
}

static int add_user(const char *name) {
    int idx = find_user(name);
    if (idx >= 0)
        return idx;
    if (g_user_count >= ACL_MAX_USERS)
        return -1;
    snprintf(g_users[g_user_count].name, sizeof(g_users[g_user_count].name),
             "%s", name);
    return g_user_count++;
}

static void acl_load_error(const char *path, int line, const char *msg) {
    fprintf(stderr, "ACL %s:%d: %s\n", path, line, msg);
}

int acl_init(const char *path) {
    acl_reset();
    if (!path || !*path)
        return 0;

    FILE *f = fopen(path, "r");
    if (!f) {
        perror("open ACL file");
        return -1;
    }

    char line[512];
    int current_user = -1;
    int lineno = 0;
    int ok = 1;

    while (ok && fgets(line, sizeof(line), f)) {
        lineno++;
        char *p = trim(line);
        if (!*p || *p == '#')
            continue;

        if (strncmp(p, "user", 4) == 0 &&
            isspace((unsigned char)p[4])) {
            char *name = trim(p + 4);
            if (!*name || strlen(name) >= ACL_MAX_NAME) {
                acl_load_error(path, lineno, "invalid user name");
                ok = 0;
                break;
            }
            current_user = add_user(name);
            if (current_user < 0) {
                acl_load_error(path, lineno, "too many users");
                ok = 0;
            }
            continue;
        }

        if (strncmp(p, "topic", 5) == 0 &&
            isspace((unsigned char)p[5])) {
            if (current_user < 0) {
                acl_load_error(path, lineno, "topic rule before user");
                ok = 0;
                break;
            }

            char *arg = trim(p + 5);
            unsigned actions = ACL_READ | ACL_WRITE;
            char *filter = arg;

            if (strncmp(arg, "readwrite", 9) == 0 &&
                (arg[9] == '\0' || isspace((unsigned char)arg[9]))) {
                if (arg[9] == '\0') {
                    acl_load_error(path, lineno, "missing topic filter");
                    ok = 0;
                    break;
                }
                actions = ACL_READ | ACL_WRITE;
                filter = trim(arg + 9);
            } else if (strncmp(arg, "read", 4) == 0 &&
                       (arg[4] == '\0' || isspace((unsigned char)arg[4]))) {
                if (arg[4] == '\0') {
                    acl_load_error(path, lineno, "missing topic filter");
                    ok = 0;
                    break;
                }
                actions = ACL_READ;
                filter = trim(arg + 4);
            } else if (strncmp(arg, "write", 5) == 0 &&
                       (arg[5] == '\0' || isspace((unsigned char)arg[5]))) {
                if (arg[5] == '\0') {
                    acl_load_error(path, lineno, "missing topic filter");
                    ok = 0;
                    break;
                }
                actions = ACL_WRITE;
                filter = trim(arg + 5);
            }

            if (!*filter || strlen(filter) >= MAX_FILTER_LEN ||
                !topic_filter_valid(filter)) {
                acl_load_error(path, lineno, "invalid topic filter");
                ok = 0;
                break;
            }
            if (g_rule_count >= ACL_MAX_RULES) {
                acl_load_error(path, lineno, "too many rules");
                ok = 0;
                break;
            }

            acl_rule_t *r = &g_rules[g_rule_count++];
            r->user = current_user;
            r->actions = actions;
            snprintf(r->filter, sizeof(r->filter), "%s", filter);
            continue;
        }

        acl_load_error(path, lineno, "unknown directive");
        ok = 0;
    }

    if (ok && ferror(f)) {
        perror("read ACL file");
        ok = 0;
    }
    fclose(f);

    if (!ok) {
        acl_reset();
        return -1;
    }
    g_enabled = 1;
    return 0;
}

void acl_cleanup(void) { acl_reset(); }

int acl_enabled(void) { return g_enabled; }

// 用它判断“一条 ACL filter 是否覆盖客户端请求的 filter”。
// 例如 rule=sensors/# 覆盖 requested=sensors/+/temp；
// rule=sensors/+ 不覆盖 requested=sensors/+/temp。
static int filter_covers(const char *rule, const char *requested) {
    const char *r = rule;
    const char *q = requested;

    for (;;) {
        if (*r == '#')
            return 1; // 规则 # 覆盖剩余所有层
        if (*r == '\0')
            return *q == '\0';
        if (*q == '\0')
            return 0;
        if (*q == '#')
            return 0; // 请求了 #，但规则不是 #

        const char *r_slash = strchr(r, '/');
        const char *q_slash = strchr(q, '/');
        size_t r_len = r_slash ? (size_t)(r_slash - r) : strlen(r);
        size_t q_len = q_slash ? (size_t)(q_slash - q) : strlen(q);

        if (r_len == 1 && *r == '+') {
            // 规则 + 覆盖任意单层（包括请求里的 +）
        } else if (q_len == 1 && *q == '+') {
            return 0; // 规则是字面量，请求是 +，范围更大
        } else if (r_len != q_len || memcmp(r, q, r_len) != 0) {
            return 0;
        }

        r = r_slash ? r_slash + 1 : r + r_len;
        q = q_slash ? q_slash + 1 : q + q_len;
    }
}

// 发布 topic 可能来自 MQTT 报文，不保证 NUL 结尾，所以按长度匹配。
static int topic_matches_filter(const char *filter, const char *topic,
                                size_t topic_len) {
    const char *f = filter;
    const char *t = topic;
    size_t remaining = topic_len;
    int has_level = 1;

    while (*f) {
        if (*f == '#')
            return f[1] == '\0';
        if (!has_level)
            return 0;

        const char *f_end = strchr(f, '/');
        size_t f_len = f_end ? (size_t)(f_end - f) : strlen(f);
        const char *t_end = memchr(t, '/', remaining);
        size_t t_len = t_end ? (size_t)(t_end - t) : remaining;

        if (!(f_len == 1 && *f == '+')) {
            if (f_len != t_len || memcmp(f, t, f_len) != 0)
                return 0;
        }

        if (f_end)
            f = f_end + 1;
        else
            f += f_len;

        if (t_end) {
            t = t_end + 1;
            remaining -= t_len + 1;
            has_level = 1;
        } else {
            t += t_len;
            remaining = 0;
            has_level = 0;
        }
    }

    return !has_level;
}

static int effective_user(const char *username) {
    const char *name = (username && *username) ? username : "anonymous";
    int idx = find_user(name);
    if (idx < 0)
        idx = find_user("*");
    return idx;
}

int acl_check_publish(const char *username, const char *topic,
                      size_t topic_len) {
    if (!g_enabled)
        return 1;
    if (!topic || topic_len == 0)
        return 0;

    int user = effective_user(username);
    if (user < 0)
        return 0;

    for (int i = 0; i < g_rule_count; i++) {
        acl_rule_t *r = &g_rules[i];
        if (r->user == user && (r->actions & ACL_WRITE) &&
            topic_matches_filter(r->filter, topic, topic_len))
            return 1;
    }
    return 0;
}

int acl_check_subscribe(const char *username, const char *filter) {
    if (!g_enabled)
        return 1;
    if (!filter)
        return 0;

    int user = effective_user(username);
    if (user < 0)
        return 0;

    for (int i = 0; i < g_rule_count; i++) {
        acl_rule_t *r = &g_rules[i];
        if (r->user == user && (r->actions & ACL_READ) &&
            filter_covers(r->filter, filter))
            return 1;
    }
    return 0;
}
