#ifndef CONFIG_H
#define CONFIG_H

#include <stddef.h>

typedef struct {
    int max_sessions;       // 持久会话上限，0 表示不限制
    int max_retained;       // retained 消息上限，0 表示不限制
    int max_subscriptions;  // 每会话订阅上限，0 表示不限制
    size_t max_read_buffer;
    size_t max_write_buffer;
    int qos_retry_ms;
    int inflight_timeout_ms;   // 发送后多久仍未确认则丢弃，0 = 不限制
    int max_inflight_retries;  // 最大重传次数，0 = 不限制
    int session_expiry_ms;     // 离线持久会话存活时间，0 = 不限制
    int stats_interval_ms;

    // 可选用户名/密码认证；username 为 NULL 表示关闭认证
    const char *auth_username;
    const char *auth_password;

    // TLS；两个都为空表示明文，任一为空表示配置错误
    const char *tls_cert;
    const char *tls_key;

    // ACL 文件；NULL/空表示不启用授权
    const char *acl_file;
} broker_config_t;

extern broker_config_t g_config;

void config_defaults(void);

#endif
