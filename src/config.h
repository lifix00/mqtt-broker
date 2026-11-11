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
} broker_config_t;

extern broker_config_t g_config;

void config_defaults(void);

#endif
