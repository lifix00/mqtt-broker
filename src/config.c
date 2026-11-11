#include "config.h"

broker_config_t g_config;

void config_defaults(void) {
    g_config.max_sessions = 1024;
    g_config.max_retained = 4096;
    g_config.max_subscriptions = 256;
    g_config.max_read_buffer = 1024u * 1024u;   // 1 MiB
    g_config.max_write_buffer = 2u * 1024u * 1024u; // 2 MiB
    g_config.qos_retry_ms = 2000;
    g_config.stats_interval_ms = 0;
    g_config.auth_username = NULL;
    g_config.auth_password = NULL;
}
