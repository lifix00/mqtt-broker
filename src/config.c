#include "config.h"

broker_config_t g_config;

void config_defaults(void) {
    g_config.max_sessions = 1024;
    g_config.max_retained = 4096;
    g_config.max_subscriptions = 256;
    g_config.max_read_buffer = 1024u * 1024u;   // 1 MiB
    g_config.max_write_buffer = 2u * 1024u * 1024u; // 2 MiB
    g_config.qos_retry_ms = 2000;
    g_config.inflight_timeout_ms = 60000;
    g_config.max_inflight_retries = 0;
    g_config.session_expiry_ms = 86400000; // 24h
    g_config.stats_interval_ms = 0;
    g_config.auth_username = NULL;
    g_config.auth_password = NULL;
    g_config.tls_cert = NULL;
    g_config.tls_key = NULL;
    g_config.acl_file = NULL;
}
