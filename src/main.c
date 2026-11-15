#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include "acl.h"
#include "config.h"
#include "connection.h"
#include "metrics.h"
#include "mqtt.h"
#include "persistence.h"
#include "retained.h"
#include "session.h"
#include "subscription.h"

#define MAX_EVENTS 64
#define LISTEN_PORT 1883

int g_epfd = -1;
static int g_listen_marker; // 用一个静态变量地址作为 listener 的标记
static volatile sig_atomic_t g_shutdown = 0;
static volatile sig_atomic_t g_stats_requested = 0;

static void handle_shutdown_signal(int sig) {
    (void)sig;
    g_shutdown = 1;
}

static void handle_stats_signal(int sig) {
    (void)sig;
    g_stats_requested = 1;
}

// 停止 accept，发布所有在线连接的 Will，尽量 flush 已缓冲数据，再统一关闭
static void broker_shutdown(int listen_fd) {
    printf("shutting down...\n");

    epoll_ctl(g_epfd, EPOLL_CTL_DEL, listen_fd, NULL);
    close(listen_fd);

    // 是否真正发布由 mqtt_publish_will 判断：
    // 只有 CONNECT 成功且 Will 尚未被正常 DISCONNECT 清除的连接才会发布。
    for (connection_t *c = connection_first(); c; c = connection_next(c))
        mqtt_publish_will(c);

    uint64_t deadline = connection_now_ms() + 1000;
    for (;;) {
        int pending = 0;
        for (connection_t *c = connection_first(); c; c = connection_next(c)) {
            if (c->dead)
                continue;
            if (c->wpos < c->wlen) {
                if (connection_try_flush(c) < 0)
                    continue;
                if (c->wpos < c->wlen)
                    pending = 1;
            }
        }
        if (!pending || connection_now_ms() >= deadline)
            break;
        struct timespec ts = {0, 10 * 1000 * 1000};
        nanosleep(&ts, NULL);
    }

    for (connection_t *c = connection_first(); c; c = connection_next(c))
        connection_mark_dead(c);
    connection_reap_dead();
    persistence_shutdown();
}

// 创建一个非阻塞的监听socket，待连接的队列长度128
static int create_listener(int port) {
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (fd < 0) {
        perror("socket");
        exit(1);
    }

    int yes = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(port);

    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind");
        exit(1);
    }
    if (listen(fd, 128) < 0) {
        perror("listen");
        exit(1);
    }
    return fd;
}

// 处理一个连接上的 EPOLLIN：先读 socket，再解析已缓冲的 MQTT 报文
static void handle_connection_read(connection_t *c) {
    connection_read_result_t rr = connection_read(c);

    int rc = mqtt_process(c);
    if (rc < 0 || c->want_close) {
        if (c->wpos < c->wlen)
            return; // 等 EPOLLOUT flush 完再关
        goto close_conn;
    }
    // 扩容到上限后仍然塞满，且协议层也消费不出完整报文：放弃
    if (rr == CONN_READ_LIMIT && c->rlen == c->rcap) {
        fprintf(stderr, "fd=%d read buffer limit reached, closing\n", c->fd);
        goto close_conn;
    }
    if (rr == CONN_READ_EOF || rr == CONN_READ_ERROR)
        goto close_conn;
    if (c->wpos < c->wlen && connection_try_flush(c) < 0)
        goto close_conn;
    return;

close_conn:
    printf("fd=%d connection closed\n", c->fd);
    connection_mark_dead(c);
}

static int parse_long_option(const char *name, const char *value, long min,
                             long max, long *out) {
    char *end = NULL;
    errno = 0;
    long v = strtol(value, &end, 10);
    if (errno != 0 || !end || *end != '\0' || v < min || v > max) {
        fprintf(stderr, "invalid %s: %s\n", name, value);
        return -1;
    }
    *out = v;
    return 0;
}

static void print_usage(const char *prog) {
    fprintf(stderr,
            "usage: %s [options]\n"
            "  --port PORT\n"
            "  --state-file PATH\n"
            "  --max-sessions N       持久会话上限，0 = 不限制\n"
            "  --max-retained N       retained 上限，0 = 不限制\n"
            "  --max-subscriptions N  每会话订阅上限，0 = 不限制\n"
            "  --max-read-buffer N    读缓冲上限（字节）\n"
            "  --max-write-buffer N   写缓冲上限（字节）\n"
            "  --qos-retry-ms N       QoS1/QoS2 重传间隔\n"
            "  --inflight-timeout-ms N 发送后未确认的超时，0 = 不限制\n"
            "  --max-inflight-retries N 最大重传次数，0 = 不限制\n"
            "  --session-expiry-ms N  离线持久会话存活时间，0 = 不限制\n"
            "  --stats-interval-ms N  定期打印统计，0 = 关闭\n"
            "  --username USER        启用用户名认证\n"
            "  --password PASS        启用密码认证（需同时提供 --username）\n"
            "  --tls-cert PATH        TLS 证书链（PEM）\n"
            "  --tls-key PATH         TLS 私钥（PEM，需与证书匹配）\n"
            "  --acl-file PATH        启用 topic 授权（ACL 文件）\n",
            prog);
}

int main(int argc, char *argv[]) {
    int port = LISTEN_PORT;
    const char *state_path = "mqtt-broker.state";
    config_defaults();

    for (int i = 1; i < argc; i++) {
        long v;
        if (strcmp(argv[i], "--port") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr, "--port requires a value\n");
                return 1;
            }
            port = atoi(argv[++i]);
            if (port <= 0 || port > 65535) {
                fprintf(stderr, "invalid port: %d\n", port);
                return 1;
            }
        } else if (strcmp(argv[i], "--state-file") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr, "--state-file requires a value\n");
                return 1;
            }
            state_path = argv[++i];
        } else if (strcmp(argv[i], "--max-sessions") == 0) {
            if (i + 1 >= argc ||
                parse_long_option("--max-sessions", argv[++i], 0, 1000000,
                                  &v) < 0)
                return 1;
            g_config.max_sessions = (int)v;
        } else if (strcmp(argv[i], "--max-retained") == 0) {
            if (i + 1 >= argc ||
                parse_long_option("--max-retained", argv[++i], 0, 1000000,
                                  &v) < 0)
                return 1;
            g_config.max_retained = (int)v;
        } else if (strcmp(argv[i], "--max-subscriptions") == 0) {
            if (i + 1 >= argc ||
                parse_long_option("--max-subscriptions", argv[++i], 0,
                                  1000000, &v) < 0)
                return 1;
            g_config.max_subscriptions = (int)v;
        } else if (strcmp(argv[i], "--max-read-buffer") == 0) {
            if (i + 1 >= argc ||
                parse_long_option("--max-read-buffer", argv[++i], 4096,
                                  64 * 1024 * 1024, &v) < 0)
                return 1;
            g_config.max_read_buffer = (size_t)v;
        } else if (strcmp(argv[i], "--max-write-buffer") == 0) {
            if (i + 1 >= argc ||
                parse_long_option("--max-write-buffer", argv[++i], 4096,
                                  64 * 1024 * 1024, &v) < 0)
                return 1;
            g_config.max_write_buffer = (size_t)v;
        } else if (strcmp(argv[i], "--qos-retry-ms") == 0) {
            if (i + 1 >= argc ||
                parse_long_option("--qos-retry-ms", argv[++i], 100, 600000,
                                  &v) < 0)
                return 1;
            g_config.qos_retry_ms = (int)v;
        } else if (strcmp(argv[i], "--inflight-timeout-ms") == 0) {
            if (i + 1 >= argc ||
                parse_long_option("--inflight-timeout-ms", argv[++i], 0,
                                  86400000, &v) < 0)
                return 1;
            g_config.inflight_timeout_ms = (int)v;
        } else if (strcmp(argv[i], "--max-inflight-retries") == 0) {
            if (i + 1 >= argc ||
                parse_long_option("--max-inflight-retries", argv[++i], 0,
                                  1000000, &v) < 0)
                return 1;
            g_config.max_inflight_retries = (int)v;
        } else if (strcmp(argv[i], "--session-expiry-ms") == 0) {
            if (i + 1 >= argc ||
                parse_long_option("--session-expiry-ms", argv[++i], 0,
                                  604800000, &v) < 0)
                return 1;
            g_config.session_expiry_ms = (int)v;
        } else if (strcmp(argv[i], "--stats-interval-ms") == 0) {
            if (i + 1 >= argc ||
                parse_long_option("--stats-interval-ms", argv[++i], 0,
                                  3600000, &v) < 0)
                return 1;
            g_config.stats_interval_ms = (int)v;
        } else if (strcmp(argv[i], "--username") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr, "--username requires a value\n");
                return 1;
            }
            g_config.auth_username = argv[++i];
        } else if (strcmp(argv[i], "--password") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr, "--password requires a value\n");
                return 1;
            }
            g_config.auth_password = argv[++i];
        } else if (strcmp(argv[i], "--tls-cert") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr, "--tls-cert requires a value\n");
                return 1;
            }
            g_config.tls_cert = argv[++i];
        } else if (strcmp(argv[i], "--tls-key") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr, "--tls-key requires a value\n");
                return 1;
            }
            g_config.tls_key = argv[++i];
        } else if (strcmp(argv[i], "--acl-file") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr, "--acl-file requires a value\n");
                return 1;
            }
            g_config.acl_file = argv[++i];
        } else {
            print_usage(argv[0]);
            return 1;
        }
    }

    if (g_config.auth_password && !g_config.auth_username) {
        fprintf(stderr, "--password requires --username\n");
        return 1;
    }
    if (g_config.auth_username &&
        strlen(g_config.auth_username) >= MAX_USERNAME_LEN) {
        fprintf(stderr, "--username too long\n");
        return 1;
    }
    if ((g_config.tls_cert && !g_config.tls_key) ||
        (!g_config.tls_cert && g_config.tls_key)) {
        fprintf(stderr, "--tls-cert and --tls-key must be provided together\n");
        return 1;
    }
    if (connection_tls_init(g_config.tls_cert, g_config.tls_key) < 0)
        return 1;
    if (g_config.acl_file && !g_config.auth_username) {
        fprintf(stderr, "warning: ACL enabled without authentication; "
                        "all clients are 'anonymous'\n");
    }
    if (acl_init(g_config.acl_file) < 0)
        return 1;

    signal(SIGPIPE, SIG_IGN); // 忽略 write 到关闭的 socket 触发的 SIGPIPE
    signal(SIGTERM, handle_shutdown_signal);
    signal(SIGINT, handle_shutdown_signal);
    signal(SIGUSR1, handle_stats_signal);

    session_store_init();
    subscription_init();
    retained_init();
    persistence_init(state_path);

    int listen_fd = create_listener(port);
    printf("Broker listening on port %d (fd=%d, %s%s)\n", port, listen_fd,
           connection_tls_enabled() ? "TLS" : "plaintext",
           acl_enabled() ? ", ACL" : "");

    g_epfd = epoll_create1(0);
    if (g_epfd < 0) {
        perror("epoll_create1");
        return 1;
    }

    struct epoll_event ev;
    ev.events = EPOLLIN;
    ev.data.ptr = &g_listen_marker;
    if (epoll_ctl(g_epfd, EPOLL_CTL_ADD, listen_fd, &ev) < 0) {
        perror("epoll_ctl ADD listener");
        return 1;
    }

    struct epoll_event events[MAX_EVENTS];
    uint64_t last_retry = connection_now_ms();
    uint64_t last_stats = connection_now_ms();
    uint64_t last_expiry = connection_now_ms();
    while (!g_shutdown) {
        // 有未落盘修改时缩短超时，让合并窗口尽快生效；否则 200ms
        // 用于驱动 QoS1 超时重传。
        int timeout_ms = persistence_is_dirty() ? 50 : 200;
        int n = epoll_wait(g_epfd, events, MAX_EVENTS, timeout_ms);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            perror("epoll_wait");
            break;
        }

        for (int i = 0; i < n; i++) {
            void *ptr = events[i].data.ptr;

            // ---- listener：accept 循环取空 ----
            if (ptr == &g_listen_marker) {
                for (;;) {
                    struct sockaddr_in ca;
                    socklen_t len = sizeof(ca);
                    int client_fd = accept4(listen_fd, (struct sockaddr *)&ca,
                                            &len, SOCK_NONBLOCK | SOCK_CLOEXEC);
                    if (client_fd < 0) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK)
                            break;
                        if (errno == EINTR)
                            continue;
                        perror("accept4");
                        break;
                    }

                    connection_t *c = connection_create(client_fd);
                    if (!c) {
                        close(client_fd);
                        continue;
                    }
                    metrics_connection_opened();

                    char ip[INET_ADDRSTRLEN];
                    inet_ntop(AF_INET, &ca.sin_addr, ip, sizeof(ip));
                    printf("Accepted %s:%d (fd=%d)\n", ip, ntohs(ca.sin_port),
                           client_fd);

                    struct epoll_event cev;
                    cev.events = EPOLLIN;
                    cev.data.ptr = c;
                    if (epoll_ctl(g_epfd, EPOLL_CTL_ADD, client_fd, &cev) < 0) {
                        perror("epoll_ctl ADD client");
                        connection_mark_dead(c);
                        continue;
                    }
                    c->epoll_events = EPOLLIN;
                }
                continue;
            }

            // ---- 普通连接 ----
            connection_t *c = (connection_t *)ptr;
            if (c->dead)
                continue; // 已标记立即关闭，等本批事件处理完统一回收

            // 先读完可读数据，避免 DISCONNECT 和 EOF/HUP 同时到达时
            // 直接按异常断开处理，错误地发布 Will
            if (events[i].events & EPOLLIN) {
                handle_connection_read(c);
                continue;
            }

            if (events[i].events & (EPOLLERR | EPOLLHUP)) {
                printf("fd=%d error/hup, closing\n", c->fd);
                connection_mark_dead(c);
                continue;
            }

            if (events[i].events & EPOLLOUT) {
                if (connection_try_flush(c) < 0) {
                    connection_mark_dead(c);
                    continue;
                }
                if (c->tls_read_wants_write) {
                    // SSL_read 之前需要先写（如 KeyUpdate），写完后重试读
                    c->tls_read_wants_write = 0;
                    handle_connection_read(c);
                    continue;
                }
                if (c->want_close && c->wpos == c->wlen) {
                    printf("fd=%d connection closed (graceful)\n", c->fd);
                    connection_mark_dead(c);
                }
            }
        }

        // 先检查 Keep Alive 超时，再统一回收 dead 连接（含 Will 发布）
        uint64_t now = connection_now_ms();
        connection_check_keepalive(now);
        connection_reap_dead();

        // 每秒扫描一次离线持久会话；用 wall clock 判断跨重启的过期时间
        if (now - last_expiry >= 1000) {
            session_expire_offline(session_wall_now_ms());
            last_expiry = now;
        }

        if (g_stats_requested) {
            g_stats_requested = 0;
            metrics_dump();
        }
        if (g_config.stats_interval_ms > 0 &&
            now - last_stats >= (uint64_t)g_config.stats_interval_ms) {
            metrics_dump();
            last_stats = now;
        }

        if (now - last_retry >= 200) {
            mqtt_retransmit_all(now);
            last_retry = now;
        }

        // 合并本轮的 retained/session 修改，把快照交给后台 writer。
        persistence_tick(now);
    }

    broker_shutdown(listen_fd);
    close(g_epfd);
    connection_tls_cleanup();
    acl_cleanup();
    printf("broker stopped\n");
    return 0;
}
