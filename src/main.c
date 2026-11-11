#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include "connection.h"
#include "mqtt.h"
#include "subscription.h"

#define MAX_EVENTS 64
#define LISTEN_PORT 1883

int g_epfd = -1;
static int g_listen_marker; // 用一个静态变量地址作为 listener 的标记

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

// 处理一个连接上的 EPOLLIN：读到 EAGAIN，把数据累积到
// rbuf，然后交给mqtt协议层解析
static void handle_connection_read(connection_t *c) {
    int peer_closed = 0;

    for (;;) {
        if (c->rlen == sizeof(c->rbuf)) {
            fprintf(stderr, "fd=%d read buffer full, closing\n", c->fd);
            goto close_conn;
        }
        ssize_t n = read(c->fd, c->rbuf + c->rlen, sizeof(c->rbuf) - c->rlen);

        if (n > 0) {
            c->rlen += (size_t)n;
        } else if (n == 0) {
            peer_closed = 1;
            break;
        } else {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                break;
            if (errno == EINTR)
                continue;
            perror("read");
            goto close_conn;
        }
    }

    int rc = mqtt_process(c);
    if (rc < 0 || c->want_close) {
        if (c->wpos < c->wlen)
            return; // 等 EPOLLOUT flush 完再关
        goto close_conn;
    }
    if (peer_closed)
        goto close_conn;
    return;

close_conn:
    printf("fd=%d connection closed\n", c->fd);
    epoll_ctl(g_epfd, EPOLL_CTL_DEL, c->fd, NULL);
    connection_destroy(c);
}

int main(int argc, char *argv[]) {
    int port = LISTEN_PORT;
    for (int i = 1; i < argc; i++) {
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
        } else {
            fprintf(stderr, "usage: %s [--port PORT]\n", argv[0]);
            return 1;
        }
    }

    signal(SIGPIPE, SIG_IGN); // 忽略 write 到关闭的 socket 触发的 SIGPIPE
    session_store_init();
    subscription_init();

    int listen_fd = create_listener(port);
    printf("Broker listening on port %d (fd=%d)\n", port, listen_fd);

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
    for (;;) {
        // 200ms 超时用于驱动 QoS1 超时重传
        int n = epoll_wait(g_epfd, events, MAX_EVENTS, 200);
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

                    char ip[INET_ADDRSTRLEN];
                    inet_ntop(AF_INET, &ca.sin_addr, ip, sizeof(ip));
                    printf("Accepted %s:%d (fd=%d)\n", ip, ntohs(ca.sin_port),
                           client_fd);

                    struct epoll_event cev;
                    cev.events = EPOLLIN;
                    cev.data.ptr = c;
                    if (epoll_ctl(g_epfd, EPOLL_CTL_ADD, client_fd, &cev) < 0) {
                        perror("epoll_ctl ADD client");
                        connection_destroy(c);
                        continue;
                    }
                    c->epoll_events = EPOLLIN;
                }
                continue;
            }

            // ---- 普通连接 ----
            connection_t *c = (connection_t *)ptr;
            if (c->dead)
                continue; // 已被新连接接管，等本轮事件处理完统一回收

            if (events[i].events & (EPOLLERR | EPOLLHUP)) {
                printf("fd=%d error/hup, closing\n", c->fd);
                epoll_ctl(g_epfd, EPOLL_CTL_DEL, c->fd, NULL);
                connection_destroy(c);
                continue;
            }

            if (events[i].events & EPOLLIN) {
                handle_connection_read(c);
                continue;
            }

            if (events[i].events & EPOLLOUT) {
                if (connection_try_flush(c) < 0) {
                    epoll_ctl(g_epfd, EPOLL_CTL_DEL, c->fd, NULL);
                    connection_destroy(c);
                    continue;
                }
                if (c->want_close && c->wpos == c->wlen) {
                    printf("fd=%d connection closed (graceful)\n", c->fd);
                    epoll_ctl(g_epfd, EPOLL_CTL_DEL, c->fd, NULL);
                    connection_destroy(c);
                }
            }
        }

        // 先回收本轮被接管的旧连接，再跑重传/补发
        connection_reap_dead();

        uint64_t now = connection_now_ms();
        if (now - last_retry >= 200) {
            mqtt_retransmit_all(now);
            last_retry = now;
        }
    }

    close(listen_fd);
    close(g_epfd);
    return 0;
}
