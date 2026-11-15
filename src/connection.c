#include "config.h"
#include "connection.h"
#include "metrics.h"
#include "mqtt.h"
#include "session.h"

#include <errno.h>
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <time.h>
#include <unistd.h>

// 所有活跃连接，供定时任务遍历
static connection_t *g_connections = NULL;

// TLS 上下文；非 NULL 时所有新连接都走 TLS
static SSL_CTX *g_tls_ctx = NULL;

static int tls_no_passwd(char *buf, int size, int rwflag, void *userdata) {
    (void)buf;
    (void)size;
    (void)rwflag;
    (void)userdata;
    // 不交互式询问密码；加密私钥请先解密或使用无密码 key。
    return 0;
}

static void tls_log_errors(const char *what) {
    unsigned long err;
    int had_error = 0;
    while ((err = ERR_get_error()) != 0) {
        char buf[256];
        ERR_error_string_n(err, buf, sizeof(buf));
        fprintf(stderr, "%s: %s\n", what, buf);
        had_error = 1;
    }
    if (!had_error)
        fprintf(stderr, "%s\n", what);
}

int connection_tls_init(const char *cert, const char *key) {
    if (!cert && !key)
        return 0;
    if (!cert || !key) {
        fprintf(stderr, "TLS requires both --tls-cert and --tls-key\n");
        return -1;
    }

    OPENSSL_init_ssl(0, NULL);
    SSL_CTX *ctx = SSL_CTX_new(TLS_server_method());
    if (!ctx) {
        tls_log_errors("SSL_CTX_new");
        return -1;
    }

    SSL_CTX_set_min_proto_version(ctx, TLS1_2_VERSION);
    long tls_opts = SSL_OP_NO_COMPRESSION | SSL_OP_CIPHER_SERVER_PREFERENCE;
#ifdef SSL_OP_NO_RENEGOTIATION
    tls_opts |= SSL_OP_NO_RENEGOTIATION;
#endif
    SSL_CTX_set_options(ctx, tls_opts);
    SSL_CTX_set_mode(ctx, SSL_MODE_ENABLE_PARTIAL_WRITE |
                              SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER);
    SSL_CTX_set_default_passwd_cb(ctx, tls_no_passwd);

    if (SSL_CTX_use_certificate_chain_file(ctx, cert) != 1) {
        tls_log_errors("load TLS certificate");
        SSL_CTX_free(ctx);
        return -1;
    }
    if (SSL_CTX_use_PrivateKey_file(ctx, key, SSL_FILETYPE_PEM) != 1) {
        tls_log_errors("load TLS private key");
        SSL_CTX_free(ctx);
        return -1;
    }
    if (SSL_CTX_check_private_key(ctx) != 1) {
        tls_log_errors("TLS certificate/private key mismatch");
        SSL_CTX_free(ctx);
        return -1;
    }

    g_tls_ctx = ctx;
    return 0;
}

void connection_tls_cleanup(void) {
    if (g_tls_ctx) {
        SSL_CTX_free(g_tls_ctx);
        g_tls_ctx = NULL;
    }
}

int connection_tls_enabled(void) { return g_tls_ctx != NULL; }

uint64_t connection_now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000u + (uint64_t)ts.tv_nsec / 1000000u;
}

connection_t *connection_create(int fd) {
    connection_t *c = calloc(1, sizeof(*c));
    if (!c)
        return NULL;

    c->rbuf = malloc(INIT_READ_BUF_SIZE);
    c->wbuf = malloc(INIT_WRITE_BUF_SIZE);
    if (!c->rbuf || !c->wbuf) {
        free(c->rbuf);
        free(c->wbuf);
        free(c);
        return NULL;
    }

    c->fd = fd;
    c->rlen = 0;
    c->rcap = INIT_READ_BUF_SIZE;
    c->wlen = 0;
    c->wpos = 0;
    c->wcap = INIT_WRITE_BUF_SIZE;
    c->epoll_events = 0;
    c->session = NULL;
    c->state = CONN_STATE_CONNECTING;
    c->keep_alive = 0;
    c->last_rx_ms = connection_now_ms();
    c->dead = 0;
    c->want_close = 0;

    // 默认明文；启用 TLS 时在这里创建 SSL 对象并进入握手状态。
    c->tls_handshake_done = 1;
    c->tls_want_write = 0;
    c->tls_read_wants_write = 0;
    if (g_tls_ctx) {
        c->ssl = SSL_new(g_tls_ctx);
        if (!c->ssl || SSL_set_fd(c->ssl, fd) != 1) {
            if (c->ssl)
                SSL_free(c->ssl);
            free(c->rbuf);
            free(c->wbuf);
            free(c);
            return NULL;
        }
        SSL_set_accept_state(c->ssl);
        c->tls_handshake_done = 0;
    }

    c->prev = NULL;
    c->next = g_connections;
    if (g_connections)
        g_connections->prev = c;
    g_connections = c;

    return c;
}

connection_t *connection_first(void) { return g_connections; }

connection_t *connection_next(connection_t *c) { return c ? c->next : NULL; }

void connection_clear_will(connection_t *c) {
    if (!c)
        return;
    free(c->will.topic);
    free(c->will.payload);
    memset(&c->will, 0, sizeof(c->will));
}

void connection_request_close(connection_t *c) {
    if (c->want_close)
        return;
    c->want_close = 1;
    // 立即尝试 flush；如果发完了，try_flush 内部会把 epoll 事件摘到只剩 EPOLLIN
    connection_try_flush(c);
}

static void connection_destroy(connection_t *c) {
    if (!c)
        return;

    // 从全局链表摘除
    if (c->prev)
        c->prev->next = c->next;
    else
        g_connections = c->next;
    if (c->next)
        c->next->prev = c->prev;

    // 会话可能比连接活得更久（clean_session=0）
    session_t *s = c->session;
    c->session = NULL;
    if (s) {
        s->conn = NULL;
        if (s->clean_session)
            session_destroy(s);
        else
            session_mark_offline(s);
    }

    // 异常断开时发布 Will；正常 DISCONNECT 已提前 clear
    mqtt_publish_will(c);
    connection_clear_will(c);

    if (c->ssl) {
        // 非阻塞连接上只尽力发送 close_notify，不等待对端确认。
        SSL_shutdown(c->ssl);
        SSL_free(c->ssl);
        c->ssl = NULL;
    }

    free(c->rbuf);
    free(c->wbuf);
    close(c->fd);
    free(c);
    metrics_connection_closed();
}

void connection_mark_dead(connection_t *c) {
    if (!c || c->dead)
        return;
    c->dead = 1;
    epoll_ctl(g_epfd, EPOLL_CTL_DEL, c->fd, NULL);
}

// 处理完一批 epoll 事件后，统一回收所有 dead 连接。
// 这样即使同一批 events[] 里还有该连接的后续事件，也不会访问已释放内存。
void connection_reap_dead(void) {
    connection_t *c = g_connections;
    while (c) {
        connection_t *next = c->next;
        if (c->dead)
            connection_destroy(c);
        c = next;
    }
}


void connection_check_keepalive(uint64_t now_ms) {
    for (connection_t *c = g_connections; c; c = c->next) {
        if (c->dead || c->state != CONN_STATE_CONNECTED ||
            c->keep_alive == 0)
            continue;

        // MQTT 3.1.1：1.5 倍 Keep Alive 内没有收到任何报文则断开
        uint64_t limit = (uint64_t)c->keep_alive * 1500u;
        if (now_ms - c->last_rx_ms > limit) {
            printf("fd=%d keepalive timeout after %u s\n", c->fd,
                   c->keep_alive);
            connection_mark_dead(c);
        }
    }
}

// ==== 动态缓冲 ====

int connection_grow_read(connection_t *c) {
    size_t max = g_config.max_read_buffer;
    if (c->rcap >= max)
        return -1;

    size_t ncap = c->rcap * 2;
    if (ncap > max)
        ncap = max;

    char *p = realloc(c->rbuf, ncap);
    if (!p)
        return -1;
    c->rbuf = p;
    c->rcap = ncap;
    return 0;
}

static int connection_ensure_write(connection_t *c, size_t extra) {
    if (c->wlen + extra <= c->wcap)
        return 0;

    // 先尝试把已发送部分移走，避免不必要地扩容
    if (c->wpos > 0) {
        memmove(c->wbuf, c->wbuf + c->wpos, c->wlen - c->wpos);
        c->wlen -= c->wpos;
        c->wpos = 0;
        if (c->wlen + extra <= c->wcap)
            return 0;
    }

    size_t max = g_config.max_write_buffer;
    size_t need = c->wlen + extra;
    if (need > max)
        return -1;

    size_t ncap = c->wcap;
    while (ncap < need) {
        if (ncap >= max)
            return -1;
        ncap *= 2;
        if (ncap > max)
            ncap = max;
    }

    char *p = realloc(c->wbuf, ncap);
    if (!p)
        return -1;
    c->wbuf = p;
    c->wcap = ncap;
    return 0;
}

static int connection_tls_handshake(connection_t *c) {
    if (!c->ssl || c->tls_handshake_done)
        return 0;

    ERR_clear_error();
    int ret = SSL_accept(c->ssl);
    if (ret == 1) {
        c->tls_handshake_done = 1;
        c->tls_want_write = 0;
        connection_update_events(c);
        return 0;
    }

    int err = SSL_get_error(c->ssl, ret);
    if (err == SSL_ERROR_WANT_READ) {
        c->tls_want_write = 0;
        connection_update_events(c);
        return 0;
    }
    if (err == SSL_ERROR_WANT_WRITE) {
        c->tls_want_write = 1;
        connection_update_events(c);
        return 0;
    }

    fprintf(stderr, "fd=%d TLS handshake failed\n", c->fd);
    tls_log_errors("TLS handshake");
    return -1;
}

connection_read_result_t connection_read(connection_t *c) {
    if (c->ssl && !c->tls_handshake_done) {
        if (connection_tls_handshake(c) < 0)
            return CONN_READ_ERROR;
        if (!c->tls_handshake_done)
            return CONN_READ_OK;
    }

    for (;;) {
        if (c->rlen == c->rcap) {
            if (connection_grow_read(c) < 0)
                return CONN_READ_LIMIT;
        }

        ssize_t n;
        if (c->ssl) {
            ERR_clear_error();
            int r = SSL_read(c->ssl, c->rbuf + c->rlen,
                             (int)(c->rcap - c->rlen));
            if (r > 0) {
                n = r;
                if (c->tls_read_wants_write) {
                    c->tls_read_wants_write = 0;
                }
                if (c->tls_want_write) {
                    c->tls_want_write = 0;
                    connection_update_events(c);
                }
            } else {
                int err = SSL_get_error(c->ssl, r);
                if (err == SSL_ERROR_WANT_READ)
                    return CONN_READ_OK;
                if (err == SSL_ERROR_WANT_WRITE) {
                    c->tls_read_wants_write = 1;
                    c->tls_want_write = 1;
                    connection_update_events(c);
                    return CONN_READ_OK;
                }
                if (err == SSL_ERROR_ZERO_RETURN)
                    return CONN_READ_EOF;
                if (err == SSL_ERROR_SYSCALL && r == 0)
                    return CONN_READ_EOF;
                fprintf(stderr, "fd=%d TLS read failed\n", c->fd);
                tls_log_errors("TLS read");
                return CONN_READ_ERROR;
            }
        } else {
            n = read(c->fd, c->rbuf + c->rlen, c->rcap - c->rlen);
            if (n < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK)
                    return CONN_READ_OK;
                if (errno == EINTR)
                    continue;
                perror("read");
                return CONN_READ_ERROR;
            }
            if (n == 0)
                return CONN_READ_EOF;
        }

        c->rlen += (size_t)n;
        c->last_rx_ms = connection_now_ms();
        metrics_bytes_in((size_t)n);
    }
}

int connection_get_fd(connection_t *c) { return c->fd; }

// 自动根据 wbuf 是否有未发送的数据，动态注册/取消 EPOLLOUT
// 如果写缓冲区中有未发送的数据，要修改注册这个连接绑定EPOLLIN | EPOLLOUT，让内核通知可以发送;
// 如果写缓冲区中无未发送的数据，要修改注册这个连接绑定EPOLLIN，就算可以发数据也不要通知。
void connection_update_events(connection_t *c) {
    uint32_t ev = EPOLLIN;
    if (c->wpos < c->wlen || c->tls_want_write) {
        ev |= EPOLLOUT; // 写缓冲未清空，或 TLS 握手/写入需要可写事件
    }
    if (ev == c->epoll_events)
        return;

    struct epoll_event e;
    e.events = ev;
    e.data.ptr = c;

    if (c->epoll_events == 0) {
        // 第一次注册（理论上 accept 时已经注册，这里兜底）
        epoll_ctl(g_epfd, EPOLL_CTL_ADD, c->fd, &e);
    } else {
        epoll_ctl(g_epfd, EPOLL_CTL_MOD, c->fd, &e);
    }
    c->epoll_events = ev;
}

// 尽量把 wbuf 里未发数据写完
int connection_try_flush(connection_t *c) {
    if (c->ssl && !c->tls_handshake_done) {
        if (connection_tls_handshake(c) < 0)
            return -1;
        if (!c->tls_handshake_done)
            return 0;
    }

    while (c->wpos < c->wlen) {
        ssize_t n;
        if (c->ssl) {
            ERR_clear_error();
            int r = SSL_write(c->ssl, c->wbuf + c->wpos,
                              (int)(c->wlen - c->wpos));
            if (r > 0) {
                n = r;
                c->tls_want_write = 0;
            } else {
                int err = SSL_get_error(c->ssl, r);
                if (err == SSL_ERROR_WANT_WRITE) {
                    // 内核写缓冲满，等 EPOLLOUT
                    c->tls_want_write = 1;
                    connection_update_events(c);
                    return 0;
                }
                if (err == SSL_ERROR_WANT_READ) {
                    // TLS 重协商/KeyUpdate 需要先读；EPOLLIN 一直开着
                    c->tls_want_write = 0;
                    connection_update_events(c);
                    return 0;
                }
                fprintf(stderr, "fd=%d TLS write failed\n", c->fd);
                tls_log_errors("TLS write");
                return -1;
            }
        } else {
            n = write(c->fd, c->wbuf + c->wpos, c->wlen - c->wpos);
            if (n < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    // 内核缓冲满，等 EPOLLOUT
                    connection_update_events(c);
                    return 0;
                }
                if (errno == EINTR)
                    continue;
                return -1;
            }
            if (n == 0) {
                // n == 0 不会发生在 write 上，保险起见
                return -1;
            }
        }

        c->wpos += (size_t)n;
        metrics_bytes_out((size_t)n);
    }

    // 全发完，重置
    c->wpos = 0;
    c->wlen = 0;
    c->tls_want_write = 0;
    connection_update_events(c); // 顺便取消 EPOLLOUT
    return 0;
}

// 上层调用的函数接口，先缓冲，再尝试发
int connection_send(connection_t *c, const void *data, size_t n) {
    if (connection_ensure_write(c, n) < 0) {
        fprintf(stderr, "fd=%d write buffer limit exceeded, closing\n",
                c->fd);
        return -1;
    }
    memcpy(c->wbuf + c->wlen, data, n);
    c->wlen += n;
    return connection_try_flush(c);
}
