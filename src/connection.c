#include "config.h"
#include "connection.h"
#include "mqtt.h"
#include "session.h"
#include "subscription.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <time.h>
#include <unistd.h>

// 所有活跃连接，供定时任务遍历
static connection_t *g_connections = NULL;

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
    c->state = SESSION_STATE_CONNECTING;
    c->keep_alive = 0;
    c->last_rx_ms = connection_now_ms();
    c->dead = 0;
    c->want_close = 0;

    c->prev = NULL;
    c->next = g_connections;
    if (g_connections)
        g_connections->prev = c;
    g_connections = c;

    return c;
}

connection_t *connection_first(void) { return g_connections; }

connection_t *connection_next(connection_t *c) { return c ? c->next : NULL; }

void connection_request_close(connection_t *c) {
    if (c->want_close)
        return;
    c->want_close = 1;
    // 立即尝试 flush；如果发完了，try_flush 内部会把 epoll 事件摘到只剩 EPOLLIN
    connection_try_flush(c);
}

void connection_destroy(connection_t *c) {
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
    }

    // 异常断开时发布 Will；正常 DISCONNECT 已把 will_suppress 置 1
    mqtt_publish_will(c);

    free(c->will_topic);
    free(c->will_payload);
    free(c->rbuf);
    free(c->wbuf);
    close(c->fd);
    free(c);
}

// 处理完一批 epoll 事件后，统一回收被接管的旧连接。
// 这样即使旧连接的事件还在同一批 events[] 里，也不会访问已释放内存。
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
        if (c->dead || c->state != SESSION_STATE_CONNECTED ||
            c->keep_alive == 0)
            continue;

        // MQTT 3.1.1：1.5 倍 Keep Alive 内没有收到任何报文则断开
        uint64_t limit = (uint64_t)c->keep_alive * 1500u;
        if (now_ms - c->last_rx_ms > limit) {
            printf("fd=%d keepalive timeout after %u s\n", c->fd,
                   c->keep_alive);
            c->dead = 1;
            c->want_close = 1;
            epoll_ctl(g_epfd, EPOLL_CTL_DEL, c->fd, NULL);
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

int connection_get_fd(connection_t *c) { return c->fd; }

// 根据 wbuf 是否有未发送的数据，动态注册/取消 EPOLLOUT
void connection_update_events(connection_t *c) {
    uint32_t ev = EPOLLIN;
    if (c->wpos < c->wlen) {
        ev |= EPOLLOUT; // 只有还有未发送数据时才关注可写
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
    while (c->wpos < c->wlen) {
        ssize_t n = write(c->fd, c->wbuf + c->wpos, c->wlen - c->wpos);
        if (n > 0) {
            c->wpos += n;
        } else if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                // 内核缓冲满，等 EPOLLOUT
                connection_update_events(c);
                return 0;
            }
            if (errno == EINTR)
                continue;
            return -1;
        } else {
            // n == 0 不会发生在 write 上，保险起见
            return -1;
        }
    }
    // 全发完，重置
    c->wpos = 0;
    c->wlen = 0;
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
