#ifndef CONNECTION_H
#define CONNECTION_H

#include <stddef.h>
#include <stdint.h>

#include "session.h"

#define READ_BUF_SIZE 4096
#define WRITE_BUF_SIZE 65536

typedef struct connection {
    int fd;

    char rbuf[READ_BUF_SIZE];
    size_t rlen; // buf 中已有字节个数

    char wbuf[WRITE_BUF_SIZE];
    size_t wlen; // 有效字节数
    size_t wpos; // 已发送位置
    uint32_t epoll_events;

    session_t *session;      // CONNECT 成功后指向会话；离线会话由会话表持有
    session_state_t state;   // 当前连接的协议状态

    // 所有连接组成的双向链表，供定时任务（如 QoS1 重传）遍历
    struct connection *prev;
    struct connection *next;

    int dead;          // 已被新连接接管，等本轮事件处理完再回收
    int want_close;    // flush完再关闭
} connection_t;

connection_t *connection_create(int fd);
void connection_destroy(connection_t *c);
int connection_get_fd(connection_t *c);
void connection_update_events(connection_t *c);
int connection_try_flush(connection_t *c);
int connection_send(connection_t *c, const void *data, size_t n);
void connection_request_close(connection_t *c);

// 回收本轮被标记 dead 的连接（必须在处理完一次 epoll 事件数组后调用）
void connection_reap_dead(void);

// 单调时钟（毫秒），用于 QoS1 重传计时
uint64_t connection_now_ms(void);

// 遍历所有连接
connection_t *connection_first(void);
connection_t *connection_next(connection_t *c);

// 由 main.c 提供
extern int g_epfd;

#endif
