#ifndef CONNECTION_H
#define CONNECTION_H

#include <stddef.h>
#include <stdint.h>

#include "session.h"

#define INIT_READ_BUF_SIZE  4096
#define INIT_WRITE_BUF_SIZE 4096
#define MAX_USERNAME_LEN    128

struct ssl_st;

// MQTT 连接协议状态：CONNECT 是否已经成功处理
typedef enum {
    CONN_STATE_CONNECTING = 0, // TCP 已连上，但还没有成功处理 CONNECT；此时只允许 CONNECT 包
    CONN_STATE_CONNECTED,  // CONNECT 已接受，session 已绑定，其他报文才允许处理
} connection_state_t;

// 当前连接在 CONNECT 中登记的 Will；连接关闭时需要发布
typedef struct {
    char    *topic;
    size_t   topic_len;
    uint8_t *payload;
    size_t   payload_len;
    uint8_t  qos;
    uint8_t  retain;
} will_message_t;

typedef struct connection {
    int fd;

    // TLS：ssl 为 NULL 表示明文；握手完成后才处理 MQTT
    struct ssl_st *ssl;
    int tls_handshake_done;
    int tls_want_write; // 握手/SSL_write 当前需要可写事件
    int tls_read_wants_write; // SSL_read 遇到重协商/KeyUpdate，需要先写

    char *rbuf;
    size_t rlen; // buf 中已有字节个数
    size_t rcap; // rbuf 容量

    char *wbuf;
    size_t wlen; // 有效字节数
    size_t wpos; // 已发送位置
    size_t wcap; // wbuf 容量
    uint32_t epoll_events;

    session_t *session;      // CONNECT 成功后指向会话；离线会话由会话表持有
    connection_state_t state; // MQTT CONNECT 握手状态

    // CONNECT 成功后确定的授权身份；未启用认证时为 "anonymous"
    char username[MAX_USERNAME_LEN];

    // 当前连接的 Keep Alive（秒），0 表示关闭
    uint16_t keep_alive;
    uint64_t last_rx_ms; // 最后一次收到任何字节的时间

    // 当前连接的 Will（属于本次连接，不属于持久会话）
    will_message_t will;

    // 所有连接组成的双向链表，供定时任务（如 QoS1 重传）遍历
    struct connection *prev;
    struct connection *next;

    int dead;          // 已被新连接接管，等本轮事件处理完再回收
    int want_close;    // flush完再关闭
} connection_t;

// TLS 初始化/清理；cert/key 都为空表示明文
int connection_tls_init(const char *cert, const char *key);
void connection_tls_cleanup(void);
int connection_tls_enabled(void);

connection_t *connection_create(int fd);
int connection_get_fd(connection_t *c);
void connection_update_events(connection_t *c);
int connection_try_flush(connection_t *c);
int connection_send(connection_t *c, const void *data, size_t n);
void connection_request_close(connection_t *c);

// 释放 Will；正常 DISCONNECT 时调用，避免连接销毁时误发布
void connection_clear_will(connection_t *c);

// 读缓冲扩容；失败（达到上限/内存不足）返回 -1
int connection_grow_read(connection_t *c);

typedef enum {
    CONN_READ_OK = 0,   // 读到数据或 EAGAIN，缓冲中可能有待解析报文
    CONN_READ_EOF,      // 对端关闭
    CONN_READ_ERROR,    // read 出错
    CONN_READ_LIMIT,    // 读缓冲达到上限且已满
} connection_read_result_t;

// 把 socket 当前可读的数据读入 rbuf；不做 MQTT 解析
connection_read_result_t connection_read(connection_t *c);

// 标记连接立即关闭：不再 flush，等本批事件处理完后由 connection_reap_dead 回收
void connection_mark_dead(connection_t *c);

// 回收本轮被标记 dead 的连接（必须在处理完一次 epoll 事件数组后调用）
void connection_reap_dead(void);

// 检查 Keep Alive 超时，超时的连接标记为 dead，等待统一回收
void connection_check_keepalive(uint64_t now_ms);

// 单调时钟（毫秒），用于 QoS1 重传计时
uint64_t connection_now_ms(void);

// 遍历所有连接
connection_t *connection_first(void);
connection_t *connection_next(connection_t *c);

// 由 main.c 提供
extern int g_epfd;

#endif
