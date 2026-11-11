#ifndef MQTT_H
#define MQTT_H

#include "connection.h"

#include <stddef.h>
#include <stdint.h>

// MQTT Control Packet type
typedef enum {
    MQTT_CONNECT = 1,
    MQTT_CONNACK = 2,
    MQTT_PUBLISH = 3,
    MQTT_PUBACK = 4,
    MQTT_PUBREC = 5,
    MQTT_PUBREL = 6,
    MQTT_PUBCOMP = 7,
    MQTT_SUBSCRIBE = 8,
    MQTT_SUBACK = 9,
    MQTT_UNSUBSCRIBE = 10,
    MQTT_UNSUBACK = 11,
    MQTT_PINGREQ = 12,
    MQTT_PINGRESP = 13,
    MQTT_DISCONNECT = 14,
} mqtt_packet_type_t;

// CONNACK 返回码
#define MQTT_CONNACK_ACCEPTED 0
#define MQTT_CONNACK_BAD_PROTO_VERSION 1
#define MQTT_CONNACK_IDENTIFIER_REJECTED 2
#define MQTT_CONNACK_SERVER_UNAVAILABLE 3
#define MQTT_CONNACK_BAD_CREDENTIALS 4
#define MQTT_CONNACK_NOT_AUTHORIZED 5

typedef struct {
    uint8_t type;              // 1~14
    uint8_t flags;             // 固定头低 4 位
    uint32_t remaining_length; // 剩余长度值
    size_t header_len;         // 固定头总字节数 = 1 + 变长编码字节数
    size_t total_len;          // = header_len + remaining_length
} mqtt_fixed_header_t;

// ==== CONNECT ====
// 解码后的 CONNECT 内容
typedef struct {
    const char *client_id; // 指向 rbuf 内部，仅本次处理有效
    size_t client_id_len;

    const char *will_topic;
    size_t will_topic_len;
    const void *will_payload;
    size_t will_payload_len;

    const char *username;
    size_t username_len;
    const char *password;
    size_t password_len;

    uint8_t proto_level;
    uint8_t clean_session;
    uint8_t has_will;
    uint8_t has_username;
    uint8_t has_password;
    uint16_t keep_alive;
} mqtt_connect_t;

// ==== PUBLISH ====
typedef struct {
    const char *topic;
    size_t topic_len;
    const uint8_t *payload;
    size_t payload_len;
    uint8_t qos;
    uint8_t retain;
    uint8_t dup;
    uint16_t packet_id; // qos > 0 时有效
} mqtt_publish_t;

int mqtt_decode_fixed_header(const uint8_t *buf, size_t len,
                             mqtt_fixed_header_t *hdr);

int mqtt_decode_connect(const uint8_t *body, size_t body_len,
                        mqtt_connect_t *out);

// 返回 1 成功, 0 数据不够, -1 格式错误
int mqtt_decode_publish(const uint8_t *body, size_t body_len, uint8_t flags,
                        mqtt_publish_t *out);

// 编码 QoS 0 发送一条 PUBLISH 给某个连接
// 返回0 成功（已入发送缓冲），-1 失败
int mqtt_send_publish_qos0(connection_t *c, const char *topic, size_t topic_len,
                           const uint8_t *payload, size_t payload_len,
                           uint8_t retain, uint8_t dup);

// 编码 QoS 1 发送一条 PUBLISH，packet_id 由调用方分配并负责 inflight 登记
int mqtt_send_publish_qos1(connection_t *c, const char *topic, size_t topic_len,
                           const uint8_t *payload, size_t payload_len,
                           uint8_t retain, uint8_t dup, uint16_t packet_id);

// 扫描所有连接，把超时未收到 PUBACK 的 QoS1 消息以 DUP=1 重发
void mqtt_retransmit_all(uint64_t now_ms);

// ==== SUBSCRIBE / SUBACK ====

// SUBSCRIBE 固定头必须是 0x02 
#define MQTT_SUBSCRIBE_FLAGS 0x02
// UNSUBSCRIBE 固定头必须是 0x02
#define MQTT_UNSUBSCRIBE_FLAGS 0x02
// SUBACK 失败返回码
#define MQTT_SUBACK_FAILURE 0x80


int mqtt_process(connection_t *c);

#endif
