"""QoS 1 客户端 API 的编码正确性测试。

大部分用例只验证客户端侧的编码/参数校验，不连 broker；
端到端 PUBACK 测试在文件末尾。
"""
    
import struct
import pytest

from mqtt_client import (
    MQTTClient,
    MQTTProtocolError,
    PacketType,
    encode_remaining_length,
)


# ---------- 参数校验 / 编码（不连 broker） ----------

class FakeSock:
    """记录 sendall 的字节，不实际发。"""

    def __init__(self):
        self.sent = b""

    def sendall(self, data: bytes):
        self.sent += data

    def gettimeout(self):
        return None

    def settimeout(self, t):
        pass

    def close(self):
        pass


def _make_offline_client():
    """不连 broker，只用 _alloc_mid / publish 编码逻辑。"""
    c = MQTTClient.__new__(MQTTClient)
    c.sock = FakeSock()
    c._next_mid = 1
    c._rbuf = b""
    c._pending = []
    return c


def test_publish_qos1_default_alloc_mid():
    c = _make_offline_client()
    pid = c.publish("t", b"hi", qos=1)
    assert pid == 1
    # 再发一条，mid 自增
    pid2 = c.publish("t", b"hi", qos=1)
    assert pid2 == 2


def test_publish_qos1_explicit_packet_id():
    c = _make_offline_client()
    pid = c.publish("t", b"hi", qos=1, packet_id=42)
    assert pid == 42


def test_publish_qos1_bytes_encoding():
    """QoS1 PUBLISH 的完整字节：0x32 <rl> <topic_len> topic <pid> payload"""
    c = _make_offline_client()
    c.publish("a/b", b"xyz", qos=1, packet_id=0x1234)

    # 期望：type=3 flags=0x2(qos1) → 首字节 0x32
    # body = 2 + 3(topic) + 2(pid) + 3(payload) = 10
    expected = (
        b"\x32\x0a"
        b"\x00\x03a/b"
        b"\x12\x34"
        b"xyz"
    )
    assert c.sock.sent == expected


def test_publish_qos0_no_packet_id_in_body():
    c = _make_offline_client()
    c.publish("a", b"x", qos=0)
    # body = 2 + 1 + 1 = 4；首字节 0x30
    assert c.sock.sent == b"\x30\x04\x00\x01a" + b"x"


def test_publish_qos1_with_dup_sets_flag():
    c = _make_offline_client()
    c.publish("t", b"x", qos=1, packet_id=1, dup=True)
    # flags = DUP<<3 | QoS<<1 | RETAIN = 0x8 | 0x2 | 0x0 = 0x0A → 首字节 0x3A
    assert c.sock.sent[0] == 0x3A


def test_publish_qos1_with_retain_sets_flag():
    c = _make_offline_client()
    c.publish("t", b"x", qos=1, packet_id=1, retain=True)
    # flags = 0x2 | 0x1 = 0x03 → 首字节 0x33
    assert c.sock.sent[0] == 0x33


def test_publish_qos1_dup_retain_both():
    c = _make_offline_client()
    c.publish("t", b"x", qos=1, packet_id=1, dup=True, retain=True)
    # flags = 0x8 | 0x2 | 0x1 = 0x0B → 首字节 0x3B
    assert c.sock.sent[0] == 0x3B


def test_publish_qos0_dup_raises():
    c = _make_offline_client()
    with pytest.raises(ValueError, match="QoS 0 cannot set DUP"):
        c.publish("t", b"x", qos=0, dup=True)


def test_publish_packet_id_zero_raises():
    c = _make_offline_client()
    with pytest.raises(ValueError, match="packet_id out of range"):
        c.publish("t", b"x", qos=1, packet_id=0)


def test_publish_packet_id_too_large_raises():
    c = _make_offline_client()
    with pytest.raises(ValueError, match="packet_id out of range"):
        c.publish("t", b"x", qos=1, packet_id=70000)


def test_publish_invalid_qos_raises():
    c = _make_offline_client()
    with pytest.raises(ValueError, match="invalid qos"):
        c.publish("t", b"x", qos=3)


def test_publish_topic_with_wildcard_raises():
    c = _make_offline_client()
    with pytest.raises(ValueError, match="wildcards"):
        c.publish("a/+", b"x", qos=1)
    with pytest.raises(ValueError, match="wildcards"):
        c.publish("a/#", b"x", qos=1)


def test_publish_empty_topic_raises():
    c = _make_offline_client()
    with pytest.raises(ValueError, match="non-empty"):
        c.publish("", b"x", qos=1)


# ---------- 发送控制包（PUBACK/PUBREC/PUBCOMP） ----------

def test_send_puback_bytes():
    c = _make_offline_client()
    c.send_puback(0x1234)
    assert c.sock.sent == b"\x40\x02\x12\x34"


def test_send_puback_zero_raises():
    c = _make_offline_client()
    with pytest.raises(ValueError, match="packet_id out of range"):
        c.send_puback(0)


def test_send_pubrec_bytes():
    c = _make_offline_client()
    c.send_pubrec(0xBEEF)
    assert c.sock.sent == b"\x50\x02\xBE\xEF"


def test_send_pubcomp_bytes():
    c = _make_offline_client()
    c.send_pubcomp(0x0001)
    assert c.sock.sent == b"\x70\x02\x00\x01"


# ---------- 与 broker 的端到端 ----------

def test_publish_qos1_to_broker_gets_puback(broker_port):
    """客户端编码正确，broker 能解析高 packet id 并原样回 PUBACK。"""
    c = MQTTClient(port=broker_port, client_id="q1")
    try:
        c.connect()
        pid = c.publish("test/topic", b"hi", qos=1, packet_id=0x1234)
        assert 1 <= pid <= 65535
        assert c.expect_puback(pid, timeout=2) == pid
    finally:
        c.close()


# ---------- 辅助：PacketType 枚举 ----------

def test_packet_type_values():
    assert PacketType.CONNECT == 1
    assert PacketType.PUBLISH == 3
    assert PacketType.PUBACK == 4
    assert PacketType.PUBREL == 6
    assert PacketType.SUBSCRIBE == 8
    assert PacketType.DISCONNECT == 14


def test_packet_type_from_first_byte():
    assert PacketType.from_first_byte(0x10) == PacketType.CONNECT
    assert PacketType.from_first_byte(0x32) == PacketType.PUBLISH
    assert PacketType.from_first_byte(0x40) == PacketType.PUBACK
    assert PacketType.from_first_byte(0xD0) == PacketType.PINGRESP