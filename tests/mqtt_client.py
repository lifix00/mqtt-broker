"""最小 MQTT 3.1.1 客户端封装，仅用于测试。"""
import socket
import struct
from enum import IntEnum

# -------- 异常 -------
class MQTTProtocolError(Exception):
    """协议层错误（非法报文 / 非预期包类型）"""

# ------- 包类型 --------
class PacketType(IntEnum):
    CONNECT = 1
    CONNACK = 2
    PUBLISH = 3
    PUBACK = 4
    PUBREC = 5
    PUBREL = 6
    PUBCOMP = 7
    SUBSCRIBE = 8
    SUBACK = 9
    UNSUBSCRIBE = 10
    UNSUBACK = 11
    PINGREQ = 12
    PINGRESP = 13
    DISCONNECT = 14

    @classmethod
    def from_first_byte(cls, b: int) -> "PacketType":
        return cls((b >> 4) & 0x0F)

# 解析固定头
ptype = PacketType.from_first_byte(0x30)   # PacketType.PUBLISH
if ptype is PacketType.PUBLISH:
    ...
# ===== 变长剩余长度编解码 ====

MAX_REMAINING_LENGTH = 0x0FFFFFFF

def encode_remaining_length(n: int) -> bytes:
    if n < 0 or n > MAX_REMAINING_LENGTH:
        raise ValueError(f"remaining length out of range: {n}")
    out = bytearray()
    while True:
        b = n % 128
        n //= 128
        if n > 0:
            b |= 0x80
        out.append(b)
        if n == 0:
            break
    return bytes(out)


def decode_remaining_length(buf: bytes, pos: int = 1):
    """返回(value, consumed_bytes); 数据不够返回(None, 0)"""
    multiplier = 1
    value = 0
    i = pos
    while i < len(buf):
        b = buf[i]
        value += (b & 0x7F) * multiplier
        if (b & 0x80) == 0:
            return value, (i - pos + 1)
        multiplier *= 128
        i += 1
        if i - pos >= 4:
            raise ValueError("remaining length > 4 byte")
    return None, 0


# ======== 客户端 ========
class MQTTClient:
    def __init__(
        self,
        host="127.0.0.1",
        port=1883,
        client_id="test_client",
        clean_session=True,
        keep_alive=60,
        timeout=2.0,
    ):
        if not clean_session and not client_id:
            raise ValueError("client_id must be non-empty when clean_session=False")
        self.sock = socket.create_connection((host, port), timeout=timeout)
        self.sock.settimeout(timeout)
        self.client_id = client_id
        self.clean_session = clean_session
        self.keep_alive = keep_alive
        self._next_mid = 1  #  Message Identifier
        self._rbuf = b""
        self._pending = []  # [FIX] 缓存非预期包，供后续读取

    # ---- 内部工具 ----
    def _alloc_mid(self) -> int:
        # packet id 在 1 ~ 65535 之间回绕
        mid = self._next_mid
        self._next_mid = self._next_mid % 65535 + 1
        return mid

    @classmethod
    def _require(cls, cond, msg):
        if not cond:
            raise MQTTProtocolError(msg)

    # ---- CONNECT / CONNACK ----
    def connect(self):
        self.sock.sendall(self._build_connect())
        session_present, code = self.read_connack()
        if code != 0:
            # 非 0 返回码时关闭连接，避免留下半死 socket
            self.close()
            raise MQTTProtocolError(f"CONNACK refused, return code={code}")
        return session_present, code

    def _build_connect(self) -> bytes:
        payload = bytearray()
        payload += b"\x00\x04MQTT"  # protocol name
        payload += b"\x04"  # protocol level
        flags = 0
        if self.clean_session:
            flags |= 0x02
        payload += bytes([flags])
        payload += struct.pack(">H", self.keep_alive)
        cid = self.client_id.encode()
        payload += struct.pack(">H", len(cid)) + cid

        fixed = b"\x10" + encode_remaining_length(len(payload)) + bytes(payload)

        return fixed

    def read_connack(self):
        pkt = self._read_one_packet()
        self._require(pkt[0] == 0x20, f"expect CONNACK, got 0x{pkt[0]:02X}")
        rl, consumed = decode_remaining_length(pkt, 1)
        self._require(rl == 2, f"CONNACK remaining length != 2: {rl}")
        body = pkt[1 + consumed : 1 + consumed + rl]
        session_present = body[0] & 0x01
        return_code = body[1]
        return session_present, return_code

    # ---- SUBSCRIBE / SUBACK ----
    def subscribe(self, topic_filter: str, qos: int = 0) -> tuple:
        self._require(qos in (0, 1, 2), f"invalid qos: {qos}")
        mid = self._alloc_mid()

        payload = bytearray()
        payload += struct.pack(">H", mid)
        tf = topic_filter.encode()
        payload += struct.pack(">H", len(tf)) + tf
        payload += bytes([qos])

        fixed = b"\x82" + encode_remaining_length(len(payload)) + bytes(payload)
        self.sock.sendall(fixed)

        # 读SUBACK
        pkt = self._read_one_packet_until(0x90)  # 容忍中间夹杂其他包
        rl, consumed = decode_remaining_length(pkt, 1)
        body = pkt[1 + consumed : 1 + consumed + rl]
        rmid = struct.unpack(">H", body[0:2])[0]
        self._require(rmid == mid, f"SUBACK mid mismatch: {rmid} != {mid}")
        return_codes = tuple(body[2:])
        return return_codes

    # ---- UNSUBSCRIBE / UNSUBACK ----
    def unsubscribe(self, topic_filter: str) -> int:
        mid = self._alloc_mid()

        payload = bytearray()
        payload += struct.pack(">H", mid)
        tf = topic_filter.encode()
        payload += struct.pack(">H", len(tf)) + tf

        fixed = bytes([0xA2]) + encode_remaining_length(len(payload)) + bytes(payload)
        self.sock.sendall(fixed)

        pkt = self._read_one_packet_until(0xB0)
        rl, consumed = decode_remaining_length(pkt, 1)
        body = pkt[1 + consumed : 1 + consumed + rl]
        rmid = struct.unpack(">H", body[:2])[0]
        self._require(rmid == mid, f"UNSUBACK mid mismatch: {rmid} != {mid}")
        return rmid

    # --- PUBLISH ----
    def publish(
        self,
        topic: str,
        payload: bytes,
        qos: int = 0,
        packet_id: int | None = None,
        retain: bool = False,
        dup: bool = False,
    ) -> int | None:
        """内部通用 publish. 返回 packet_id (qos>0), qos=0 返回None"""
        if not topic:
            raise ValueError("topic must be non-empty")
        if "+" in topic or "#" in topic:
            raise ValueError("PUBLISH topic must not contain wildcards")
        if qos not in (0, 1, 2):
            raise ValueError(f"invalid qos: {qos}")
        if qos == 0 and dup:
            raise ValueError("QoS 0 cannot set DUP")

        t = topic.encode()

        if qos > 0:
            if packet_id is None:
                packet_id = self._alloc_mid()
            if not (1 <= packet_id <= 65535):
                raise ValueError(f"packet_id out of range: {packet_id}")

        body = bytearray()
        body += struct.pack(">H", len(t)) + t

        if qos > 0:
            body += struct.pack(">H", packet_id)
        body += payload

        flags = ((1 if dup else 0) << 3) | (qos << 1) | (1 if retain else 0)
        fixed = bytes([0x30 | flags]) + encode_remaining_length(len(body)) + bytes(body)
        self.sock.sendall(fixed)
        return packet_id

    def publish_qos0(self, topic: str, payload: bytes, retain: bool = False):
        """QoS 0 发布便捷方法（不分配 packet id）。"""
        return self.publish(topic, payload, qos=0, retain=retain)

    def expect_puback(self, packet_id: int | None = None, timeout=None):
        """等待 PUBACK；如果指定了 packet_id 则校验。返回 packet_id。"""
        ptype, _flags, body = self.recv_packet_of_type(PacketType.PUBACK, timeout=timeout)
        if len(body) != 2:
            raise MQTTProtocolError(f"PUBACK body len {len(body)} != 2")
        got_id = struct.unpack(">H", body)[0]
        if packet_id is not None and got_id != packet_id:
            raise MQTTProtocolError(f"PUBACK id {got_id} != expected {packet_id}")
        return got_id

    def send_puback(self, packet_id: int):
        """发送 PUBACK（2 字节固定头 + 2 字节 packet_id）"""
        if not (1 <= packet_id <= 65535):
            raise ValueError(f"packet_id out of range: {packet_id}")
        pkt = b"\x40\x02" + struct.pack(">H", packet_id)
        self.sock.sendall(pkt)

    def send_pubrec(self, packet_id: int):
        pkt = b"\x50\x02" + struct.pack(">H", packet_id)
        self.sock.sendall(pkt)

    def send_pubcomp(self, packet_id: int):
        pkt = b"\x70\x02" + struct.pack(">H", packet_id)
        self.sock.sendall(pkt)

    # ---- 接收 PUBLISH ----
    def recv_publish(self, timeout=None):
        """返回 (topic, payload, qos, retain, packet_id)。QoS 0 时 packet_id 为 None。
        不自动回 PUBACK——调用者决定。
        """
        topic, payload, qos, retain, packet_id, _dup = self.recv_publish_ex(timeout)
        return topic, payload, qos, retain, packet_id

    def recv_publish_ex(self, timeout=None):
        """同 recv_publish，但额外返回 DUP 标志：
        (topic, payload, qos, retain, packet_id, dup)
        """
        old = self.sock.gettimeout()
        try:
            if timeout is not None:
                self.sock.settimeout(timeout)
            pkt = self._read_one_packet_until(0x30)
        finally:
            self.sock.settimeout(old)

        flags = pkt[0] & 0x0F
        qos = (flags >> 1) & 0x03
        retain = flags & 0x01
        dup = (flags >> 3) & 0x01
        if qos not in (0, 1, 2):
            raise MQTTProtocolError(f"invalid QoS={qos} in PUBLISH")

        rl, consumed = decode_remaining_length(pkt, 1)
        start = 1 + consumed
        body = pkt[start : start + rl]

        tlen = struct.unpack(">H", body[:2])[0]
        topic = body[2 : 2 + tlen].decode()
        pos = 2 + tlen

        packet_id = None
        if qos > 0:
            packet_id = struct.unpack(">H", body[pos : pos + 2])[0]
            pos += 2

        payload = body[pos:]
        return topic, payload, qos, retain, packet_id, dup

    def try_recv_publish(self, timeout=0.5):
        """尝试接受一条 PUBLISH 超时返回 None"""
        try:
            return self.recv_publish(timeout)
        except (TimeoutError, socket.timeout):
            return None

    # ---- PING / DISCONNECT ----
    def ping(self):
        self.sock.sendall(b"\xc0\x00")
        # 循环读，直到拿到 PINGRESP；其他包缓存进 _pending
        pkt = self._read_one_packet_until(0xD0)
        self._require(pkt[1] == 0x00, "PINGRESP remaining length != 0")

    def disconnect(self):
        try:
            self.sock.sendall(b"\xe0\x00")
        except OSError:
            pass
        self.close()  # DISCONNECT 后直接关闭

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass

    def recv_packet_of_type(self, expected_packet_type: int, timeout=None):
        """接收任意指定类型的报文。返回 (type, flags, body)。"""
        old = self.sock.gettimeout()
        try:
            if timeout is not None:
                self.sock.settimeout(timeout)
            pkt = self._read_one_packet_until(expected_packet_type << 4)
        finally:
            self.sock.settimeout(old)
    
        rl, consumed = decode_remaining_length(pkt, 1)
        start = 1 + consumed
        body = pkt[start : start + rl]
        return pkt[0] >> 4, pkt[0] & 0x0F, body

    # --- 底层读取函数 ----

    def _read_one_packet(self) -> bytes:
        """阻塞直到收到一条完整的报文，返回原始字节"""
        while True:
            if len(self._rbuf) >= 2:
                rl, consumed = decode_remaining_length(self._rbuf, 1)
                if rl is not None:
                    total = 1 + rl + consumed
                    if len(self._rbuf) >= total:
                        pkt = self._rbuf[:total]
                        self._rbuf = self._rbuf[total:]
                        return pkt
            # 继续读
            chunk = self.sock.recv(4096)
            if not chunk:
                raise ConnectionError("connection closed by broker")
            self._rbuf += chunk

    def _read_one_packet_until(self, expected_first_byte: int) -> bytes:
        """读取直到拿到首字节匹配的包，沿途不匹配的包存入 _pending

        注意：当前实现只把不匹配的包暂存，后续若调用 recv_publish 等，
        需要先从 _pending 里取。为此在 _read_one_packet 前加一层 peek。
        """
        # 检查 pending
        mask = 0xF0
        for i, pkt in enumerate(self._pending):
            if (pkt[0] & mask) == (expected_first_byte & mask):
                self._pending.pop(i)
                return pkt
        while True:
            pkt = self._read_one_packet()
            if (pkt[0] & mask) == (expected_first_byte & mask):
                return pkt
            self._pending.append(pkt)
