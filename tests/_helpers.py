"""测试辅助：裸 socket 的 MQTT 流读取器（保留跨调用残留缓冲）"""

import socket
from mqtt_client import decode_remaining_length


class RawMQTT:
    """管理一个裸 socket，按 MQTT 报文边界读数据，跨调用保留残留。"""

    def __init__(self, host: str, port: int, timeout: float = 2.0):
        self.sock = socket.create_connection((host, port), timeout=timeout)
        self.sock.settimeout(timeout)
        self._rbuf = b""

    def send(self, data: bytes):
        self.sock.sendall(data)

    def recv_packet(self) -> bytes:
        """读一条完整报文，返回原始字节（含固定头）。"""
        while True:
            if len(self._rbuf) >= 2:
                rl, consumed = decode_remaining_length(self._rbuf, 1)
                if rl is not None:
                    total = 1 + consumed + rl
                    if len(self._rbuf) >= total:
                        pkt = self._rbuf[:total]
                        self._rbuf = self._rbuf[total:]
                        return pkt
            chunk = self.sock.recv(4096)
            if not chunk:
                raise ConnectionError("connection closed by broker")
            self._rbuf += chunk

    def expect_close(self, timeout: float = 2.0):
        """断言 broker 主动关闭（读到 EOF）。"""
        old = self.sock.gettimeout()
        try:
            self.sock.settimeout(timeout)
            if self._rbuf:
                # 还有未读报文，说明 broker 先发了东西再关——先消费掉再看
                self._rbuf = b""
            data = self.sock.recv(16)
            assert data == b"", f"expected EOF, got {data.hex()}"
        finally:
            self.sock.settimeout(old)

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass
