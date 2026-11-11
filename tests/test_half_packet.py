"""半包 / 粘包（低级 socket 测试）"""
import socket
import time

from _helpers import RawMQTT

CONNECT_PKT = (
    b"\x10\x0d"
    b"\x00\x04MQTT"
    b"\x04"
    b"\x02"
    b"\x00\x3c"
    b"\x00\x01t"
)

def test_half_packet_connect(broker_port):
    """分两次发 CONNECT，broker 应正确拼装后回 CONNACK。"""
    c = RawMQTT("127.0.0.1", broker_port)
    try:
        c.send(CONNECT_PKT[:5])
        time.sleep(0.2)
        c.send(CONNECT_PKT[5:])
        pkt = c.recv_packet()
        assert pkt[:2] == b"\x20\x02", f"expected CONNACK, got {pkt.hex()}"
    finally:
        c.close()
        

        
def test_sticky_packets(broker_port):
    c = RawMQTT("127.0.0.1", broker_port)
    try:
        c.send(CONNECT_PKT + b"\xc0\x00")
        connack = c.recv_packet()
        assert connack == b"\x20\x02\x00\x00", f"got {connack.hex()}"
        pingresp = c.recv_packet()
        assert pingresp == b"\xd0\x00", f"got {pingresp.hex()}"
    finally:
        c.close()
