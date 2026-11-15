"""TLS：OpenSSL 加密链路与 TLS-only 监听。"""

import shutil
import subprocess
import time

import pytest

from mqtt_client import MQTTClient, MQTTProtocolError


@pytest.fixture(scope="module")
def tls_cert(tmp_path_factory):
    if shutil.which("openssl") is None:
        pytest.skip("openssl CLI not available")

    d = tmp_path_factory.mktemp("tls")
    cert = d / "cert.pem"
    key = d / "key.pem"
    subprocess.run(
        [
            "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
            "-keyout", str(key), "-out", str(cert), "-days", "1",
            "-subj", "/CN=localhost",
        ],
        check=True,
        capture_output=True,
    )
    return cert, key


def _tls_broker(broker_factory, tls_cert):
    cert, key = tls_cert
    return broker_factory(
        "--tls-cert", str(cert), "--tls-key", str(key)
    )


def test_tls_pubsub_with_large_payload(tls_cert, broker_factory):
    port, _ = _tls_broker(broker_factory, tls_cert)

    sub = MQTTClient(
        port=port, client_id="tls-sub", tls=True, timeout=10
    )
    pub = MQTTClient(
        port=port, client_id="tls-pub", tls=True, timeout=10
    )
    try:
        sub.connect()
        pub.connect()
        assert sub.subscribe("tls/#", qos=1) == (1,)

        payload = bytes((i * 13) % 256 for i in range(32 * 1024))
        pid = pub.publish("tls/x", payload, qos=1)
        pub.expect_puback(pid, timeout=5)

        topic, got, qos, retain, packet_id = sub.recv_publish(timeout=5)
        assert (topic, qos, retain) == ("tls/x", 1, 0)
        assert got == payload
        sub.send_puback(packet_id)
    finally:
        sub.close()
        pub.close()


def test_tls_rejects_plaintext_client(tls_cert, broker_factory):
    port, _ = _tls_broker(broker_factory, tls_cert)

    plain = MQTTClient(port=port, client_id="plain", tls=False, timeout=1)
    try:
        with pytest.raises(
            (ConnectionError, TimeoutError, OSError, MQTTProtocolError)
        ):
            plain.connect()
    finally:
        plain.close()


def test_tls_keepalive_ping(tls_cert, broker_factory):
    port, _ = _tls_broker(broker_factory, tls_cert)

    c = MQTTClient(port=port, client_id="tls-ping", tls=True, timeout=5)
    try:
        c.connect()
        for _ in range(3):
            c.ping()
            time.sleep(0.05)
    finally:
        c.close()
