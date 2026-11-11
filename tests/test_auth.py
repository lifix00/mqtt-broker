"""用户名/密码认证：CONNACK 返回码 4，认证关闭时保持兼容。"""

import pytest

from mqtt_client import MQTTClient, MQTTProtocolError


def test_auth_accepts_correct_credentials(broker_factory):
    port, _ = broker_factory("--username", "alice", "--password", "secret")

    c = MQTTClient(
        port=port, client_id="ok", username="alice", password="secret"
    )
    try:
        _, code = c.connect()
        assert code == 0
    finally:
        c.close()


def test_auth_rejects_wrong_password(broker_factory):
    port, _ = broker_factory("--username", "alice", "--password", "secret")

    c = MQTTClient(
        port=port, client_id="bad", username="alice", password="wrong"
    )
    try:
        with pytest.raises(MQTTProtocolError, match="return code=4"):
            c.connect()
    finally:
        c.close()


def test_auth_rejects_missing_credentials(broker_factory):
    port, _ = broker_factory("--username", "alice", "--password", "secret")

    c = MQTTClient(port=port, client_id="bad")
    try:
        with pytest.raises(MQTTProtocolError, match="return code=4"):
            c.connect()
    finally:
        c.close()


def test_auth_username_only(broker_factory):
    port, _ = broker_factory("--username", "alice")

    c = MQTTClient(port=port, client_id="ok", username="alice")
    try:
        _, code = c.connect()
        assert code == 0
    finally:
        c.close()

    bad = MQTTClient(port=port, client_id="bad")
    try:
        with pytest.raises(MQTTProtocolError, match="return code=4"):
            bad.connect()
    finally:
        bad.close()


def test_password_requires_username_on_client():
    with pytest.raises(ValueError, match="password requires username"):
        MQTTClient(username=None, password="secret")
