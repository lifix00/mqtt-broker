# MQTT Broker

C11 + epoll 实现的 MQTT 3.1.1 Broker，支持 QoS 0 / QoS 1、主题通配符和持久会话。

## 构建与运行

```bash
make
./mqtt-broker --port 1883
```

## 测试

```bash
make test
# 或
.venv/bin/python -m pytest tests/ -q
```

## 文档

- [项目进度与计划](docs/PROJECT_STATUS.md)
