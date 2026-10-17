# 消息持久化与 MQ 确认回归测试

这些测试可以单独构建，不需要启动 brpc、MySQL、Elasticsearch 或文件服务。
持久化测试对存储依赖注入故障；MQ 集成测试使用真实 RabbitMQ 与生产 `MQClient`。
它们不替代整套微服务联调。

## 环境与构建

需要 C++17 编译器、CMake、Protobuf 编译器和开发库、GoogleTest、spdlog/fmt、
AMQP-CPP（含 TCP 支持）、libev、OpenSSL，以及 Python 3。
运行真实 broker 测试还需要 Python `pika`、`protobuf` 运行库和隔离的本机 RabbitMQ。
在 Debian 中对应的 Python 包为 `python3-pika` 与 `python3-protobuf`。

例如在已安装上述依赖的 Linux 环境中，从仓库根目录运行：

```bash
cmake -S server/tests -B build/mq-tests -DPython3_EXECUTABLE=/usr/bin/python3
cmake --build build/mq-tests -j2
ctest --test-dir build/mq-tests --output-on-failure
```

默认只运行不访问外部服务的持久化测试。

## 真实 RabbitMQ 回归

仅在隔离的测试环境运行：脚本连接 `127.0.0.1:5672`，使用本机测试账号
`guest/guest`，创建以 `chatsystem-ack-test-` 开头的唯一测试队列与死信交换机，
测试结束后删除这些测试资源。不要指向业务实例。

```bash
cmake -S server/tests -B build/mq-tests \
  -DPython3_EXECUTABLE=/usr/bin/python3 -DMQ_BROKER_TESTS=ON
cmake --build build/mq-tests -j2
ctest --test-dir build/mq-tests --output-on-failure
```

覆盖成功与重复消费、ES/MySQL/文件服务临时失败后的重投、异常隔离、
毒消息死信处理、故障期间的投递背压，以及等待重投时退出后的消息恢复。
存储依赖仍为可控的故障注入，broker、确认帧和重新投递是真实行为。

## 与旧 MQ 实现对照

可将修复前的 `rabbitmq.hpp` 放在独立目录，并用相同集成测试验证旧实现：

```bash
cmake -S server/tests -B build/mq-baseline \
  -DPython3_EXECUTABLE=/usr/bin/python3 \
  -DMQ_BASELINE_INCLUDE_DIR=/absolute/path/to/old/common \
  -DMQ_BROKER_TESTS=ON
cmake --build build/mq-baseline -j2
ctest --test-dir build/mq-baseline -R mq_delivery --output-on-failure
```

旧实现会忽略回调返回的失败结果并 ACK，因此重投和死信断言应失败。
对照选项只替换 MQ 封装，不替换测试中的持久化组件。
