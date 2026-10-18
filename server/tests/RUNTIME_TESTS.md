# 运行可靠性回归

## Qt 6 客户端

需要 Qt 6 Widgets、Network、WebSockets、Protobuf、Multimedia、Test 及 C++17 编译器：

```sh
cmake -S client/ChatClient/tests -B build/client-tests -DCMAKE_PREFIX_PATH=<qt-prefix>
cmake --build build/client-tests --config Release
ctest --test-dir build/client-tests -C Release --output-on-failure
```

测试可在无音频设备环境执行：覆盖空设备、空播放、无效存储不生成录音，以及乱序 HTTP/回执、双 ID 去重、历史滞后合并和真实 Qt WebSocket 接收/ACK。硬件录音效果、权限弹窗和声卡热插拔仍需手工检查。

## 服务端组件

沿用消息持久化测试所需的 Protobuf、GTest、spdlog、AMQP-CPP/libev/OpenSSL，新增 Boost.System。集成测试另需 redis++/hiredis、elasticlient/cpr、JsonCpp，以及可访问的 Redis 和 ES 7：

```sh
cmake -S server/tests -B build/server-tests -DRUNTIME_INTEGRATION_TESTS=ON
cmake --build build/server-tests -j
TEST_REDIS_HOST=127.0.0.1 TEST_ES_URL=http://127.0.0.1:9200/ \
  ctest --test-dir build/server-tests --output-on-failure
```

测试创建唯一前缀 Redis 键和临时 ES 索引，并仅删除自身创建的对象；不清空数据库或修改已有索引。覆盖登录原子性、未绑定登录过期、断线重连、旧连接释放、历史永久标记、令牌失效；覆盖真实索引创建、搜索、旧 keyword 子字段、并发初始化及错误 mapping/分词器。

## Gateway HTTP/WebSocket 协议

使用与 Gateway 相同的完整 Linux 构建依赖。Python 需要 `protobuf`、`requests`、`websocket-client`。测试在本机占用 19000–19002 端口；不要与其他测试并发占用这些端口：

```sh
cmake -S server/gateway -B build/gateway-tests \
  -DCMAKE_CXX_STANDARD=17 -DGATEWAY_RUNTIME_TESTS=ON
cmake --build build/gateway-tests --target gateway_runtime_fixture -j
TEST_REDIS_HOST=127.0.0.1 ctest --test-dir build/gateway-tests --output-on-failure
```

使用真实 Gateway、Redis 和 brpc 传输，但下游 TransmitService 是确定性测试替身，不验证 MQ/MySQL 持久化。使用 Redis DB 15 中的唯一测试键，退出时按显式键名清理，不执行 FLUSHDB。

覆盖 HTTP 身份注入、未 ACK 保持 SENT、非接收者/未知消息/发送者伪造 ACK、重复 ACK、群聊唯一计数、离线投递、重连与连接替换、无效认证、未认证 ACK 和非法 protobuf 隔离。
