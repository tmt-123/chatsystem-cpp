# 消息链路修复与性能对照（2026-10-19）

## 测试条件

- Ubuntu 20.04 / Linux 5.15，双路 Xeon Silver 4310，24 物理核、48 逻辑 CPU，约 125.5 GiB 内存，共享主机。
- 7 个服务与 5 个中间件共同绑定 CPU 0–3；压测端绑定 CPU 4–7。每服务单实例，RPC threads=1，MySQL pool=4。
- MySQL 8.0.39、Redis 7.4、RabbitMQ 3.10、etcd 3.5.17、Elasticsearch 7.17.21（JVM 512 MiB，standard analyzer）。
- 同机 Docker bridge，无 TLS，256 B 可压缩文本；每发送用户每秒发送一条。
- MySQL 位于 SSD，Docker/ES 数据位于机械硬盘。MySQL `innodb_flush_log_at_trx_commit=1`、`sync_binlog=1`，ES translog=`REQUEST`，未降低持久化设置。

基线程序来自 `748dd8ea9c1b8de99a7ed9e90595af09bc5c9b4e`。最终版本包含本次修复、4 个会话哈希工作分片和 prefetch=16；中间对照采用相同修复，但仅启用 1 个存储 worker。

HTTP 从 POST 前计时至完整响应解析；送达 ACK 从同一起点计至发送端收到接收方回执，可能早于 HTTP 回调。分位数只包含成功请求，失败单列。落库速度为采样区间 SQL 增量，不是 HTTP 吞吐。

## 50 条/秒的对照

每轮预热 30 秒、正式测量 60 秒；最终版本执行三轮，分位数从 9000 条正式样本合并计算。

| 指标 | 原代码 | 修复后 1 worker | 修复后 4 workers |
|---|---:|---:|---:|
| 正式 HTTP 成功/尝试 | 3000/3000 | 3000/3000 | 9000/9000 |
| 正式送达 ACK | 3000 | 3000 | 9000 |
| HTTP P95 | 50.98 ms | 6.23 ms | 6.35 ms |
| ACK P95 | 6.27 ms | 6.64 ms | 6.80 ms |
| SQL 采样写入速率 | 17.56 条/秒 | 18.11 条/秒 | 约 50 条/秒 |
| 测量窗口 ready 队列峰值 | 2605 | 2559 | 25 / 59 / 72 |
| 负载结束后的存储核验等待 | 首次 181 秒仍未全部落库 | 159.89 秒 | 每轮约 0.29–0.30 秒 |

最终独立审计中，各轮含预热消息均完整存在于 SQL 和 ES。原代码 50/s 轮次也最终补齐，不能把首次核验超时称作永久丢失。

HTTP P95 约降低 87.6%，但 ACK P95 没有改善；不能把前者写成“消息送达延迟降低 87.6%”。单 worker 对照表明，仅降低 HTTP 延迟并没有解决存储积压。

## 持续负载与突发的边界

| 场景 | 结果 | 存储状态 |
|---|---|---|
| 50/s，正式测量 300 秒 | 15000/15000 HTTP、ACK 成功；HTTP P95 6.33 ms，ACK P95 6.91 ms | 含预热 16500 条最终 SQL/ES 一致；采样写入 49.91/s；ready 峰 216、末尾 86；结束后核验等待 3.27 秒 |
| 100/s，正式测量 60 秒 | 6000/6000 HTTP、ACK 成功 | 含预热 9000 条最终一致，但采样写入约 59.22/s、测量窗口 ready 峰 3277；结束后约 68.16 秒才完成核验，不是稳定容量 |
| 500/s，5 秒突发，三轮 | 实际尝试 2500、2499、2499，共 7498 条；HTTP、ACK 全成功，最终 SQL/ES 无缺失 | 结束后仍需约 39–43 秒核验，不能宣传稳定 500 QPS |
| 原代码 500/s，5 秒突发 | 实际 2491 次尝试，HTTP 成功 175、失败 2316 | 最终仅 172 条 SQL/ES；3 条已接受消息缺失的原因尚未独立证明 |

原代码突发失败主要为连接断开。最终版本有限场景通过，不代表已证明所有故障下绝不丢消息。

## 可靠性回归

- 5 组 GTest 共 44 项通过：持久化 22、回执 5、Redis 会话 6、ES 7、服务发现 4。
- 真实 RabbitMQ 回归 14 项通过：逐 tag 重试、prefetch=16 窗口、停机重投、800 条并发发布、160 条分片并行/FIFO 等。
- Gateway 回执集成测试通过，包含 15 项检查。
- 并发注册 C8，三轮共 72/72 成功，SQL/ES 逐用户对账一致。
- ES 暂停时 16/16 注册成功，16 个 outbox 任务保留；停止用户服务，恢复 ES 并重启用户服务后约 1.38 秒核验清空。
- 同发送者重试只落一条；不同发送者使用相同 client_message_id，分别生成服务端 ID 并落两条。
- Message/ES 分别暂停约 6 秒，每场景 50 条消息最终 HTTP、ACK、SQL、ES 一致；历史查询恢复无需手动重启消息服务。
- 1000 个 WebSocket、最大 FD 1527 时，HTTP 请求 200/200 成功。

## 复现入口与限制

测试源码位于 [server/tests](../server/tests)，负载与核验入口见 [benchmarks/README.md](../benchmarks/README.md)，设计和迁移见 [reliability-performance.md](reliability-performance.md)。集成测试需要隔离的 Redis、ES、etcd 和 RabbitMQ，不能连接演示或业务实例。

在具备依赖且已配置隔离中间件地址的 Linux 环境中：

```sh
cmake -S server/tests -B build/tests -DRUNTIME_INTEGRATION_TESTS=ON -DMQ_BROKER_TESTS=ON
cmake --build build/tests -j4
ctest --test-dir build/tests --output-on-failure
```

RabbitMQ 测试使用专用 localhost guest 实例；Redis、ES、etcd 分别使用 `TEST_REDIS_HOST`（固定 6379 端口）、`TEST_ES_URL`、`TEST_ETCD_URL` 指定隔离实例。网关测试需另外构建网关测试目标。完整系统压测还依赖独立数据集、服务部署及 `/bench` 挂载，并非上述命令即可启动整套压测。

原始逐消息样本、最终审计和构建/测试日志保存在本轮独立报告包中；不提交账号、令牌、私有配置、原始运行数据或二进制。这里列出的是该轮实测摘要，而不是在每次 PR 检查中自动生成的成绩。

局限：共享主机、数据库随轮次增长、未逐轮恢复同一快照。新隔离库从开始就使用复合索引，原程序的查询未获得历史全局唯一索引的同等条件，不能把全部差异只归因于 worker 数。磁盘主机级统计包含其他工作负载。未覆盖大文件、弱网、TLS、多网关、broker 断电持久性和全系统竞态证明。

ES Bulk 仅做过单层方向探针，尚未接入业务，不计入上述成绩；后续优化应独立保留本轮对照。
