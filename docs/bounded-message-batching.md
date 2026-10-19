# 有界文本微批持久化

## 范围

在既有 RabbitMQ → MessageService 路径内部增加微批，不新增中间件，不改变客户端协议和数据库结构。原单条路径可用 `--persistence_batch_size=1` 保留；与前一轮 4 workers 的对照需使用相同硬件和持久化设置。

候选参数：

| 参数 | 当前值/边界 | 含义 |
|---|---|---|
| persistence_workers | 默认 4，1–64 | 会话哈希工作分片，各有独立 ES HTTP 会话 |
| mq_prefetch | 默认 16，1–65535 | 单消费者未确认投递上限；与批大小联动，不是容量指标 |
| persistence_batch_size | 默认 16，1–1024 | 每分片最多聚合条数；1 为原单条路径 |
| persistence_batch_wait_ms | 默认 10，0–1000 | 从当前批最老投递入队时间计算的聚合等待上限 |
| persistence_sql_batch | 默认 true | ES 成功的文本项使用同一 MySQL 事务；false 用于逐条事务对照 |

另外每批正文目标不超过 1 MiB，所有工作队列及在途正文预算仍为 64 MiB。单个超过 1 MiB 的消息单独处理，仍受总预算约束。聚合等待上限不包含前面工作的排队和 ES/MySQL 执行耗时，不是端到端延迟保证。

本轮高负载候选使用显式配置，而不是把参数升大视为所有部署的最优默认值：

```sh
--persistence_workers=4 --mq_prefetch=128 \
--persistence_batch_size=32 --persistence_batch_wait_ms=10 \
--persistence_sql_batch=true
```

相对默认值，这会允许更多未确认消息；ready 队列为 0 也可能有最多 128 条在途投递。单条路径、只改 prefetch、Bulk/逐条 SQL、Bulk/批事务应分别对照，不能将所有收益归因于某一个开关。

## 处理与确认

1. 保持 AMQP Channel 操作只在 libev 线程执行；工作分片按条数、字节或等待期限取批。
2. 校验 Protobuf、消息身份和 SQL 字段容量；非法投递单独 reject。兼容旧消息的空客户端 ID 回退。
3. 按 `(sender_id, client_message_id)` 合并批内重试，检查已提交记录。不同发送者不会合并。
4. 文本以稳定服务端消息 ID 发 ES Bulk。HTTP 成功不代表每项成功，必须逐项核对 status、ID、error 和返回条数；失败项不进入 SQL。
5. ES 成功项进入 SQL 单事务：逐条 INSERT，共享一次 commit，而不是一个 multi-row SQL。事务失败回滚，未提交消息保持 Retry，重投时重新查幂等身份。
6. 媒体仍走原单条文件 RPC 流程；到达媒体操作前先刷新前面的文本段，避免批处理人为跨越媒体屏障。
7. 每个投递返回独立结果，最后由 I/O 线程逐 tag ACK/NACK，不使用累计 ACK。回调抛异常或结果数量不匹配时保留投递重试；停机中已取出但尚未处理的消息不成功确认。

SQL/ES 仍不是分布式事务。SQL 回滚时 ES 可能已有文档，重投按稳定 ID 覆写；提交后 ACK 丢失通过幂等查询收敛。永久性数据库约束错误仍需人工处理或独立死信策略，不能承诺所有合法形状的坏数据会自动跳过。重试、多消费者及跨实例没有全局 Seq 保证。

## 原生可观测性

批模式复用 brpc 内部 `/vars/message_persist_*`，无需引入新监控组件：

- `message_persist_batches` / `message_persist_items`：回调批次和投递次数之和，差值之比可估计平均批大小。
- `message_persist_acks` / `retries` / `rejects`：该批回调给出的结果数，不等于 broker confirm，也不等于业务唯一消息数；重投会重复计数。
- `message_persist_callback_*`：brpc LatencyRecorder 的批回调处理耗时与速率等指标；不包含聚合等待和前序排队，不能替代逐消息持久化延迟。

这些计数仅覆盖批回调；batch_size=1 的原路径不计入上述批指标。通过内部 RPC 监听地址访问即可，不应为查看指标将未鉴权的管理端口暴露到公网。

批模式不再为每条文本输出成功日志，而以计数和批耗时观测为主。原单条 ES 写入仍有逐条 INFO 日志；本轮的 INFO 级别对照因此是整条批处理路径的综合收益，不是隔离日志开销后的纯 ES 接口性能消融。

## 验证

单测覆盖批内重复/跨发送者、部分 ES 失败、SQL 异常、不匹配结果长度、旧消息兼容、媒体屏障与字段容量。真实 RabbitMQ 测试覆盖低流量超时、满批、字节边界、混合结果的逐条 ACK、回调异常与停机重投；真实 ES 覆盖 Unicode/换行和 HTTP 200 中的部分失败。

完整性能对照必须核对 HTTP、接收端 ACK、队列趋势、SQL 唯一记录及 ES 内容。原始结果与失败日志保留，不以编译成功、微基准或发送结束后的补写宣称持续容量。本轮已完成的参数选择和验证数字见 [实测摘要](batching-results-20261019.md)。
