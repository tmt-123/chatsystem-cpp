# 可靠性修复与持久化并发设计

## 本轮调整

### 消息幂等键必须包含发送者

幂等身份为 `(user_id, client_message_id)`，查询与 MySQL 复合唯一索引使用同一作用域。不同发送者可以独立使用相同的客户端 ID；同一发送者重试仍由唯一约束兜底。服务端消息 ID 的生成规则不变。

### 用户搜索采用事务 outbox

注册、资料更新在同一个 MySQL 事务中写入 `user` 和 `user_search_outbox`。API 成功表示主记录和待索引任务均已提交，不再因为 ES 写失败而返回“注册失败但账号已存在”。搜索是最终一致的，更新不保证立即出现在搜索结果中。

后台按任务 ID 顺序取一批任务，先锁定对应用户行，读取最新已提交资料，写 ES 成功后才删除该任务。失败或进程退出后任务保留。多个索引器对同一用户通过行锁串行，避免旧任务把较新资料覆盖掉。任务采用自增 ID 追加，不在缺失的 outbox 行上做 `SELECT FOR UPDATE` 后插入，避免并发注册的间隙锁死锁。

代价是索引期间持有该用户的行锁，ES 慢请求可能拖慢同一用户的资料更新。后续可考虑带版本的索引写入与租约式任务领取，以缩短数据库锁持有时间；本轮没有声称实现了无阻塞 outbox。

### MQ I/O 与存储工作分离

所有 AMQP Channel 操作通过命令队列回到同一个 libev 线程。该线程负责接收、分派和确认，不直接执行同步 ES/MySQL/文件 RPC。发布端也通过相同入口提交 Channel 操作，避免业务线程和 I/O 线程同时访问 Channel。

消息按会话 ID 哈希分派至有限个存储工作分片，每个分片一个线程、一个独立 ES HTTP 会话，数据库共享连接池。正常处理时同一分片按队列 FIFO 执行，不同会话可并行等待存储 I/O。RPC 搜索使用另一 ES 客户端；共享 elasticlient 客户端的其他入口仍通过互斥保护其可变 CPR Session。

- `--persistence_workers=4`：工作分片数，允许 1–64。
- `--mq_prefetch=16`：整个消费者连接最多未确认消息数，允许 1–65535；默认不使用无限预取。
- 待处理和正在处理的正文合计上限为 64 MiB，超过预算的投递保持未成功确认并延迟重投。
- 成功落库或确认幂等记录已存在后单条 ACK；临时失败按 delivery tag 单独延迟重投；非法消息 reject，可由部署方配置 DLX。
- 停机先结束存储工作，再停止 I/O；未完成投递不发送成功 ACK，连接关闭后交由 broker 重投。

这不是严格的会话 Seq 方案：重试可能与后续消息交错，多消费者实例也没有跨实例的会话分区保证。不能据此宣称分布式全局有序或端到端 exactly-once。

### 网关与注册恢复

Linux 网关启用 cpp-httplib 的 `poll` 路径，避免 `select` 在 socket FD ≥1024 时拒绝处理 HTTP。启用 HTTP `TCP_NODELAY`，减少小 Protobuf 响应受 Nagle/延迟 ACK 交互影响。此处没有把接入层整体替换成 epoll Reactor 框架。

服务节点以 etcd 实例键管理，重复 PUT 不再追加重复 Channel；实例地址更新替换原 Channel，DELETE 按实例键删除。Registry 定期检查租约，失效后重新获取租约并恢复已登记的实例，不需要靠重启恢复注册。

## 升级已有数据库

以下仅是部署说明，本轮测试没有修改演示环境数据库。

1. 备份数据库，在维护窗口停止相关写入服务。
2. 对已经应用 `002_message_idempotency.sql` 的旧库执行一次 `003_sender_scoped_idempotency.sql`。
3. 执行一次 `004_user_search_outbox.sql`，创建待索引任务表并为已有用户回填任务。
4. 部署同时包含新 ODB 映射、用户服务和消息服务的版本，再恢复流量。

全新库的 `server/sql/message.sql`、`user.sql` 已包含新结构，不要重复执行历史迁移。初始化脚本含 DROP TABLE，不能拿它们代替生产升级迁移。

## 验证与边界

`server/tests` 覆盖消息持久化、回执、会话租约、共享 ES 客户端并发、Channel 幂等更新、租约撤销恢复，以及真实 RabbitMQ 的重试、背压、停机重投、多线程发布和分片 FIFO。Redis、ES、etcd、RabbitMQ 测试必须使用隔离实例。

`benchmarks/optimization_probe.py` 和测试编排进一步验证真实 API 注册、ES 故障与进程重启后的 outbox 收敛、高 FD 下 HTTP/WebSocket 共存。性能结果应同时核对 HTTP 成功、接收端 ACK、队列趋势、SQL 唯一记录和 ES 文档，不能仅以响应 P95 或短时突发宣称持续吞吐。

未覆盖的保证包括：broker 断电后的消息持久性、发布者 confirm、broker 重连、ES 消息索引丢失后的全量重建、多网关/多存储实例的路由和顺序、弱网与大文件压力。现有普通队列声明和发布返回值不能被解释成 broker 已持久化确认。

## 参考与下一步

- [OpenIM 的消息转移处理](https://github.com/openimsdk/open-im-server/blob/main/internal/msgtransfer/online_history_msg_handler.go)：按键分片、有限 worker/buffer，以及聚合处理。当前借鉴分片与有界并发原则，没有照搬其 worker 数或 Kafka/MongoDB 架构。
- [OpenIM Batcher](https://github.com/openimsdk/open-im-server/blob/main/pkg/tools/batcher/batcher.go)：数量与时间双阈值聚合。后续可评估 ES Bulk 与 MySQL 批量事务，但需要逐条处理部分失败、幂等和确认，不能仅把一批消息统一 ACK。
- [Drogon 线程模型](https://github.com/drogonframework/drogon/wiki/ENG-FAQ-1-Understanding-drogon-threading-model)：事件循环中避免阻塞 I/O。当前在原有 AMQP-CPP/libev 架构内实现 I/O 与工作线程分离，没有引入 Drogon 依赖。
- [RabbitMQ Consumer Prefetch](https://www.rabbitmq.com/docs/consumer-prefetch)：预取限制未确认投递数。扩大预取必须与逐 tag 确认、失败重试和内存预算一起设计。
- [WuKongIM](https://github.com/WuKongIM/WuKongIM)：可继续参考会话/channel 维度的存储、同步与顺序模型；它的集群与存储实现不同，项目宣传指标不能作为本项目的测试结果。
