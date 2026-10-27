# 性能机制调研与选择（2026-10-19）

## 先定位本项目成本

前一轮单条存储路径在 100/s 输入下积压，CPU 未用满；ES 同步写入和机械盘 fsync 是候选瓶颈。已经分离 MQ I/O 与 worker，本轮不再把“多加线程”当作唯一方案，也不通过关闭持久化或降低密码哈希强度提高成绩。

## 项目和机制对照

| 来源 | 源码/文档中可核对的机制 | 对当前项目的判断 |
|---|---|---|
| [OpenIM Batcher](https://github.com/openimsdk/open-im-server/blob/main/pkg/tools/batcher/batcher.go)、[消息转移](https://github.com/openimsdk/open-im-server/blob/main/internal/msgtransfer/online_history_msg_handler.go) | 数量/时间触发聚合，按 key 分片，有限 worker/channel；处理与消费进度关联 | 借鉴微批和有界调度，不迁移到 Kafka/MongoDB，不照搬 worker/interval 参数 |
| [Apache Kafka Producer](https://kafka.apache.org/41/configuration/producer-configs/) | batch.size、linger.ms、buffer.memory 等分别约束聚合、等待和内存 | 本地加入条数、字节数、最老消息等待时间三个约束；仍保留 RabbitMQ |
| [Centrifuge Config](https://github.com/centrifugal/centrifuge/blob/master/config.go) | ClientQueueMaxSize 限制慢连接队列，历史/恢复数量限制，重复只读查询可 singleflight | 慢客户端隔离和查询合并是后续方向；当前不是已证实的吞吐瓶颈，暂不改在线协议 |
| [GoIM Channel](https://github.com/Terry-Mao/goim/blob/master/internal/comet/channel.go)、[Room](https://github.com/Terry-Mao/goim/blob/master/internal/comet/room.go) | 每连接有限 signal 队列，Push 满队列直接返回错误；Room 扇出遍历连接 | 可借鉴慢连接隔离，但源码中 Room 忽略 Push 错误的丢弃策略不能照搬到可靠聊天；需要明确失败回执与离线补偿 |
| [uWebSockets App](https://github.com/uNetworking/uWebSockets/blob/master/src/App.h) | maxPayloadLength、maxBackpressure、drain/dropped 回调及事件循环内的发布/缓冲 | 借鉴资源上限与背压；不能用框架自身的基准宣称本项目成绩，也不直接整体换网关 |
| [Drogon 线程模型](https://github.com/drogonframework/drogon/wiki/ENG-FAQ-1-Understanding-drogon-threading-model) | 事件循环与非阻塞处理模型，避免在 loop 中运行阻塞任务 | 上轮已经在 AMQP/libev 内落实 I/O/worker 分离；本轮保留这个线程亲和边界 |
| [WuKongIM](https://github.com/WuKongIM/WuKongIM) | 面向 channel 的消息、存储、同步架构，可作为会话维度设计参考 | 其集群存储架构与本项目不同；不拿宣传吞吐做横向对比，不为了指标整体重构 |
| [Elasticsearch 7.17 Bulk](https://www.elastic.co/guide/en/elasticsearch/reference/7.17/docs-bulk.html)、[索引性能](https://www.elastic.co/guide/en/elasticsearch/reference/7.17/tune-for-indexing-speed.html) | Bulk 减少单请求成本，响应逐 item 表达成败；并发与 batch 大小需实测，429 表示过载 | 接入文本 Bulk，稳定文档 ID，逐项判定，成功写 ES 后才允许进入 SQL；不设 refresh=true，不降低 translog 持久性 |
| [RabbitMQ Prefetch](https://www.rabbitmq.com/docs/consumer-prefetch) | 限制未确认投递数 | batch/prefetch 必须联动测试，仍逐 tag ACK/NACK；较大 prefetch 本身不等于吞吐提升 |

以上是机制选择，不是对这些项目的实机性能排名。浏览 main/master 文档会随时间变化，本轮实现和结果应以本地源码、构建记录及测试条件为准。

## 本轮候选与对照

1. 保留 `persistence_batch_size=1` 的原单条路径作对照。
2. 有界微批 + ES Bulk，SQL 仍逐条事务，区分 ES 聚合收益。
3. 同一微批对 ES 成功项使用 SQL 批事务，区分事务提交成本。
4. 比较 batch、wait、prefetch 参数；保持数据持久性、CPU 配额、负载方式和核验规则一致。无收益或失败的轮次也保留。

文本批处理仅在 ES 对应项成功且 SQL 确认提交后 ACK。SQL 事务失败回滚整批，重投时重新查询发送者作用域幂等键。批内相同幂等键合并，不同发送者不合并；媒体仍走现有单条路径。正常分片 FIFO 不等于重试或多实例全局有序。

本轮已完成构建、故障回归、参数对照及重复长测，见 [实测摘要](batching-results-20261019.md)。单层 Bulk 探针或编译成功本身仍不能替代完整链路证据。
