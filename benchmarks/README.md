# 消息链路与并发测试工具

## 2026-10-19 增补

第二轮有界微批的源码与边界见 [微批设计](../docs/bounded-message-batching.md)，项目调研见 [机制选择](../docs/performance-research.md)。`batch_analyze.py` 分开汇总 HTTP/ACK、SQL 采样速率、ready 队列和批回调计数，支持从原始样本合并重复轮次的分位数：

```sh
python benchmarks/batch_analyze.py path/to/second-round/raw
```

批回调耗时不含前序排队，ready 不含未确认在途投递；都不能直接当作逐消息持久化延迟。新的实验名称使用 `p2-*`，旧轮次的数据不覆盖。

可靠性修复和存储分片设计见 [设计说明](../docs/reliability-performance.md)。新增工具：

- `optimization_probe.py`：并发注册、ES 故障补偿相关 API、并发资料更新、高 FD 下 HTTP/WS 共存。
- `optimization_analyze.py`：读取基线/单 worker/四分片原始结果，分开统计队列、持久化和接口延迟；50/s 三轮分位数从原始样本合并计算。
- `es_transport_probe.cc`：独立 ES 请求耗时探针。
- `es_bulk_probe.py`：ES 单层逐条/Bulk 方向验证，**不代表 IM 吞吐，也不是已接入业务的批量持久化实现**。

新隔离环境使用 `soarim-opt-*` 容器及其旧 `soarim-bench-*` DNS 别名，原压测器可保持不变；资源汇总支持两套名字。宿主机编排脚本与操作记录在本轮报告包中，依赖已经搭建的隔离环境，并不是可直接执行到任意服务器的部署脚本。

任何重跑都应使用新的 case 名，保留初次核验与最终审计，不覆盖失败记录。生成的账号、令牌与中间件密码仍只能留在 `/bench/private/`，不应进入报告或 Git。

这组工具为提交 `748dd8ea9c1b8de99a7ed9e90595af09bc5c9b4e` 的隔离测试编写，不自动修改业务代码或提交到 GitHub。

## 范围

- `load.py`：通过真实 HTTP 注册测试账号；使用真实 WebSocket 认证、收消息和发送 ACK。固定节拍发送，超时不无限堆积补发任务，并记录实际尝试数与目标数。
- `concurrency_probe.cc`：12 线程连接替换/注销与回执追踪测试；重复 Channel 注册探测。`COMPONENTS_ONLY` 用于隔离核心路径的 ThreadSanitizer 检查，不覆盖 brpc 初始化或全部服务端。
- `discovery_probe.cc`：真实 etcd Watch/租约与两个 brpc 测试节点，测试摘除、无节点、恢复和重复 PUT。
- `registration_probe.py` / `registration_audit.py`：并发注册结果及 MySQL、ES 一致性核对。
- `reliability_probe.py`：重试、伪造/延迟 ACK、离线历史、跨发送者幂等键作用域和非法输入。
- `queue_snapshot.py` / `verify_es.py`：只读队列采样与逐消息 ES `_mget` 校验。
- `analyze.py`：从原始数据生成 `analysis.json` 和 `performance.csv`。

## 运行约束

负载脚本依赖 Python 3.11、aiohttp、Protobuf、requests、pika；`PYTHONPATH` 需要包含服务端 protoc 生成的 Python 模块。它们使用专用容器 DNS `soarim-bench-*` 和 `/bench` 挂载路径，不能直接指向演示或业务环境。

`/bench/private/accounts.json`、`credentials.json` 等只用于测试，不应分享或提交；原始 `results/` 不记录登录令牌与数据库凭据。会话测试集通过测试环境的 SQL 预置，账号注册、认证与消息发送仍走实际服务。建库脚本中有 DROP TABLE，因此只能挂载到全新的隔离 MySQL，不能用于已有业务数据库。

压测器与整套后端分别绑定 CPU；各容器的内存上限、版本和资源限制必须记录。消息正文为 256 B 可压缩文本，不能外推到音频/大文件、TLS、跨地域或弱网场景。

## 数据解释

1. HTTP 成功仅代表转发服务接受请求；接收端 ACK 和最终落库需要另查。
2. 五秒突发不等于持续容量；即使 HTTP/ACK 全成功，只要 MQ 积压持续增长，也不能称为全链路稳定吞吐。
3. HTTP 与 ACK 使用同一个压测进程的单调时钟分别计时。ACK 可以先于 HTTP 响应回调完成。
4. CPU 的 100% 为一个逻辑核；内存为 Docker stats 工作集口径，非应用堆内存。
5. 测试未检测到竞态不等于证明所有路径无竞态。第三方库告警与覆盖边界需单独保留。

分析已有结果：

```sh
python benchmarks/analyze.py output/benchmark-20261018/raw
```

报告与原始结果需要一起查看，尤其是被排除的压测器连接池耗尽试验，不应把它算成服务端错误。
