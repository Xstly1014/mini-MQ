# 1. 采用全局单 CommitLog 顺序写 + 稀疏 ConsumeQueue 索引，不抄 Pulsar 分层存储

- Date: 2026-10-15 (M0)
- Status: Accepted

## Context

立项时对比了三家主流 MQ 的存储模型：

| 模型 | 代表 | 特点 |
|---|---|---|
| 每分区独立日志 | Kafka | 分区级并行吞吐；分区数增长后文件句柄与随机 I/O 上升 |
| 全局单 CommitLog + 稀疏索引 | RocketMQ | 所有 topic 共享一条顺序写流，消费按索引跳转 |
| 分层存储（段分层 + BookKeeper） | Pulsar | 存算分离，分层卸载；依赖第三个组件 Bookie |

本项目的部署形态是单机 3 节点（多进程）、目标场景是"正确核机制 + 可量化性能"的学习验证系统。
M0 基准（bench-v0，10 万条 × 1KB，单线程）实测：mmap 不 force 315.9 万 msg/s，逐条 force
83 msg/s（fsync-bound），FileChannel+DirectBuffer 63.4 万 msg/s——顺序写 + 页缓存是吞吐底座。

## Decision

抄 RocketMQ：全局单 CommitLog（mmap 预分配段，集群 64 MiB / 单机默认 1 GiB，文件名即起始全局
offset）+ 顺序追加；消费侧由 dispatch 建立稀疏 ConsumeQueue 索引（M2 落地），topic 过滤按索引跳转。

否决的备选：

- **Pulsar 分层存储**：3 节点部署形态下"分层卸载冷段"的收益无法兑现，先引入 BookKeeper
  组件复杂度；复杂度先到、收益后到，裁剪。
- **Kafka 每分区独立日志**：分区级并行度不是本项目的目标场景；全局单文件把"全顺序写"
  做到极致，且写路径只需实现一遍（组提交、崩溃恢复、mmap 管理全部单点）。

## Consequences

- **+** 写路径单一：组提交、CRC 恢复、mmap 段管理只需做一遍，M2/M3 的演进都在这条单流上叠加。
- **+** 全 topic 共享一条顺序写流，mmap 页缓存收益最大化。
- **+** 消费按稀疏索引跳转，读顺序性不受 topic 数量影响。
- **−** 单文件是写热点：topic 级并行度低于 Kafka 分区日志模型（复制路径逐条 append 亦受此约束，
  见 M5 四桶归因的桶①）。
- **−** 写路径多一跳 dispatch（索引构建）；消费按全局 offset 扫描时需跳过非本 topic 记录。
- **→ 失效边界**：当写吞吐需求超过单文件顺序写上限，或分区级并行 / 多盘扩展成为主要需求时，
  此决策失效——届时应转向 Kafka 式分区日志或 Pulsar 式分层，而不是继续优化单文件。
