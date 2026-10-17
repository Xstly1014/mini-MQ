# 5. 数据面 Kafka 式 fetch 复制 + HW，选举只取 Raft 内核（KRaft 同款混合架构）

- Date: 2026-10-16 (M3)
- Status: Accepted

## Context

多副本需要共识机制。两个极端候选：

- **全链路 Raft**：所有 entry 进一条全序日志、每写多数派确认——消息队列的真实负载是
  "多分区并行 + 分区内局部序"，全序点就是多分区高吞吐下的序列化瓶颈（写放大模型不匹配）。
- **纯 Kafka ISR 数据面**：每分区独立 fetch 流水线吞吐优先，但选举/仲裁语义弱——纯 HW
  恢复在旧主复活场景会截错方向（KIP-101 引入 Leader Epoch 的原因），脑裂防护需补。

工业界参照：KIP-500/KRaft 把 Kafka 元数据层迁到 Raft，但数据面保持 fetch 复制没动——
"控制面 Raft、数据面分区复制"是被行业验证过的合体判断，不是拼凑。

## Decision

混合架构：

- **数据面抄 Kafka**：follower FETCH 拉取（follower 是"特殊的消费者"，协议帧复用
  FOLLOWER_FETCH）+ Leader Epoch checkpoint 仲裁（STALE_EPOCH 拒绝 / TRUNCATE_TO 截断 /
  脏尾巴回截）+ HW 高水位（ISR 各 LEO 最小值，消费只可见 HW 之前）+ min.insync.replicas
  下限（ISR 跌破响亮报错 ISR_TOO_SMALL，绝不静默退化成 acks=1）。
- **选举只取 Raft 内核**：term/votedFor 落盘、随机超时（[750,1500)ms，etcd 10:1 形态）、
  多数派、PreVote（防旧主扰动）。

否决的备选：

- **全链路 Raft**：写放大 + 全序瓶颈，与 MQ 负载模型不匹配（上文）。
- **纯 ISR 无 epoch**：旧主复活的脏尾巴没有仲裁依据——混沌矩阵的 stale-leader cell 正是
  验证这条防线的设计动机。

## Consequences

- **+** 复制协议复用消费帧，M3 网络层零新增协议成本；每分区独立流水线不引入全局序列化点。
- **+** epoch 仲裁在混沌矩阵实证：旧主复活不扰动现任（RPO=0）；kill -9 检测+选举 1,152ms，
  客户端重试链端到端 ~2.9s（8s 预算内）。
- **−** 选举弱语义需要 lease 心跳 + epoch 双机制补全——复杂度没有消失，只是从"共识协议"
  搬到了"仲裁机制"。
- **−** M5 满速压测暴露 ISR 时间制判定竞态：live LEO 比较把健康 follower 误踢（fetch 飞行
  期间终点线持续前移）——修复为冻结目标算法（fetch 到达时刻 LEO 快照冻结为追赶目标，门槛
  永不后移），对齐 Kafka replica.lag.time.max.ms 的时间制语义。慢速全绿不等于满速正确。
- **→ 失效边界**：需要跨分区强一致事务、或全局强一致的元数据视图时，此混合架构不足，
  应引入独立共识层。
