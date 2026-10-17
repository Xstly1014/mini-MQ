# 7. 会话级 seq_key 去重窗口（LRU 10 万条），不做跨会话精确一次

- Date: 2026-10-17 (M4)
- Status: Accepted

## Context

at-least-once 语义下生产者重试必然引入重复：acks=all 超时后 SDK 自动重发，
同一条消息被 append 两次。M4 的第三段防线需要 broker 侧把"同一会话内的
重试重复"折叠掉，把 at-least-once 在会话内收敛到 exactly-once。跨会话/
跨 leader 的精确一次在业界需要事务 + 两阶段提交 + 跨纪元 offset 仲裁
（Kafka 到 KIP-360 才补齐 epoch-tagged producer state）。

## Decision

Leader 进程内维护一个**会话级**去重窗口（`DedupWindow`）：

- **数据结构**：`LinkedHashMap<KEY_HASH, offset>`，access-order LRU，
  容量默认 10 万条（`ClusterMain` 第 7 参 `dedupEntries=100000` 启用，
  0/缺省 = M1-M3 的 append-always 语义——能力 opt-in，不偷偷改语义）；
- **键**：存储层 KEY_HASH（FNV-1a 64），10 万条下哈希碰撞概率 ~1e-7，
  与存储层对同一哈希的信任度一致；
- **命中**：produce 路径查到已见 key → 不再 append，返回
  `ProduceStatus.DUPLICATE` + **原 offset**（客户端拿到的仍是第一条的
  位置，语义对齐"重试透明"）；
- **Follower 永不去重**：leader 已过滤的数据原样复制，数据面 FETCH
  把裁决结果带下游。

否决的备选：

- **跨会话精确一次（事务 + 两阶段提交）**：需要持久化 producer state、
  跨 epoch 的 offset 仲裁、abort/恢复协议——超出 mini-MQ 的边界，
  且 Kafka 走通这条路用了数年（KIP-98 到 KIP-360）。最终仲裁权
  留给消费端按 seq_key 幂等，这是 Kafka 前未开事务时同款分工。
- **窗口持久化（broker 重启后仍生效）**：把会话级问题升级成共识问题，
  重启后窗口本就该失效（生产者会话大概率也已重建），收益不抵复杂度。
- **Follower 侧也去重**：复制路径引入二次裁决，ISR 收敛语义被污染
  （follower 的职责是忠实镜像，不是二次过滤）。

## Consequences

- **+** 会话内重复折叠：SDK 重发命中窗口，返回原 offset，矩阵中
  kill -9 + 重试场景下业务侧零重复消费。
- **+** 诚实边界写进了协议：ProduceStatus.DUPLICATE 是 v1.4 显式新增的
  状态码，客户端能区分"新写入"与"去重命中"。
- **−** **跨 leader 失效**：故障转移后重试落到新 leader，窗口为空，
  会重复 append——这是声明的边界，不是隐藏的缺陷；消费端 seq_key
  幂等兜底。
- **−** 极端并发下尽力而为：check 与 record 虽在同一 synchronized 段，
  两个业务线程同瞬同 key 仍可双写（矩阵中生产者串行重试，不触发）。
- **→ 失效边界**：需要跨会话/跨 leader 精确一次的那一刻（如金融扣费
  场景），本决策失效——要么引入事务系统，要么把仲裁权全部下放消费端。
