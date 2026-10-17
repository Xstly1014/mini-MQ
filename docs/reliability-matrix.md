# M4 可靠性矩阵（12 cells）—— 混沌注入 × 时点钩子 × 逐格硬断言

> 本文件是 12 格混沌矩阵的摘要档：每格注入什么、押了哪条防线、结果如何。
> 原始证据：`result/m5-chaos-matrix.txt`（M4 验收轮 + 2026-10-17 全仓回归摘要）。
> 一条命令可重跑（约 248s，会拉起真实多 JVM 进程集群）：

```bash
mvn -pl client -am test -Dtest=ReliabilityMatrixTest
```

## 为什么是这 12 格

可靠性不是"跑一次 kill -9 不丢"就完事——**故障不挑时刻，测试必须主动挑各种时刻**。
矩阵 = 故障场景（kill -9 / 双向分区 / 单向分区 / 复活 / 降级 / 少数派 / 重试风暴）
× 确定性时点钩子（@election / @flush-window / @storm / @steady，原子计数触发，
同一命令重跑同一格、时点相同）。

三段防线各押一格：

1. **客户端重试 + FailoverTransport**（M3）：故障转移后客户端自动追到新 leader；
2. **Leader 会话级去重窗口**（M4，ADR-0007）：重试重发折叠为 DUPLICATE + 原 offset；
3. **消费端重试上限 + 死信**（M4）：毒消息 3 次后进 dead-log，不堵死队列。

## 12 格结果（2026-10-17 回归轮，RPO 逐格硬断言）

| # | Cell | RPO | 可见重复 | 断言口径 |
|---|---|---:|---:|---|
| 01 | kill-9 leader @storm（写入风暴中） | 0 | 0 | acked 50 / consumed 50，对账全等 |
| 02 | kill-9 leader @flush-window | 0 | 0 | acked 46 / consumed 46，SYNC 落盘语义生效 |
| 03 | kill-9 leader @election（选举进行中） | 0 | 0 | acked 46 / consumed 46 |
| 04 | kill-9 follower @storm | 0 | 0 | acked 51 / consumed 51，ISR 收缩后继续服务 |
| 05 | kill-9 follower @flush-window | 0 | 0 | acked 54 / consumed 54 |
| 06 | 双向分区 @storm | 0 | 1 | acked 51 / consumed 52；1 例重复属重试语义（见下） |
| 07 | 双向分区 @election | 0 | 0 | acked 51 / consumed 52 |
| 08 | 单向分区 @steady（旧主被隔离而不自知） | 0 | 0 | acked 50 / consumed 50 |
| 09 | 旧 leader 复活 @serving（stale-leader-revival） | 0 | 0 | acked 65 / consumed 65，term 仲裁第一次接触即终结 |
| 10 | ISR 收缩 → 响亮报错（isr-shrink） | 0 | 0 | ISR_TOO_SMALL 替代静默降级，不可用时长单列 |
| 11 | 少数派分区不可用记录（minority） | 0 | 0 | 宁可不可用，绝不错写入；窗口单列 |
| 12 | 重试风暴 → 去重窗口收敛 | — | — | 同 seq_key 重发必须回 DUPLICATE + 原 offset（端到端证明） |

**可见重复 ≠ 丢失**：第 06 格的 1 例重复是 at-least-once 重试语义的诚实展示；
同一会话内的重试由去重窗口折叠（第 12 格端到端断言），跨 leader 的重复由
消费端 seq_key 幂等兜底（ADR-0007 声明的边界）。

## RTO 与"诚实不可用窗口"分开记

- **RTO（恢复型故障）**：客户端重试链最坏 ~2.9s 恢复（8s 预算 = 检测 ≤1.5s +
  选举 ≤0.5s + 客户端重定向 ~1s + 裕量；协议段 1,152ms 为 M3 实测）。
- **不可用窗口（非 RTO，如实单列）**：
  - isr-shrink 响亮报错：18.3–23.7s（跨轮波动）——ISR 跌破 min.insync.replicas=2
    后拒绝 acks=all 写入，**选择明确报错优于静默降级**；
  - 少数派分区：4.4–4.7s——少数派凑不齐 ISR 写入必然失败，
    **宁可不可用，绝不出错**。

## 矩阵揪出的真 bug（postmortem）

双向分区验收一跑即现形：**少数派 Leader 用过期 HW 服务消费读**——被隔离的
Leader 收不到 ISR 更新，它眼里的高水位已失真，消费者通过它读到的"已提交"
数据可能未过多数派仲裁。FETCH 的 HW 门禁当时只看 Leader 本地视角，
而少数派 Leader 的本地视角已经失真。

修复三件套：① 少数派 Leader 对 FETCH 直接回 ISR_TOO_SMALL（拒绝服务，
绝不给出失真的提交视图）；② leader 变更后取消在途的 stale follower FETCH；
③ 客户端 hint 失效即清、回 bootstrap 列表重扫。这是"宁可不可用，绝不出错"
从口号变成代码的地方。

教训入档：**矩阵的信用不在全绿，而在它有能力揪出真 bug**；12/12 全绿是
修复后的复验结果，修复前的红格才是矩阵的第一次回报。
