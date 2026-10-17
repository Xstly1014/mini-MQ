# 3. 组提交刷盘：双参数（linger/count）攒批 + 三档刷盘语义，否决逐条 force

- Date: 2026-10-16 (M0 raised the baseline, M2 finalized)
- Status: Accepted

## Context

M0 bench-v0 实测：逐条 force 83 msg/s，与不 force 的 315.9 万 msg/s 相差 38,055 倍——
fsync 串行上限（单次 0.5–2ms）决定了"ack 前必须逐条落盘"在这个量级的硬件上等于不可用。
持久化语义需要一条中间路线：既保留"ack 即已 force"的强语义档位，又不被 fsync 上限掐死。

## Decision

`FlushService` 双参数组提交：linger 500µs + count 64（双参数对照 MySQL `ordered_commit()`
的 delay/count 两旋钮）；SYNC 档 producer park 在 `awaitDurable`，flush 线程一次 force
唤醒整批。默认参数由 5×4 扫描矩阵定（吞吐平台区 + P99 拐点左沿，理由成文 docs/bench-v2.md），
不是拍的。刷盘语义分三档（SYNC_FLUSH force / OS_FLUSH 页缓存 / ASYNC_FLUSH），可靠性-吞吐
权衡显式化为可选档位。

否决的备选：

- **逐条 force**：M0 实测上限 83 msg/s，直接否决。
- **单参数攒批**（只 linger 或只 count）：扫描曲线显示吞吐与 P99 对两个参数的敏感度不同，
  单参数无法分别控制"攒多久"与"攒多少"。

## Consequences

- **+** SYNC 档从 83 提升至 21.3 万→71.4 万 msg/s（并发 1→8，每 force 聚合 241→909 条）——
  "并发越高组提交越赚"的反常识曲线亲手复现。
- **+** OS_FLUSH vs SYNC_FLUSH 两档把可靠性-吞吐代价变成可测量的数字（M5：37.6 万 vs
  16.8 万 ≈ 2.2×），而不是一句"看配置"。
- **−** 单条等待延迟上升（攒批窗口内的 park）；低并发下聚合条数少、收益摊薄。
- **−** 共享 flushedCount 计数器曾与 force 期间的并发 append 竞态（M2 4a bug）——修复为
  字节级单调 flushedOffset，有专门单测。
- **→ 失效边界**：低并发 + 强延迟敏感场景下攒批收益反转；参数需按 fsync 实测重新扫描。
