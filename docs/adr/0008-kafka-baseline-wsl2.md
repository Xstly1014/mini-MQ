# 8. Kafka 对照统一在 WSL2 执行，不做 Windows 原生对照

- Date: 2026-10-17 (M5)
- Status: Accepted

## Context

M5 的最终数字组要求自研 mini-MQ 与 Kafka 同机同参对照（同宿主、RF=3、
acks=all、1KB 消息）。开发机是 Windows（NTFS），而 mini-MQ 的关键机制
依赖类 Linux 的存储/网络语义：FileRegion 零拷贝透传、mmap 刷盘分层、
回环 TCP 行为。此前 M2 在 Windows 原生跑出的数字曾出现"看着对但
镜像失真"的坑（NTFS 的 mmap/页缓存行为与 Linux ext4 不同源），
拿失真的自研数字去对照失真环境的 Kafka 数字，比值无意义。

## Decision

**对照双方统一在 WSL2（同宿主 Linux）执行**：

- mini-MQ 三节点：WSL2 内裸进程集群（`bench/m5-deploy-wsl.sh` 一键部署）；
- Kafka 对照：Kafka 4.1.2 KRaft 三节点，同宿主 WSL2、同回环拓扑
  （`bench/m5-kafka-rf3.sh`），RF=3、acks=all、linger 5ms、batch 16KB、
  无压缩、600k 条 × 1KB × 3 轮；
- 全部产物（jsonl、火焰图、日志）入档 `result/m5-20261017-rf3-wsl/`，
  对照结论进 `tuning.md`：吞吐 51%（38,001 vs 74,776 msg/s）、
  P99 1.65×。

否决的备选：

- **Windows 原生对照**：NTFS mmap 语义失真（M2 已踩过），且 Kafka 官方
  对 Windows 的支持长期残缺（官方文档明示不建议生产使用）——两组失真
  数字相除得到的比值，比没有比值更危险（假精确）。
- **跨机云上对照**：两台机器的 CPU/磁盘/网络不可比，"同机"这一半身位
  优势消失，对照失去"同宿主"这一最大控制变量。
- **只跑自研不跑对照**：没有对照的吞吐数字无法回答"这个数字意味着
  什么"，M5 的四桶归因（把 51% 差距拆到锁/复制/刷盘/客户端协议）
  就没有分母。

## Consequences

- **+** 对照环境与生产语义同源：FileRegion/mmap/回环行为不失真，
  51% 这个比值两边都可信。
- **+** 一键可复现：部署与压测脚本全部入库（`bench/`），任何人在
  同版 WSL2 上可重跑整个对照。
- **−** WSL2 与裸 Linux 仍有差异（I/O 经过 9p/virtio 层）：绝对数字
  偏保守，但这层开销对对照双方同时存在，比值受影响有限。
- **−** Windows 侧的"能跑"证据由单元测试（209 个）承担，性能证据
  统一走 WSL2——两条证据链分开陈述，不混用。
- **→ 失效边界**：若未来要在 Windows 原生提供生产级支持，需要重新
  建立一套 Windows 原生的基准与对照（并诚实标注 mmap 语义差异），
  本决策的对照结论不可平移。
