# mini-MQ bench-v0：存储写入路径四模式基准

日期：2026-10-15 · 里程碑：M0 · 工具：`bench` 模块 `BenchV0`

> 注：本文档数字基于 M0 的存储格式 v1.0（29B 定长头，记录 1,029B）。M1 引入 TOPIC 字段升级为 v1.1（31B 头，记录 1,036B）后的重跑结果与结论对照见 [bench-v1.md](bench-v1.md) §4。

## 1. 目的

在实现任何上层协议之前，先量化「RocketMQ 式全局单 CommitLog + mmap 顺序写」这条写入路径的收益边界，回答三个问题：

1. mmap 相比 FileChannel 每条一写的增益有多大？（A vs D）
2. 逐条 force 刷盘的代价有多大？（A vs B）
3. 组提交（每 N 条刷一次）能挽回多少？（C 的位置）

## 2. 环境

| 项 | 值 |
|---|---|
| OS | Windows 10 专业版 10.0.19045 (amd64) |
| CPU | Intel Core i9-14900K |
| JDK | Oracle 17.0.12 (HotSpot 64-Bit Server VM) |
| Maven | 3.5.3 |
| 存储 | Samsung 990 PRO 2TB (NVMe) + ST8000NM017B 8TB (SATA HDD)，工作区位于 E: 盘 |
| 负载参数 | 100,000 条 × 1,029 B 记录（1,000 B payload + 29 B 定长头），warmup 10,000 条不计入 |

运行方式：`mvn -pl bench clean compile exec:java`（B 模式因逐条 force 过慢，采样 10,000 条）。

## 3. 四模式定义

| 模式 | 写路径 | 刷盘策略 | 度量的问题 |
|---|---|---|---|
| A | mmap（MappedByteBuffer.put） | 从不 force | 纯内存写上限（page cache） |
| B | mmap | 每条 force（msync 等价） | 每消息持久化的代价 |
| C | mmap | 每 100 条 force | 组提交的收益曲线 |
| D | FileChannel.write + DirectBuffer | 从不 force | 每条一次 write 系统调用的开销 |

记录采用 CommitLog v0 真实线格式（小端、CRC32、定长头），编码一次复用，排除编解码开销，只测写路径。

## 4. 结果

| mode | msgs | MiB | elapsed(ms) | msg/s | MiB/s | note |
|------|------|-----|-------------|-------|-------|------|
| A | 100000 | 98.1 | 31.7 | 3,158,569 | 3099.6 | mmap, no force (page cache) |
| B | 10000 | 9.8 | 120400.3 | 83 | 0.1 | mmap, force every record |
| C | 100000 | 98.1 | 21057.7 | 4,748 | 4.7 | mmap, force every 100 records |
| D | 100000 | 98.1 | 157.6 | 634,400 | 622.6 | FileChannel + DirectBuffer, no force |

- **A/B = 38,055x**（验收线 ≥ 10x）
- **A/D = 4.98x**（mmap vs 逐条 syscall 写）
- **C/A = 0.15%**，单次 force 平均延迟：B ≈ 12 ms，C ≈ 21 ms（C 每次冲刷约 100 KB 脏页，延迟高于 B 的单条）

## 5. 分析

### A 模式的「水分」说明（必读）

A 的 3.16 M msg/s 是 **page cache 写入速率，不是持久化吞吐**：数据只进了脏页，由 OS 异步刷盘（Windows 上通常 8-10 秒窗口）。断电可丢最近数秒数据；进程崩溃不丢（页缓存属于 OS）。它作为**写入路径上限**存在，生产语义对应「异步刷盘 + 主从复制兜底」的配置。若把它当持久化吞吐宣传，即是注水。

### B 模式：逐条 force 不可用

83 msg/s（每条 12 ms 级 force 延迟）意味着逐条同步刷盘在本机比 A 慢近四万倍。这不是本机特例：force（fsync/FlushViewOfFile+FlushFileBuffers）要等存储设备确认，HDD 约 10 ms、消费级 SSD 开启写缓存时同样在 10 ms 量级。**任何「每消息一次 fsync」的设计在单机上都不可行**，这正是 Kafka/RocketMQ 都用组提交或异步刷盘的原因。

### C 模式：组提交是唯一可行折中

每 100 条（≈100 KB）刷一次，吞吐 4,748 msg/s：相比 A 慢 660 倍，但相比 B 快 57 倍。刷盘延迟被 100 条摊薄后，单条摊销成本 ≈ 0.21 ms。**v0 的结论：写路径采用 mmap + 组提交异步刷盘**，组提交的具体形态（攒批唤醒 / 定时刷）在 M1 与 Broker 线程模型一起实现。

### D 模式：mmap 的净增益约 5 倍

同为不刷盘，A（3.16 M msg/s）比 D（634 K msg/s）快 4.98 倍。D 每条一次 write 系统调用 + 堆外缓冲中转；A 只有内存 put，页缺失时由内核惰性补页。这与业界 mmap 与 syscall 写的典型对比（3-6x）一致，验证了「FileChannel 每条一写在顺序写场景下仍是次优」的调研判断。

## 6. M0 验收对照

| 验收项 | 线 | 实测 | 结论 |
|---|---|---|---|
| A 模式吞吐 | ≥ 50,000 msg/s | 3,158,569 msg/s | PASS（63x 余量） |
| A/B 差距 | ≥ 10x | 38,055x | PASS |
| 结果可复现 | 脚本入仓 | `mvn -pl bench clean compile exec:java` | PASS |
| 结论入档 | 对照表 + 水分说明 | 本文档 §4-§5 | PASS |

## 7. 复现

```powershell
mvn -pl bench clean compile exec:java                 # 默认 100k × 1KB
mvn -pl bench exec:java -Dexec.args="200000 200 50"   # count payloadSize forceEveryN
```

数据文件落在 `data/bench/<mode>/`（已 gitignore）。
