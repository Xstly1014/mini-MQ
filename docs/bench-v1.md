# mini-MQ bench-v1：端到端吞吐 / P99 基线与十万条对账

日期：2026-10-15 · 里程碑：M1 · 工具：`bench` 模块 `BenchV1` + `Reconcile`

## 1. 目的

M0 证明了存储裸快（mmap 写路径 2.99M msg/s）。M1 加上协议、网络、编解码、SDK 之后回答两个问题：

1. **可靠性**：10 万条消息经过完整链路（批量聚合 → protobuf 编码 → TCP → broker 解码 → 业务池落盘 → FETCH 读回 → 位移持久化），能否做到丢失=0 / 重复=0 / 乱序=0？
2. **性能**：端到端还剩多快？——这是简历数字的起点，也是 M2 组提交优化的对照组。

## 2. 环境

| 项 | 值 |
|---|---|
| OS | Windows 10 专业版 10.0.19045 (amd64) |
| CPU | Intel Core i9-14900K |
| JDK | Oracle 17.0.12 (HotSpot 64-Bit Server VM) |
| Maven | 3.5.3 |
| 拓扑 | 同机 loopback，broker 与客户端同 JVM 进程内（绝对值可变，相对关系才是结论） |
| 负载 | 1KB payload（[8B seq][8B 发送时间戳][填充]） |
| 生产参数 | batch=256KB / linger=1ms / 单 in-flight / 本地未确认窗口 16,384 条 |
| 消费参数 | FETCH maxCount=256 / broker 扫描预算 256 条 / idle 3s 判定收尾 |

## 3. 对账结果（可靠性一票否决项）

`Reconcile`：100,000 条 × 1KB，producer 全部 ack 后 close（优雅停机保证最后一批落地），consumer 从 0 拉到 idle，逐条校验全局序号。

| item | value |
|------|-------|
| produced (acked) | 100,000 |
| produce failures | 0 |
| consumed records | 100,000 |
| **lost** | **0** |
| **duplicate seqs** | **0** |
| **out-of-order** | **0** |
| bad payloads | 0 |
| max end-to-end latency | 615.6 ms |

produce 阶段 0.51s，consume 阶段 3.52s（含 3s idle 等待）。脚本可重复执行（每次运行使用独立临时 store + 时间戳 topic，无需手工清理）。

**结论：丢失=0 / 重复=0 / 乱序=0，精确到零，验收线达成。**

## 4. 端到端基线（BenchV1）

1 轮 warmup 丢弃 + 5 轮测量取中位，每轮独立 broker / 独立 topic / 5 万条 × 1KB。

| round | produce msg/s | send P50 (ms) | send P99 (ms) | send max (ms) | consume msg/s | max e2e (ms) |
|-------|---------------|---------------|---------------|---------------|---------------|--------------|
| 1 | 213,563 | 65.5 | 85.7 | 87.8 | 236,909 | 308.1 |
| 2 | 470,473 | 30.4 | 40.3 | 41.5 | 551,376 | 146.0 |
| 3 | 484,766 | 30.7 | 35.0 | 35.2 | 666,514 | 131.9 |
| 4 | 490,067 | 29.9 | 34.8 | 35.6 | 679,035 | 125.4 |
| 5 | 504,238 | 28.9 | 34.2 | 34.3 | 678,115 | 125.0 |

（第 1 轮偏低为 JIT 继续编译 + 文件系统冷缓存，取中位可容错。）

### 中位数汇总

| metric | median |
|--------|--------|
| **端到端 produce 吞吐（send→落盘→ack 闭环）** | **484,766 msg/s** |
| producer send P50 | 30.4 ms |
| producer send P99 | 35.0 ms |
| consume 吞吐（pull 视角） | 666,514 msg/s |
| max end-to-end latency | 131.9 ms |

门槛：端到端 ≥ 10,000 msg/s → **实测 484,766 msg/s，48 倍余量，PASS**。

### bench-v0 v1.1 格式重跑（格式变更后复测）

存储格式 v1.0→v1.1（31B 头 + topic 字段，记录 1029B→1036B）后重跑四模式：

| mode | M0 (1029B) msg/s | v1.1 (1036B) msg/s | 变化 |
|------|------------------|--------------------|------|
| A: mmap, no force | 3,158,569 | 2,619,604 | -17%（记录变大 + 运行波动） |
| B: force every record | 83 | 83 | 持平 |
| C: force every 100 | 4,748 | 4,829 | 持平 |
| D: FileChannel | 634,400 | 625,901 | -1.3% |

结论不变：A/B 仍 >30,000x，写路径结论（mmap + 组提交异步刷盘）维持有效。

## 5. 吞吐差距归因猜想（M2 用火焰图验证）

端到端 484,766 msg/s 是裸存储 A 模式 2,619,604 msg/s 的 **18.5%**。差距分布猜想：

1. **RTT 串行是第一瓶颈（约占 80% 的批周期）**。单 in-flight 模型下吞吐 ≈ 批大小 / 往返延迟。实测 256KB 预算切出 ~237 条/批，484,766 msg/s 反推批周期 ≈ 0.49 ms。而存储侧每批 mmap 落盘按 A 模式速率折算仅 ~90 µs（18%）——**剩余 ~0.4 ms 消耗在 loopback TCP 往返 + proto 编解码 + 两次线程交接（发送方 EventLoop ↔ broker worker ↔ 业务池）的唤醒开销上**。验证方式：M2 火焰图按帧拆分批周期构成。
2. **protobuf 编解码 + 拷贝链**。每条消息经历 heap→ByteString→Netty ByteBuf→broker 侧 byte[]→mmap，至少 4 次内存拷贝，1KB 消息约 4µs/条的纯拷贝成本。M2 对象池（复用 ByteBuf）预计可回收一部分。
3. **send P99 = 本地窗口排队，不是 broker 慢**。P99 35.0ms ≈ 窗口上限 16,384 条 ÷ 237 条/批 = 69 批 × 0.49ms ≈ 34ms，与实测吻合——尾部延迟完全由生产者本地攒批决定，是"批量换吞吐"的直接代价。业务方调小窗口即可换低延迟（吞吐略降），SDK 层面这是可配置的 trade-off 而非缺陷。
4. **消费吞吐 666K 同样是 RTT 主导**：256 条/FETCH ÷ 0.38ms ≈ 674K，与实测吻合。当前 FETCH 的 topic 过滤靠 CommitLog 顺序扫描（扫描预算 256 条），单 topic 全命中时无浪费；**多 topic 混合写入时会退化为"扫描 256 条只命中少量"——这正是 M2 ConsumeQueue 索引要解决的问题**。
5. **Windows loopback + 同进程的加成与失真**：无真实网卡中断/协议栈开销，但 Java 侧 syscall 边界一个不少。绝对数字标注 loopback，跨机器重测在 M3。

## 6. 过程中发现并修复的缺陷（压测的价值证明）

**现象**：Reconcile 首跑立即失败——broker 侧 `TooLongFrameException: 17313081 > 1MB`，producer 侧连接被关闭。

**定位**（拆分排查，非猜测）：17,313,081B ÷ ~1,060B/条 ≈ 16,336 条 ≈ 恰好等于压测的本地未确认窗口 16,384。锁定 `MQProducer.flushLocked()` 的 relay 路径：批 ack 回来后把 in-flight 期间堆积的 buffer **全量**打包成一帧发出（`new Batch(new ArrayList<>(buffer))`），没有按 `batchSizeBytes` 切片。单元测试用脚本化 transport 只测了小批量，从未堆出超预算的 backlog，属于典型的"假批量"盲区。

**修复**：`flushLocked()` 按字节预算从队头切片，溢出部分留在 buffer 由下一批 ack 驱动 relay；单条超预算仍单独成批防饿死。回归单测 `relayAfterAckSlicesOversizedBacklogIntoBudgetBatches` 固化该场景（gate 卡住 in-flight → 堆积 6 条 → 断言切成 3 个预算内小批）。

**通用教训**：客户端攒批组件必须有"relay 切片"语义，否则慢 ack 场景下本地缓冲会在一瞬间转化为超限帧；协议的帧长上限（1MB）是最后防线，不该被日常流量触达。

## 7. M1 验收对照（DoD 相关项）

| 验收项 | 线 | 实测 | 结论 |
|---|---|---|---|
| 10 万条对账 0/0/0 | 丢失=0 重复=0 乱序=0 | §3 | PASS |
| 端到端吞吐 | ≥ 10,000 msg/s | 484,766 msg/s（中位） | PASS（48x 余量） |
| bench-v1.md 入档 | 三组数字 + 环境 + 归因猜想 | §2/§4/§5 | PASS |
| bench-v0 格式变更重跑 | 维持结论 | §4 | PASS |

## 8. 复现

```powershell
mvn -pl bench clean compile exec:java "-Dexec.mainClass=com.minimq.bench.Reconcile" "-Dexec.args=100000 1024"
mvn -pl bench exec:java "-Dexec.mainClass=com.minimq.bench.BenchV1" "-Dexec.args=50000 1024"
mvn -pl bench exec:java    # bench-v0 默认（v1.1 格式）
```

Reconcile / BenchV1 均使用临时目录（运行后自动清理）与随机端口，可重复执行；Reconcile 非零退出码即对账失败。
