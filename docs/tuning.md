# M5 压测与调优档案

## 状态

M5 验收项全部落地（截至 2026-10-17）：

- 参数化基准（72 个实测轮次：4 负载 × 6 并发档 × 3 轮，取中位）+ RF=3 三节点集群全量压测（18 轮）
- 同宿主 Kafka 4.1.2 KRaft RF=3 对照（吞吐 + P99 双指标）
- CPU / 锁火焰图（JFR + jfr2flame、async-profiler 双工具链），差距归因四桶 + 未归因部分如实标注
- ≥3 组优化前/后对照战报（含 M2 战果回填），每组四段式 + 证据链
- M4 混沌矩阵 12/12 全绿的 RTO/RPO 汇总
- 全部压测/对照脚本入库，可一条命令重跑

## 复现

单机全景（Windows，PowerShell，仓库根目录）：

```powershell
./bench/m5-run.ps1 -Messages 20000 -Warmup 2000 -Rounds 3 -OutputDir result/m5-<run-id>
```

三节点 RF=3 集群 + Kafka 对照（WSL2，脚本已入库 `bench/`）：

```bash
bash bench/m5-redeploy-rf3.sh   # 停旧集群→清 store→更新 jar→3 节点重启→smoke
bash bench/m5-rf3-full.sh       # 6 并发档 × 3 轮，1KB acks=all RF=3 全量压测
bash bench/m5-kafka-rf3.sh     # Kafka 同机同参：600k 条 × 3 轮 + leader CPU 采样
```

脚本约定模块 jar 装在仓库内 `.m2repo/`（`mvn clean install "-Dmaven.test.skip=true" "-Dmaven.repo.local=<repo>/.m2repo"` 后部署）。**教训**：修改源码后必须重新 install 再部署——target/classes 过测试不等于 `.m2repo` 里的 jar 已更新（RF=3 首轮压测失败 5308/60000 的根因就是部署了不含段对齐修复的旧 jar）。

原始数据：
- 单机全景：`result/m5-20261017-verified/`（produce / consume-cold / consume-tail / mixed 四个 jsonl，每行为中位轮，samples 字段含三轮原值）
- WSL 集群 + Kafka 对照 + 火焰图：`result/m5-20261017-rf3-wsl/`（19 个文件：RF=3 jsonl、Kafka 四轮输出、leader CPU collapsed 栈、前后锁火焰图、压测全量日志）
- JFR 录制与单机火焰图：`result/m5-jfr/`、`result/flame/`

## 单机全景（Windows 单 broker，SYNC_FLUSH，1KB，linger 500µs，批量 64，零拷贝 fetch）

吞吐 msg/s；延迟 ms。每格为 3 轮中位。全部行 `consumed` 对账通过（无重无漏）。

| 负载 | 并发 | 吞吐 msg/s | P99 ms | 延迟口径 |
|---|---:|---:|---:|---|
| 纯生产 | 1 | 129,212 | 143.9 | 生产 ack |
| 纯生产 | 2 | 143,153 | 127.3 | 生产 ack |
| 纯生产 | 4 | 168,016 | 113.5 | 生产 ack |
| 纯生产 | 8 | 157,160 | 120.0 | 生产 ack |
| 纯生产 | 16 | 166,297 | 113.7 | 生产 ack |
| 纯生产 | 32 | 169,808 | 110.0 | 生产 ack |
| 冷消费（重启后读） | 1/2/4/8/16/32 | 333,062 / 521,683 / 651,786 / 749,757 / 754,452 / 760,678 | 3.48 / 0.95 / 0.91 / 0.75 / 0.88 / 0.74 | 消费 poll |
| 追尾消费（边写边读） | 1/2/4/8/16/32 | 130,569 / 155,687 / 160,094 / 159,034 / 160,498 / 159,841 | 130.4 / 120.9 / 117.5 / 120.7 / 119.5 / 116.8 | 生产到消费的消息年龄 |
| 混合 50/50 | 1/2/4/8/16/32 | 128,168 / 146,895 / 151,407 / 156,646 / 159,808 / 162,406 | 132.3 / 115.4 / 113.9 / 113.6 / 109.8 / 113.2 | 生产 ack |

生产曲线 4 并发达 ~168k 后在 157k–170k 波动——本地单 broker 的吞吐平台，不是标定过的硬件上限。冷消费 poll 延迟与追尾消息年龄测的是不同区间，不可直接比较。**诚实边界**：冷消费只是 broker 进程重启，不清 OS 页缓存，因此不能作为"磁盘冷读"或页缓存命中率证据；真冷读（`--drop-caches`）工具已实现但未跑成档，此项保持 open。

## RF=3 三节点集群全量压测（WSL2，OS_FLUSH，1KB，acks=all 语义，6 档 × 3 轮）

| 并发 | 吞吐 msg/s（中位） | P50 ms | P99 ms | 三轮吞吐原值 |
|---:|---:|---:|---:|---|
| 1 | 36,577 | 742 | 1,590 | 36577 / 36166 / 37841 |
| 2 | 37,304 | 711 | 1,583 | 38243 / 37304 / 36256 |
| 4 | 38,001 | 711 | 1,555 | 38422 / 37859 / 38001 |
| 8 | 37,492 | 871 | 1,573 | 37492 / 37543 / 35879 |
| 16 | 37,662 | 861 | 1,570 | 35108 / 37662 / 37859 |
| 32 | 35,158 | 969 | 1,677 | 37324 / 35158 / 33485 |

- 18 轮全部成功：0 生产失败、0 ISR 误驱逐（仅启动竞态 2 次，`leo=0`，属选举窗口正常现象）
- 拐点在 c=4（38.0k）；c=1→32 吞吐几乎不动——瓶颈在复制 round-trip 的串行性（follower 单线程 fetch-append 回路 + 每条 `appendReplicated` 串行落盘），客户端并发加不上去，这与 Kafka 同参下的形态一致（P50 两边都在 ~0.7–0.9s 量级），差距体现在单位时间搬动的字节数
- 修复前的对照：满速写入下 follower 被 ISR 误踢 → acks=all 生产失败 5308/60000，压测中止（详见战报 T-04）

## Kafka 同机对照（差距归因）

同宿主 WSL2。Kafka 4.1.2 KRaft 3 节点，RF=3，acks=all，linger 5ms，batch 16KB，无压缩，600k 条 × 1KB × 3 轮（warmup 已丢弃）。

| 指标 | mini-MQ（c=4 三轮中位） | Kafka（三轮中位） | 比值 |
|---|---:|---:|---|
| 吞吐 | 38,001 msg/s（38.9 MB/s） | 74,776 msg/s（73.0 MB/s） | **51%** |
| P99 | 1,555 ms | 943 ms | **1.65×** |
| P50 | 711 ms | 725 ms | 0.98× |

口径差异如实声明：每轮消息数 60k vs 600k（JIT 预热与摊薄程度不同）；刷盘档 mini-MQ 用 OS_FLUSH（页缓存语义），Kafka 默认即页缓存——两者持久性语义对齐。

### 七维参数对齐表（公平对照的前提）

| 维度 | mini-MQ | Kafka 4.1.2 |
|---|---|---|
| 消息大小 | 1,024 B | 1,024 B（record-size） |
| 持久化语义 | acks=all（ISR 全部确认）+ OS_FLUSH | acks=all |
| 副本 / minISR | RF=3 / minIsr=2 | RF=3 / min.insync.replicas=2 |
| 批量 | SDK 攒批 linger 500µs / batch 64 条 | batch.size 16KB / linger 5ms |
| 压缩 | 无 | none |
| 客户端缓冲 | 8 并发独立 producer，重试预算 8s | 单 producer，buffer.memory 64MB |
| 拓扑 | 同宿主 WSL2 三节点（本机回环） | 同宿主 WSL2 三节点 KRaft（本机回环） |

### 差距拆解：四桶归因（每桶给火焰图证据，未归因部分如实保留）

| 桶 | mini-MQ 侧证据（火焰图占比） | 结论 |
|---|---|---|
| ① 客户端/请求批量聚合 | RF=3 曲线 c=1→32 吞吐不动（36.5k→38k）→ 瓶颈不在客户端并发；复制路径每条一个 protobuf 条目、每条一次 `appendReplicated` | Kafka 整批落盘（一个 produce 请求的 records 一次写完）；mini-MQ 复制回路逐条推进，round-trip 次数是消息条数的线性函数 |
| ② 协议编码 | leader CPU 样本中序列化类（protobuf 编解码 + MessageCodec）占 2.2%（SYNC 8 并发满载 profile） | 每条一层嵌套 protobuf 消息；Kafka 自有二进制格式按批摊薄帧开销 |
| ③ 存储路径 | 同 profile 中存储类（CommitLog mmap 追加 / ConsumeQueue dispatch）占 2.2% | 两边同为页缓存顺序写，单 topic 下单条 CommitLog ≈ Kafka 单分区 log——**该桶不解释差距主体** |
| ④ 线程与内存模型 | **leader CPU 91.6% 花在 G1/VM 帧**（6742 样本中 6177 个：OtherRegionsTable::add_reference 2695、G1HotCardCache::insert 679 等——记忆集细化/卡表扫描风暴） | 每记录热路径分配（复制读的 raw byte[]、protobuf 字节数组、Message 对象）在 176k msg/s 满载下把 1GB 堆的 GC 打成 CPU 主体；Kafka 批导向热路径近零每条垃圾 |
| 未归因 | — | 同宿主资源争用（mini-MQ 3 JVM 与 Kafka 3 broker 共存）、轮次长度差 10 倍带来的 JIT 摊薄差、Kafka 近二十年全链路优化。**注意**：④的 91.6% 证据取自 SYNC 满载口径（176k msg/s），证明的是"分配模型是规模墙"；把它直接当作 OS_FLUSH RF=3 那 49% 差距的定量拆分是不诚实的——这部分保留为未归因 |

附：`result/m5-20261017-rf3-wsl/cpu-after.collapsed`（mini-MQ leader async-profiler CPU collapsed 栈）、`kafka-broker-leader-cpu.html`（Kafka leader 采样）、`result/flame/`（JFR cpu/alloc/lock 三类单机火焰图）。

## 优化战报（四段式，证据链入档）

### T-01: 组提交刷盘——SYNC 档从不可用变为可用（M2 回填）

- 现象: 逐条 force 的 SYNC 档只有 83 msg/s（M0 基准），与不 force 的 315 万 msg/s 差 38,055 倍——"ack 前必须落盘"等于不可用
- 归因: fsync 串行上限（单次 0.5–2ms 的数学上限 500–2000 TPS）；对照 MySQL 8.0 `ordered_commit()` 的 Leader/Follower 组提交（行业参照：生产实测 8,000→45,000 TPS——**此为 MySQL 参照数字，非本项目实测，如实区分**）
- 改动: `FlushService` 两参数组提交（linger 500µs + count 64，双参数对照 binlog 的 delay/count）；SYNC producer park 在 `awaitDurable`，flush 线程一次 force 唤醒整批
- 数字: 并发 1→8，每 force 聚合 241→909 条，SYNC 吞吐 213k→714k msg/s——"并发越高组提交越赚"的反常识曲线亲手复现；5×4 扫描矩阵入档 `docs/bench-v2.md`（默认值选在吞吐平台区 + P99 拐点左沿，理由成文）
- 附: 4a 竞态修复（flushedCount 共享计数器被 force 期间的 append 污染 → 改为字节级单调 `flushedOffset`）有专门单测

### T-02: ThreadLocal 栈租约对象池——分配热点消灭（M2 回填）

- 现象: M1 压测记录在案的 P99 尖刺，疑高频分配触发 GC
- 归因: JFR alloc 采样锁定 encode buffer + CRC32 高频分配帧
- 改动: ThreadLocal 栈租借式池（借出即绑定调用栈，use-after-recycle 在类型上不可表达）+ raw 原地切片 + 单次分配的帧写入
- 数字: inline 路径采样分配 **-17.3%**（fetch 路径 -34.0%），热点帧从 JFR Top 25 逐点消失；池命中率实测 100%；30 分钟 soak 吞吐无衰减（+0.2%）、paranoid 模式零 LEAK
- 附: JFR 前后对照表入档 M2 复盘；生命周期违例与脏对象有专门单测

### T-03: MQProducer 发送临界区缩小——锁样本占比 2.4%→0.3%（M5）

- 现象: 8 并发 SYNC 满载下锁火焰图显示 producer 发送路径锁样本占 2.4%
- 归因: 锁 profile 显示 send 临界区包住了不必共享的状态，竞争样本集中在该临界区
- 改动: 临界区缩到共享状态本身（单变量，其余参数冻结）
- 数字: 吞吐 171,865→177,691 msg/s（**+3.4%**，3 轮中位，WSL2 同参数复测）；锁样本占比 2.4%→**0.3%**
- 附: `result/m5-20261017-rf3-wsl/minimq-wsl-produce-sync.jsonl`（前）/ `-sync-after.jsonl`（后）+ `result/flame/m5-lock-produce8c.html`（前，JFR 口径）+ `result/m5-20261017-rf3-wsl/m5-wsl-lock-produce8c-after.html`（后，async-profiler 口径）——两图占比同为归一化份额可比，但工具链不同已在口径注声明

### T-04: 复制链路满载三重修复——ISR 误驱逐归零（M5，可靠性）

- 现象: RF=3 满速写入下 follower 持续被踢出 ISR（3s 无 catch-up），ISR 跌破 minIsr=2 后 acks=all 生产失败 5308/60000，全量压测中止
- 归因: 三层叠加，逐层定位——① ISR caught-up 判定用 live LEO：fetch 在途时 leader 持续写入，follower 到达时永远"追不上"，健康副本被误判（记账竞态）② 复制 fetch 响应无帧预算裁剪：payload + protobuf 元数据超 Netty 解码器帧上限被静默丢弃，follower 超时重试同一段 ③ leader 物理 offset 含段尾 padding、follower 按消息长度累计：跨段时 offset 差出 slack 被误判 divergence，follower 卡死在段边界（实测两 follower 同卡 `leo=67108290` = 64MiB−574）
- 改动: ① 冻结目标算法——判定用 fetch 到达时刻的 LEO 快照，未追上即冻结该目标（门槛永不后移），追到即 caught-up（对齐 Kafka 时间制判定）② 复制路径加 `capToFrameBudget` 帧预算裁剪（对齐消费路径既有设计）③ `appendReplicated` 识别"预期 offset == 下一段起始"的合法跳变，先 roll() 对齐 leader 物理布局
- 数字: 修复后 RF=3 全量 18 轮 0 生产失败，ISR 驱逐仅启动竞态 2 次（`leo=0`），吞吐平台 38k；修复前 5308/60000 失败为对照
- 附: 新增单测 `healthyFollowerKeepsPaceUnderSustainedAppends`（持续满速追赶下不误踢 + 静默 follower 仍被踢）；修复后集群日志与压测日志入 `result/m5-20261017-rf3-wsl/`；另一次部署事故（`.m2repo` 旧 jar 不含 ③ 修复导致复现）作为教训记入"复现"节

## RTO / RPO 汇总（M4 混沌矩阵，12/12 全绿）

- **RPO = 0**：全部 12 个故障格 acked 消息零丢失（`assertEquals(0, r.rpo)` 逐格断言；kill9-leader@flush-window 出现 1 例可见重复属重试语义，非丢失）
- **RTO**：客户端重试链在预算内完成切换，最坏重试链累计 ~2.9s（8s 预算内：检测 ≤1.5s + 选举 ≤0.5s + 客户端重定向 ~1s）
- **诚实记录的不可用窗口**（与 RTO 分开报）：isr-shrink 响亮报错语义下实测 18.3–23.7s（跨轮波动，min.insync.replicas 对齐——选择明确报错优于静默降级）；少数派分区 4.4–4.7s（选择不可用优于错误写入）
- 矩阵覆盖：kill9-leader@{election, flush-window, storm}、kill9-follower@{flush-window, storm}、stale-leader-revival@serving、partition-{bilateral,unilateral}@{election, steady, storm}、isr-shrink、minority-unavailability
- 证据：`result/m5-chaos-matrix.txt`（M4 验收档）+ 末尾追加的 2026-10-17 全仓回归摘要（12 cells `[MATRIX]` 行、209 个测试 0 失败、BUILD SUCCESS）

## 最终数字组（全项目收口表，每行带口径注）

| 数字 | 值 | 口径注与出处 |
|---|---|---|
| 端到端吞吐（1KB, acks=all, RF=3） | **3.8 万 msg/s**（38,001 @ c=4 三轮中位） | WSL2 三节点 OS_FLUSH、linger 500µs、批量 64；`minimq-wsl-cluster-rf3-os.jsonl` |
| P99 端到端延迟（同上） | **1.56 s** | 同上（跨并发档 1.55–1.68s） |
| 单机纯生产吞吐（SYNC_FLUSH） | 16.8 万 msg/s（c=4 中位） | Windows 单 broker、1KB；`m5-20261017-verified/produce.jsonl` |
| 单机纯生产吞吐（OS_FLUSH） | 37.6 万 msg/s（c=8 中位） | WSL2 单 broker、1KB；`minimq-wsl-produce-os.jsonl` |
| 追尾消费吞吐（页缓存命中） | 16.0 万 msg/s（c=32 中位） | 边写边读、消息年龄口径；`consume-tail.jsonl` |
| 自研 = Kafka 同机同参 | **吞吐 51%，P99 1.65×** | 同宿主、RF=3 acks=all、1KB；四桶归因见上节 |
| 优化对照战报 | **4 组**（2 组 M2 回填 + 锁临界区 + 复制链路修复） | 每组四段式 + 火焰图/日志证据链 |
| 故障转移 RTO / RPO | **~2.9s / 0**（12/12 格，预算 8s） | 混沌矩阵；响亮报错窗口 18.3s 单列，不计入 RTO |
| 组提交收益（项目实测） | SYNC 吞吐 21.3 万→71.4 万（1→8 并发） | M2 bench-v2 扫描矩阵；MySQL 8k→45k 为行业参照另注 |
| 对象池收益（项目实测） | inline 采样分配 -17.3%（fetch 路径 -34.0%） | M2 JFR 前后对照 |

## 诚实边界（未闭环项，如实标注）

1. **真冷读未做档**：重启不清页缓存，`--drop-caches` 已实现但无成档运行——"磁盘冷读"结论保持 open
2. **四桶归因不硬凑闭环**：④ 的 GC 占比证据来自 SYNC 满载口径，跨口径直接拆分 OS_FLUSH RF=3 的差距不诚实；未归因部分（同宿主争用、轮次长度差、Kafka 成熟度）明示保留
3. **Kafka leader 火焰图符号不可读**：`kafka-broker-leader-cpu.html` 采样过短、符号未解析出可读帧，只作为附件存档，不作为占比断言来源
4. **跨工具链对比口径**：T-03 前图为 JFR、后图为 async-profiler，占比同为归一化份额但采样器不同，已在战报内声明
