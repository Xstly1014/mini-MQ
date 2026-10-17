# mini-MQ bench-v2：组提交 / 零拷贝 / 池化三机制量化与三档崩溃对账

日期：2026-10-16 · 里程碑：M2 · 工具：`bench` 模块 `BenchV2` + `Kill9Reconcile` + `Reconcile` + `AllocProfile` + `SoakV2`

## 1. 目的

M1 交付了端到端可用（484,766 msg/s，0 丢失）。M2 回答四个问题：

1. **组提交值不值**：SYNC_FLUSH（ack 等 force）相对 OS_FLUSH（ack 不等）到底损失多少吞吐？linger/count 双参数各自买到了什么？
2. **可靠性红线**：kill -9 三档（SYNC/ASYNC/OS）崩溃后，语义承诺是否精确兑现（SYNC 零丢失；ASYNC/OS 崩溃窗口如实暴露）？
3. **零拷贝与池化的真实收益**：sendfile 相对堆内拷贝、ThreadLocal 池化相对裸分配，数字是多少？（JFR 分配采样入档）
4. **稳定性**：30 分钟持续负载下吞吐是否衰减、内存是否平稳、paranoid 泄漏检测是否零报告、池命中率是否 >90%？

## 2. 环境（相对 bench-v1 的变化）

| 项 | 值 |
|---|---|
| OS | WSL2 Ubuntu（内核 5.10.16.3-microsoft-standard-WSL2） |
| CPU | 同机 32 逻辑核（i9-14900K，WSL2 视图） |
| JDK | OpenJDK 17.0.20.1（HotSpot 64-Bit Server VM） |
| 拓扑 | 同机 loopback，broker 与客户端同 JVM 进程内（绝对值可变，相对关系才是结论） |
| 存储 | /tmp（WSL2 ext4 虚拟磁盘）——**真 Linux syscall**：mmap/force/sendfile 不再走 Windows 模拟层 |

> 与 bench-v1 的关键差异：v1 在 Windows 原生跑（mmap 无 force 语义、sendfile 缺失），M2 为拿到真实 Linux I/O 语义（DoD k05）迁移 WSL2。两代数据只做定性衔接，不做跨环境定量对比。

## 3. 实验 1：组提交矩阵（SYNC_FLUSH，1 producer，linger × count 扫描）

每格独立 broker + 独立 store，2 万条 × 1KB，双触发（linger 到时 / count 满先到先触发）。

### 吞吐（msg/s）

| linger(us) \ count | 16 | 64 | 256 | 1024 |
|--------------------|--------:|--------:|--------:|--------:|
|                  0 |   18,691 | 166,666 | 190,476 | 186,915 |
|                200 |  198,019 | 178,571 | 208,333 | 212,765 |
|                500 |  208,333 | 210,526 | 204,081 | 212,765 |
|               1000 |  210,526 | 210,526 | 202,020 | 212,765 |
|               2000 |  112,359 | 111,731 | 112,359 | 110,497 |

### send 延迟 P50 / P99 / max（ms）

| linger(us) \ count | 16 | 64 | 256 | 1024 |
|--------------------|---------------:|---------------:|---------------:|---------------:|
|                  0 | 56/325/666 | 30/49/50 | 26/42/43 | 27/41/42 |
|                200 | 26/39/39 | 26/40/41 | 24/35/36 | 24/35/35 |
|                500 | 24/36/36 | 24/36/36 | 26/37/38 | 24/36/36 |
|               1000 | 24/35/36 | 24/35/36 | 25/39/40 | 24/35/35 |
|               2000 | 47/70/71 | 47/69/70 | 47/69/70 | 47/69/70 |

### 每 force 聚合条数（聚合窗口买到的东西）

| linger(us) \ count | 16 | 64 | 256 | 1024 |
|--------------------|--------:|--------:|--------:|--------:|
|                  0 |    183.5 |   212.8 |   227.3 |   224.7 |
|                200 |    224.7 |   224.7 |   235.3 |   238.1 |
|                500 |    238.1 |   238.1 |   238.1 |   241.0 |
|               1000 |    241.0 |   241.0 |   238.1 |   241.0 |
|               2000 |    241.0 |   241.0 |   238.1 |   241.0 |

**读法**：

1. **linger=0 且 count=16 是唯一崩塌格**（18,691 msg/s，P99 325ms）：窗口太小 → 聚不起来 → 每次 force 只带一两条 → fsync 延迟全进 P99。这就是"逐条 force"模式（M0 B 模式 83 msg/s）的组提交版。
2. **count=16 但 linger≥200 就满血**（~200k）：单 producer 下聚合上限由**攒批节奏**（SDK batch 256KB ≈ 238 条）决定，不由 count 决定——241 msgs/force ≈ SDK 每批 238 条 + 尾巴，说明 force 恰好每批一次。
3. **linger=2000us 全面劣化**（112k，P50 47ms）：窗口比攒批周期还长，producer 在白白等—— linger 应小于 SDK 攒批周期（~1ms 量级）。
4. **count 的作用在低 linger 区救场**（linger=0 时 16→64 从 18k 跳到 166k），在正常 linger 区几乎无感——它是保险丝，不是油门。
5. 延迟地板 P50≈24ms 由 SDK 本地攒批（batch 256KB / linger 1ms / 窗口 4096）决定，与刷盘参数基本无关（除 2000us 档）——**尾部延迟是攒批的代价，不是 force 的代价**（与 M1 归因一致）。

**默认值选择：linger=500us / count=64**（即 broker `BrokerConfig` 默认）。理由：吞吐 210,526 已到平台区（+2% 内拿不到更多）；P99 36ms 是拐点左沿（2000us 才开始劣化，留 4 倍余量）；500us 窗口的崩溃暴露带（kill -9 最多丢最近 500us 内未 force 的 append）在 ASYNC/OS 档是可接受语义；count=64 保证极端低流量下（攒不满批）也能及时刷出。

## 4. 实验 2：并发曲线——"组提交高并发红利"复现（DoD k03）

OS_FLUSH（ack 不等 force，M1 语义）vs SYNC_FLUSH（500us/64），1→16 producers，每格 2 万条 × 1KB。

| producers | OS_FLUSH msg/s | SYNC_FLUSH msg/s | SYNC msgs/force | SYNC P50/P99 (ms) |
|----------:|---------------:|-----------------:|----------------:|------------------:|
|         1 |        526,315 |          212,765 |           241.0 | 24/35 |
|         2 |        689,655 |          333,333 |           454.5 | 23/40 |
|         4 |      1,000,000 |          540,540 |           740.7 | 22/30 |
|         8 |        800,000 |          714,285 |           909.1 | 14/24 |
|        16 |        400,000 |          434,782 |           645.2 | 21/33 |

**读法**：

1. **反直觉曲线复现**：SYNC_FLUSH 吞吐随并发**上升**（213k → 333k → 541k → 714k）。机制：更多 producer → flusher 睡 500us 期间攒到更多 append → msgs/force 241 → 455 → 741 → 909 → **fsync 延迟被更大批量摊薄**。这正是 Kafka/BookKeeper 类组提交的核心红利：并发越高，每条消息分摊的 force 成本越低。
2. **8 producers 时组提交代价只剩 11%**（714k vs 800k）：M0 曾测逐条 force 83 msg/s vs mmap 315 万——组提交把 force 的 38,000 倍鸿沟压缩到 11%。
3. **16 producers 双双回落**（OS 400k / SYNC 435k）：瓶颈不再是刷盘而是协议层（16 连接的 EventLoop 争抢 + 窗口控制线程切换），SYNC 反而与 OS 持平——高并发下刷盘已不是短板。
4. **P99 在 8 producers 时最低**（14/24ms）：SDK 窗口 4096/producer 在高并发下攒批更快，等待时间更短。

## 5. 实验 3：零拷贝 FETCH A/B（DoD k04/k05）

1 producer + 1 consumer，OS_FLUSH，5 万条 × 1KB，**每路径 3 轮取中位**（此环境单轮方差实测 ~75%，不取中位的对照不可信——这是本轮压测方法论上的最大教训）。

| fetch 路径 | 3 轮 (msg/s) | 中位 msg/s | CPU us/msg |
|------------|----------------|-------------:|-----------:|
| inline（堆内拼装，M1 路径） | 132,978 / 159,744 / 174,216 | **159,744** | 6.0 |
| sendfile（头 ByteBuf + FileRegion） | 136,612 / 140,845 / 198,412 | **140,845** | 7.0 |

**如实入档：WSL2 loopback 下 sendfile 路径比 inline 慢 11.8%，CPU 高 1 us/msg。**

归因（M3 真机验证项）：

1. **WSL2 的 sendfile 走虚拟化 I/O 栈**（9p/HCS 层），非原生网卡 DMA 路径，零拷贝的"省内核→网卡搬运"收益不存在；剩下的只有 syscall 本身的开销。
2. **两步写出的 syscall 税**：零拷贝路径每个 FETCH 响应 = 头部 write + FileRegion transferTo，比 inline 单次 write 多一次内核切换；1KB 小消息场景该税无法被摊薄。
3. 架构价值不在本环境兑现：零拷贝的期望收益场景是**大 payload + 真实网卡 + 多消费者扇出**（broker 用户态内存带宽卸载）。v1.2 双路径布局（`zeroCopyFetch` 开关）保留了两条路径的正确性与可回归性（见 kill-9/对账/soak 均 zeroCopy=on 跑），真机数据 M3 补。

## 6. kill -9 三档崩溃对账（DoD k03/k06）

`Kill9Reconcile`：独立 broker 进程 → producer 灌载 → **随机时刻 SIGKILL** → 重启恢复（CRC 全量校验 + ConsumeQueue 重放）→ 拉回对账。三档语义（对齐 innodb_flush_log_at_trx_commit）。

### SYNC_FLUSH（ack 必须等 force 完成才返回 —— 红线：acked 消息 0 丢失）

| item | value |
|------|-------|
| sent (send attempted) | 81,961 |
| acked (OK from broker) | 65,336 |
| send failures (post-kill) | 16,385 |
| consumed after restart | 65,336 |
| **acked but lost** | **0** |
| duplicate / out-of-order / fabricated | 0 / 0 / 0 |

**VERDICT: PASS** —— kill 前 broker 确认的每一条在 kill 后都读得回来。

### ASYNC_FLUSH（ack 不等 force，承认崩溃窗口）

| item | value |
|------|-------|
| sent / acked / failed | 106,592 / 90,488 / 15,864 |
| consumed after restart | 90,488 |
| **acked but lost（崩溃窗口实测）** | **0** |
| duplicate / out-of-order / fabricated | 0 / 0 / 0 |

**VERDICT: PASS**（不变式全零）。注意：ASYNC 档语义**允许**窗口丢失，本次实测窗口内恰好零丢失——linger 500us 的暴露带极小，kill 时刻落在 force 间隙的概率低；这是"窗口存在但未命中"，不是"窗口不存在"。

### OS_FLUSH（仅 OS 页缓存，进程死数据在、主机死全丢）

| item | value |
|------|-------|
| sent / acked / failed | 76,629 / 60,484 / 15,905 |
| consumed after restart | 60,484 |
| **acked but lost** | **0** |
| duplicate / out-of-order / fabricated | 0 / 0 / 0 |

**VERDICT: PASS**（不变式全零）。SIGKILL 只杀进程不杀页缓存，OS 页缓存内的数据重启后仍然可读——丢失窗口只在主机断电/内核崩溃时打开。

**三档共同不变式**：恢复后 CommitLog 无 CRC 损坏（部分写入的尾部记录被精确截断，不计入任何序列号校验）、ConsumeQueue 与 CommitLog 对齐重放、消费无重复无乱序无伪造。

## 7. 10 万条对账回归（M2 全栈，DoD k06）

M1 的 `Reconcile`（100,000 × 1KB，producer 全 ack 后优雅停机，consumer 从 0 拉到 idle）在 M2 全栈（FlushService 组提交 + ConsumeQueue 索引 + 零拷贝 FETCH + 池化编解码）上回归：

| item | value |
|------|-------|
| produced (acked) | 100,000 |
| produce failures | 0 |
| consumed records | 100,000 |
| **lost** | **0** |
| **duplicate seqs** | **0** |
| **out-of-order** | **0** |
| bad payloads | 0 |
| max end-to-end latency | 1,316.0 ms |
| produce / consume elapsed | 1.19 s / 3.82 s |

**VERDICT: PASS**（0 丢失 0 重复 0 乱序红线保持）。produce 速率 84k msg/s（默认 OS_FLUSH + M2 栈；v1 同项 196k——差额来自 M2 消费链路 ConsumeQueue 分发与 reput 线程的 CPU 争抢，属可解释回归，最终端到端见实验 2 OS_FLUSH 526k~1000k 无回归）。

## 8. JFR 池化对照（DoD k07/k08）

`AllocProfile`：单 JVM 内 20 万条 × 1KB 完整生产+消费轮，JFR `ObjectAllocationInNew/OutsideTLAB`（byte-weighted 采样）。

| 配置 | inTLAB (MB) | outTLAB (MB) | 合计 (MB) | vs 基线 |
|------|------------:|-------------:|----------:|--------:|
| 池化前 inline（M2 初基线） | 22.9 | 623.4 | **646.3** | — |
| 池化后 zeroCopy（sendfile 路径） | 8.2 | 418.4 | **426.6** | **-34.0%** |
| 池化后 inline（同路径对照） | 14.4 | 520.4 | **534.8** | **-17.3%** |

**读法**：

1. **同路径（inline）池化砍掉 17.3% 采样分配字节**：`CodecPools`（ThreadLocal 栈租约：encode buffer 复用 + CRC32 复用）+ `raw` 原地切片 + `prefixedFrame` 单次分配，逐点消灭 M1 JFR Top 分配位（热点帧在池化后从 Top 25 消失）。
2. **零拷贝+池化合计 -34.0%**：sendfile 路径绕过 payload 的堆内拼装，与池化收益叠加。
3. 采样口径说明：JFR allocation 事件按 TLAB 阈值采样，比例可信、绝对值低估；对照结论只看相对差。

池化设计要点（为什么零 use-after-recycle）：`CodecPools` 只池化**完全绑定当前调用栈**的对象（encode buffer 借出后仅本次 encode 栈帧持有，返回即隐式失效），无跨线程逃逸，无显式回收协议——生命周期违例在类型系统上不可表达，配套单测覆盖脏对象与违例场景。

## 9. 30 分钟稳定性 soak（DoD k08/k09）

`SoakV2`：限速 10,000 msg/s（8 producers × 512B，30 分钟 ~9.4GB CommitLog），全 M2 栈（SYNC_FLUSH 500us/64 + ConsumeQueue + 零拷贝 FETCH + 池化），Netty `leakDetection.level=paranoid`。每分钟报告吞吐 / 堆 / GC / 泄漏 / 池命中率；consumer 侧逐条校验 per-producer 严格序列（30 分钟 × 1,800 万条全量对账）。

**第四次运行 VERDICT: PASS**（前三次分别死于：误删运行中 store、误删运行中 store、2 GiB 溢出 bug——最后一次是被 soak 抓到的产品缺陷，见 §11.6）。跨过 2^31 段边界（约第 6-7 分钟）后全程满速，修复实战验证。

| item | value |
|------|-------|
| 总 acked = 总 consumed | **17,920,450 = 17,920,450** |
| verify errors (loss/dup/reorder/bad) | **0 / 0 / 0 / 0**（30 分钟逐条全量对账） |
| produce first-5m vs last-5m | 9,943/s vs **9,959/s（+0.2%，零衰减）** |
| 每分钟 produce 速率 | 30 分钟全部钉在 597,400~597,600/min（9,943~9,960/s） |
| GC | 250 次 / 827 ms 总计，趋势走平（22 次/分钟 → 6 次/分钟） |
| 堆 | 156~898MB 锯齿振荡（committed 恒定 1648MB，无扩容无爬升） |
| Netty LEAK（paranoid） | **0** |
| encode-buffer 池命中率 | **100.0%**（17,920,418/17,920,450，仅启动预热 32 次未命中） |
| crc32 池命中率 | **100.0%**（53,761,317/53,761,350） |
| 写入总量 | ~10.1 GB（~38 个 256MB 段） |

consume 每分钟速率在 555k~627k/min 间波动（消费端批量追赶节奏），30 分钟总计与 produce 精确相等且逐条校验零差错。

> 报表勘误：本次日志 summary 行 `(-500.8% decay)` 是 SoakV2 窗口快照 bug（25m 后每分钟覆盖快照，把 30 分钟累积当 5 分钟量），已修复（快照一次 + `final − snapshot` 计算）；上表衰减为从原始每分钟数据重算的正确值。

## 10. 结论

| 机制 | 结论 | 证据 |
|------|------|------|
| 组提交（SYNC_FLUSH） | 单 producer 代价 2.5×（213k vs 526k），8 producers 时仅 11%（714k vs 800k）；高并发红利曲线复现（msgs/force 241→909） | 实验 1/2 |
| 三档持久性语义 | SYNC 红线兑现（65,336 acked 零丢失）；ASYNC/OS 不变式全零，崩溃窗口如实标注 | kill -9 三档 |
| 可靠性回归 | 0 丢失 0 重复 0 乱序，M1 红线在 M2 栈保持 | 10 万对账 |
| 零拷贝 | WSL2 loopback 下 -11.8%（洼地如实入档）；架构保留，真机验证 M3 | 实验 3 |
| 池化 | inline -17.3% / zeroCopy -34.0% 采样分配；命中率 100%（见 soak） | JFR + soak |
| 稳定性 | 30 分钟 1,792 万条零差错零衰减（+0.2%）、GC 走平、paranoid 零 LEAK、池命中率 100%；并抓到 2 GiB int 溢出 P0 bug | soak |

**默认参数定型**：`flushLingerUs=500` / `flushBatchCount=64` / `flushMode=OS_FLUSH`（吞吐默认档，可靠性场景切 SYNC）/ `zeroCopyFetch=false`（WSL2 无收益，真机重评）——均已写入 `BrokerConfig` 默认值。

## 11. 本轮压测的缺陷记录（方法论资产）

1. **单轮对照不可信**：零拷贝 A/B 首跑单轮显示 sendfile -35% 或 +12% 随机漂移——同配置重跑方差 75%。修复：3 轮取中位。教训：**loopback 微基准必须多轮中位数**。
2. **spawn 的 broker 会随 stdin EOF 立即退出**：BrokerMain 原靠 stdin 读循环驻留，nohup 后台化即退出。修复：EOF 时 park 等待，QUIT 线控退出。
3. **kill 时机太早 = 弱证据**：首次 killer 500-1500ms 即杀，冷 JVM 还没完成首轮攒批，样本只有几十条 ack。修复：攒满 1 万 ack 再随机杀。
4. **soak 限速公式错**：burst 间隔按 per-producer 而非全局限速计算，实际跑出 18×目标速率，30 分钟会写爆磁盘。修复：`burstSleepMs = burst × producers × 1000 / targetRate`。
5. **soak 输出标签误导**：每分钟计数标成 `/s`，读数差 60 倍（本次验收时差点据此误杀健康进程）。修复：`597,400/min (9,957/s)` 双单位输出。教训：**报告的标签错误比没有报告更危险**。
6. **[P0] 2 GiB int 溢出——soak 抓到的产品 bug**（修复于 M2-4b，本轮最大收获）：
   - **现象**：第三次 soak 第 7 分钟起 consume 从 9,957/s 腰斩至 5,767/s 再到 0，GC 从 15 次/分钟暴涨至 268 次/分钟，crc32 池计数每分钟暴增 4.6 亿次（每条消息被重扫 ~767 次），produce 侧全程正常。
   - **定位链**（拆分排查，非猜测）：jstack 抓 reput 线程 RUNNABLE 卡在 `dispatchLoop → readFrom → decode`（9.3 分钟烧 170 秒 CPU）→ ConsumeQueue 索引文件反推卡点：最后条目 end = 2,147,763,712，恰跨 **2^31 = int 溢出边界**（第 8 个 256MB 段 base = 2,147,483,648）。
   - **根因**：`CommitLog.readFrom` 用 `idx * segmentSize`（**int 乘法**）判断游标是否落在本段。idx=8 时 8×256MB = 2^31 溢出为负 → 条件永假 → pos 被静默重置为 0 → reput 从段头重读 512 条旧记录，算出的新游标 == 旧游标 → **无限重扫**（batch 非空不走 park，每秒 ~6,300 轮 × 512 次 decode+CRC）。消费饿死、CPU 空转、GC 风暴全部由此而来。
   - **修复**：段定位改为 `fileFromOffset` 范围判断（long 运算），语义等价、无溢出。
   - **回归测试**：手建 3×1GiB 稀疏段链（base 0 / 2^30 / 2^31）绕过"必须真写 2GB 数据"的复现门槛，断言 `readFrom` 在段内定位不回卷（`readFromLocatesOffsetsPastThe2GiBIntOverflowBoundary`）。
   - **为什么只有 soak 能抓到**：全项目测试（单测/对账/kill-9/BenchV2）写入量全部 < 2GB，**只有 30 分钟 soak 会跨过 2^31**。这是稳定性压测存在价值的直接证明——长时程不是仪式，是唯一能触达这类"量变引起质变"缺陷的测试。附带发现：ConsumeQueueStoreTest 两个用例 reopen 前泄漏 mmap 句柄（WSL2 的 unlink 语义掩盖了它，Windows 文件锁暴露），一并修复。
   - 教训：**offset 算术必须全程 long；byte 级 offset 系统的边界测试要覆盖 2^31**（int 天地的天花板）。
7. **SoakV2 衰减报表 bug**：summary 输出 `-500.8% decay` 而实际零衰减——last-5m 快照变量在 25m 后每分钟被覆盖为累积值，输出时把 30 分钟总量除以 300 秒。修复：快照只取一次（25m 标记处），last-5m = `final − snapshot`。教训同 #5：**报表口径错误会直接把"完全平稳"显示成"崩塌"**，验收数字必须能从原始每分钟数据反推。
