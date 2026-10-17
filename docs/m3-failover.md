# mini-MQ m3-failover：多进程集群故障转移验收档案

日期：2026-10-16 · 里程碑：M3 · 工具：`broker` 模块 `ClusterMain` + `client` 模块 `MultiProcessClusterTest`

## 1. 目的

M3-8 的同 JVM 集群证明了协议逻辑正确，但进程内的"kill"只是 `shutdown()`。本档案回答只有真实进程边界才能验证的问题：

1. **真 kill -9**（Windows `TerminateProcess`，无 shutdown hook、无刷盘）下，leader 死亡到客户端恢复要多久？
2. **优雅停机 vs kill -9**：两种死法下，acks=all 的持久性承诺是否都兑现？（M1 埋的伏笔在此收线）
3. **同盘复活**：被 kill -9 的节点带着旧 epoch 和脏尾巴重启，epoch 仲裁是否零污染？

## 2. 环境

| 项 | 值 |
|---|---|
| OS | Windows 10 专业版 10.0.19045 (amd64) |
| CPU | Intel Core i9-14900K |
| JDK | Oracle 17.0.12 |
| 拓扑 | **3 个独立 java 子进程**（各自 mmap 存储、选举计时器、复制线程），loopback TCP |
| 选举参数 | etcd 10:1 形状：心跳 75ms / 选举超时随机 [750, 1500)ms |
| ISR 参数 | min.insync.replicas=2 / replica.lag.max.ms=3000 |
| 刷盘 | OS_FLUSH（kill -9 语义交给页缓存与多数派） |
| 观测 | 纯黑盒：单节点 PRODUCE 探测找 leader（NOT_LEADER+hint）；tail 子进程日志找 `elected leader, epoch N` 行 |

## 3. 8s 预算分解 vs 实测

手册预算：`8s = 检测 1.5 + 选举 0.5 + 客户端重试 1 + 4s 安全余量`。

实测（kill -9 leader，稳态 acks=all 写入中）：

| 阶段 | 预算 | 实测 | 落点 |
|---|---|---|---|
| 超时检测 + PreVote + 选举 | ≤2.0s | **1152ms** | `RaftNode` 随机超时 [750,1500)ms 触发，PreVote 探测 → 多数派投票 → `elected leader, epoch N` 落日志 |
| 客户端感知 + 重定向恢复 | ~1s | **4072ms** | 见下方注 1 |
| 安全余量 | 4s | — | GC / 本机负载 |
| **端到端总计** | **8s** | **5224ms** | **达标，余量 2.78s** |

> 注 1（客户端段为何 4s）：failover 窗口内三层重试叠加——writer 线程 `send().get(3s)` 超时放弃 → `MQProducer` 线性退避（100ms×attempts）重发 → `FailoverTransport` 收到 NOT_LEADER/连接拒绝后按 hint 或 bootstrap 轮询。新 leader 就位前的每次探测都要走完整退避节拍，4s 是"保守重试策略"的成本，不是检测慢。改进方向（M4+）：NOT_LEADER 快速通道（hint 命中即零退避重发）。

复现命令（在仓库根目录）：

```
mvn test -pl client -am -Dtest=MultiProcessClusterTest
```

## 4. 四场景验收结果

| # | 场景 | 预期 | 实测 |
|---|---|---|---|
| 1 | **kill -9 leader**（稳态 acks=all 压测中） | ≤8s 新 leader 服务；对账 0 丢失 | **5.2s 恢复**；全部 acked offset 在 failover consumer 侧 0 丢失 |
| 2 | **旧 leader 复活**（同 storeDir 重启） | 以 follower 回归，epoch 仲裁截断脏尾，不抢占现任 | 复活节点未扰动现任 leader；复活后继续写 10 条 acks=all 全部入账，0 丢失 |
| 3 | **follower 挂 ~10s 再回** | ISR 收缩、写入继续、回归追平 | kill 后 acks=all 仅短暂 stall（ISR sweep 3s 窗口内踢出死副本）即恢复；40 条 acked 全部对账 0 丢失 |
| 4 | **优雅停机 vs kill -9** | 两种死法 acked=all 数据都完整 | 全集群优雅停机（QUIT）→ 重启：10/10 在；全集群 kill -9 → 重启：累计 20/20 在（对照细节见 §5） |

同 JVM 版（`ClusterIntegrationTest`，5 场景含脏尾构造/ISR_TOO_SMALL）作为逻辑回归保持全绿；多进程版是本档案的验收依据。

## 5. 优雅停机 vs kill -9 对照（M1 伏笔收线）

| 维度 | 优雅停机（stdin QUIT） | kill -9（TerminateProcess） |
|---|---|---|
| 停机路径 | `BrokerServer.shutdown()` 顺序关闭：停接入 → 复制组件退位（唤醒 acks=all 等待者）→ 业务池排空 → SYNC 等待者唤醒 → flush+close 存储 → 索引收尾 | 无任何 hook：进程映像直接撕毁，页缓存/ISR 多数派兜底 |
| 本地落盘 | 强制 flush，页缓存 → 磁盘 | 依赖 OS_FLUSH 语义：数据在页缓存，进程死不清页缓存（仅断电/系统崩溃丢失） |
| 已 ack 数据 | 0 丢失 | **0 丢失**（acks=all ack = HW 覆盖 = ISR ≥2 节点持有） |
| 未 ack 数据 | 优雅排空 | 脏尾巴风险，交给 epoch 仲裁截断（场景 2 验证零污染） |
| 丢失窗口 | 理论为零 | 等于 M2 刷盘档位语义：SYNC_FLUSH 0 丢失 / ASYNC+OS_FLUSH 以页缓存为界 |

场景 4 实测：两轮全集群停机（先优雅后 kill -9）共用同一存储，重启后 20/20 条 acks=all 数据全部可读——**acks=all 的承诺在两种死法下都兑现**，差异只在"谁替你兑现"（优雅=主动刷盘；kill -9=多数派+页缓存）。

## 6. 观测手段（黑盒，无进程内引用）

多进程后没有 `replication()` 句柄可查，验收完全走运维视角：

- **找 leader**：逐端口单节点 PRODUCE 探测——leader 接受写入，follower 秒回 `NOT_LEADER{leader_hint}`，死节点连接拒绝；
- **选举时刻**：tail 子进程 stderr 日志，`node-N elected leader, epoch N` 行首次出现的时间戳（检测+选举阶段的终点）；
- **ISR 健康**：行为推断——acks=all 成功即 ISR ≥ minIsr；死副本不阻塞写入即 sweep 已踢出；
- **对账**：`FailoverTransport` consumer 从 bootstrap 任意节点拉全量，acked offset 集合 ⊆ consumed offset 集合。

## 7. 结论

- 端到端 failover **5.2s / 预算 8s**，四场景全绿，acks=all 全程 0 丢失；
- M3 DoD k01（多进程互通）/ k07（四场景脚本化 + 8s 耗时分解 + 0 丢失）/ k09（优雅停机 vs kill -9 对照）三项验收通过；
- 174 个测试全仓库零回归（`mvn clean test`）。
