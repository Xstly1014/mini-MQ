# 6. 自研 ChaosRunner 混沌注入器（+ PartitionShield 网络分区），不引入 Chaos Mesh

- Date: 2026-10-17 (M4)
- Status: Accepted

## Context

M4 的 12 格可靠性矩阵需要四类注入（优雅停机 / kill -9 / 双向分区 / 单向分区）×
确定性时点钩子（@election / @flush-window / @storm / @steady）。被测对象是同一台机器上的
三个裸 JVM 进程 + 它们之间的 TCP 连接，没有 K8s/Pod。Chaos Mesh 等成熟工具的注入粒度
是容器/网络设备级；且矩阵的信用来自"时点确定可复现"（@第 N 条 ack 注入），现成工具反而
难做到时点级确定性。

## Decision

自研三件套：

- **ChaosRunner**：注入执行器（kill -9 / 优雅 kill 双入口），被测试进程驱动；
- **ChaosControlServer**：broker 内的注入控制通道（回环端口 opt-in）；
- **PartitionShield**：节点间 socket 建立层的黑名单——挡新建连 + 踢存量连接，模拟的是
  网络层故障而非业务代码里的 if 分支。

时点控制用原子计数钩子（"@第 N 条 ack / @选举中 / @刷盘窗口"），不依赖时钟。

否决的备选：

- **Chaos Mesh / Chaos Toolkit**：K8s/Pod 级工具对"本机三裸进程"无从发力；引入 K8s
  只为注入，测试形态成本倒挂。
- **业务代码内 if 注入**：污染被测代码路径，注入器与业务逻辑耦合——分区要发生在
  socket 层才接近真实网络故障。

## Consequences

- **+** 触发时点确定性：同一测试命令重跑同一 cell，注入时点相同（可复现性的根基）。
- **+** 12/12 格一条命令重跑（ReliabilityMatrixTest，248s），RPO 逐格硬断言。
- **+** 挡建连 + 踢存量的分区语义比业务 if 更真实（单向分区"旧主被隔离却不知道"
  的最阴险形态可测）。
- **−** 注入器本身需要被信任：自研工具的代价是用矩阵全绿 + 跨轮一致回验来建立信用。
- **−** chaos-log.jsonl 独立回放档未落盘（复现性暂由测试命令承担），如实记录为 DoD
  部分达成项。
- **→ 失效边界**：被测系统一旦进容器/云环境，本注入器粒度失效，应换平台级工具。
