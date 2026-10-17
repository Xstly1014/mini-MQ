# 2. 自定义 16B 定长头二进制协议 + protobuf body，不直接上 HTTP/gRPC

- Date: 2026-10-15 (M1)
- Status: Accepted

## Context

需要一个 client↔broker 与节点间（M3 起：心跳/选举/副本拉取）共用的网络协议。候选：

- HTTP/REST：文本开销、请求模型不适配 broker 主动语义；
- gRPC：HTTP/2 帧开销 + 双向流复杂度，帧预算与透传成本不可自主控制；
- 裸 protobuf over TCP：无帧切分依据、无快速校验层、无版本演进位。

网络层用 Netty，需要与 LengthFieldBasedFrameDecoder 参数一一对应的定长头。

## Decision

16B 定长头（MAGIC 2B = 0xCACE / VERSION 1B / TYPE 1B / REQUEST_ID 8B / BODY_LENGTH 4B，
大端）+ protobuf body（变长）。帧解码参数 (1 MiB, 12, 4, 0, 0)；类型只追加不复用不改语义；
协议演进规则：新 TYPE 或 protobuf 加字段，帧布局冻结（见 docs/protocol.md，v1.3/v1.4 均为追加式演进）。

否决的备选：

- **HTTP/gRPC**：帧开销与透传成本；1 MiB 级帧上限、MAGIC 前置断连这类"把垃圾流量挡在解析前"
  的防线无法自主实现。
- **裸 protobuf**：缺快速识别非法流量、缺版本位、缺粘包拆包依据——三条全是要在 TCP 长连接
  上自己补的。

## Consequences

- **+** MAGIC 前置把非法连接挡在解析之前；帧总长超 1 MiB 直接断连（DoS 防线）。
- **+** REQUEST_ID 8B long 做异步请求-响应配对（Kafka correlation id 同款），int 高频复用有回绕风险。
- **+** 节点间复用同一帧：M3 的 VOTE/HEARTBEAT/FOLLOWER_FETCH 零新协议成本；FOLLOWER_FETCH
  独立 TYPE 让 broker 不解包 BODY 即可分流到复制单线程域。
- **−** 没有现成生态工具（抓包/调试自写）；协议文档即契约（docs/protocol.md）。
- **−** 帧预算约束要在每条新路径上重新适用：M5 复制路径漏加帧预算裁剪，导致 fetch 响应
  超限被 Netty 静默丢弃、follower 死循环重试同一段——修复即 `capToFrameBudget`（T-04 ②）。
  教训入档：协议的约束不会自动传播给后来者。
