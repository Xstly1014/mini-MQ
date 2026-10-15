# mini-MQ 网络协议 v1（protocol.md）

> M1 任务 1 交付物。本文件是协议的唯一权威契约：帧格式、类型表、字节序、错误码在此定稿。
> 实现：`protocol` 模块（`Frame` / `FrameEncoder` / `FrameDecoder` / `MessageType`），BODY 结构见 `protocol/src/main/proto/mq.proto`。
> 协议帧格式一旦发布即冻结（改协议的代价随里程碑递增——M3 fetch 同步、心跳、选举投票全部复用本帧）。

## 1. 帧格式

```
┌─────────┬─────────┬──────┬────────────┬─────────────┬──────────────┐
│  MAGIC  │ VERSION │ TYPE │ REQUEST_ID │ BODY_LENGTH │    BODY      │
│  2B     │   1B    │  1B  │    8B      │    4B       │   变长       │
│ 0xCACE  │  0x01   │ 见§2 │   long     │    int      │  protobuf    │
└─────────┴─────────┴──────┴────────────┴─────────────┴──────────────┘
|←———————————— 定长头 16B ————————————→|←——— BODY_LENGTH 字节 ———→|
```

| 字段 | 大小 | 存在理由 |
|------|------|----------|
| MAGIC | 2B | 快速识别非法流量：前两字节不匹配 0xCACE 直接断连，把垃圾流量挡在解析之前 |
| VERSION | 1B | 协议演进命脉：后续加字段/改语义时，旧客户端可被识别并拒绝（当前 0x01） |
| TYPE | 1B | 消息类型路由（§2），一个字段的"方法表" |
| REQUEST_ID | 8B | 请求-响应配对钥匙：异步网络下响应乱序返回，客户端按 ID 取回 Future（Kafka correlation id 同款）。选 8B long 而非 4B int：高频复用下 int 有回绕风险 |
| BODY_LENGTH | 4B | 粘包拆包切割依据；同时是 DoS 防线：帧总长超 1MiB 直接断连 |
| BODY | 变长 | protobuf 序列化载荷（见 mq.proto） |

## 2. 类型表

| TYPE 值 | 类型 | 方向 | BODY（protobuf） | 说明 |
|---------|------|------|------------------|------|
| 0x01 | PRODUCE | client→broker | ProduceRequest | 批量发送（聚合后的消息批） |
| 0x02 | PRODUCE_ACK | broker→client | ProduceAck | 逐条 offset + 状态码 |
| 0x03 | FETCH | client→broker | FetchRequest | pull 拉取：topic + 起始全局 offset + maxCount |
| 0x04 | FETCH_RESP | broker→client | FetchResponse | 记录集 + next_offset + high_watermark |
| 0x05 | COMMIT_OFFSET | client→broker | CommitOffsetRequest | v1 已定义、broker 侧存储 M3 启用（v1 位移在消费端本地持久化） |
| 0x06 | COMMIT_OFFSET_ACK | broker→client | CommitOffsetAck | 同上预留 |
| 0x07 | HEARTBEAT | 任意 | （M3 定义） | 租约心跳，M3 副本/选举使用 |

**新增类型只追加，不复用、不改语义。**

## 3. 字节序约定（两层各定各的，绝不混用）

| 层 | 字节序 | 理由 |
|----|--------|------|
| 网络帧（本协议） | **大端** Big-Endian | 网络行业惯例（TCP/IP 协议族字段序），Netty ByteBuf 读写方法默认大端 |
| CommitLog 存储（v1.1） | **小端** Little-Endian | x86 亲和，直接 putInt/getLong 无需字节翻转 |

混用症状：数据"看着对但偶发几位错乱"——最难查的 bug 之一。两层各自写死约定并各自单测。

## 4. LengthFieldBasedFrameDecoder 参数推导

`new LengthFieldBasedFrameDecoder(maxFrameLength, lengthFieldOffset, lengthFieldLength, lengthAdjustment, initialBytesToStrip)`

本协议取值 **(1MiB, 12, 4, 0, 0)**，推导：

1. `lengthFieldOffset = 12`：BODY_LENGTH 前有 magic(2) + version(1) + type(1) + requestId(8) = 12B
2. `lengthFieldLength = 4`：BODY_LENGTH 字段本身 4B
3. `lengthAdjustment = 0`：length 值 = body 字节数，帧总长 = 12 + 4 + body，无需修正
4. `initialBytesToStrip = 0`：解码后还要读 TYPE/REQUEST_ID 构造 Frame，不剥离头部
5. `maxFrameLength = 1 MiB`：**约束整帧（16B 头 + body）**，故单条 body 上限 = 1MiB − 16B；超限抛 TooLongFrameException 断连

半包语义：字节不足一帧时 decoder 静默等待（返回 null），这是特性不是 bug。MAGIC/VERSION 在凑够 2/3 字节时即校验，不必等整帧——非法连接更早被拒。

编码侧：FrameEncoder 手写 length 字段，pipeline 中**不得**再挂 LengthFieldPrepender（双重长度前缀 bug）。

## 5. 错误码

协议级（不占字段，直接断连）：MAGIC 不符 / VERSION 不支持 / TYPE 未知 / 帧超长。

应用级（PRODUCE_ACK.status，见 mq.proto `ProduceStatus`）：

| 值 | 状态 | 语义 | 客户端动作 |
|----|------|------|-----------|
| 0 | OK | 全部落盘 | complete 各 Future(offset) |
| 1 | RETRY_LATER | broker 业务池饱和（背压） | SDK 自动重发：scheduler 线性退避（不阻塞回调线程），超过 maxRetries 后回调失败 |
| 2 | ERROR | 服务端错误 | complete 异常 |

## 6. BODY 契约（protobuf）

完整定义见 `protocol/src/main/proto/mq.proto`（proto 文件即接口契约，字段演进遵循 protobuf 前后兼容规则）。要点：

- **ProduceRequest.messages**：批量聚合体，一次 RTT 摊薄 N 条消息
- **ProduceAck.offsets**：与请求 messages **同序**一一对应，-1 表示该条未落盘
- **FetchResponse.next_offset**：全局扫描位置（含被 topic 过滤跳过的记录）——消费端把它作为位移持久化，重启后从此续读
- **FetchResponse.high_watermark**：M3 副本高水位；v1 = broker maxOffset，字段链路先行打通
- **MessageData.seq_key**：幂等键，hash 后写入存储 KEYHASH 字段；broker 去重窗口 M4 落地，v1 先打通字段链路

## 7. 版本演进规则

- 帧格式冻结；需要新语义时：新 TYPE（追加）或 BODY 内 protobuf 加字段（向后兼容）
- VERSION 只在头布局不兼容变更时 bump，且 broker 必须显式拒绝旧版本（当前行为）
