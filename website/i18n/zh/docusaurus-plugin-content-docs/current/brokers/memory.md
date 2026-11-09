---
description: "MemoryBroker 是运行在服务进程内的完整 Broker：不需要服务器，由 ruststream 的 memory feature 提供。"
---

# 内存 Broker

`MemoryBroker` 是一个运行在服务进程内部的完整 Broker。它属于 `ruststream` 的 `memory` feature，
不需要服务器，适合只属于单个应用的队列。默认的 `cargo generate` 模板就以它起步。它运行在 tokio 运行时上；embassy 运行时，以及不带 tokio 的
compio，运行在[本地内存 Broker](local-memory.md) 上。

```toml
ruststream = { version = "0.8", features = ["macros", "memory", "json"] }
```

## 支持的能力

| 方面 | `MemoryBroker` |
|---|---|
| 订阅 | 按名称订阅主题（`#[subscriber("orders")]` 或 `MemorySource`），以及按主题名模式订阅（`MemoryPattern`） |
| 投递 | 订阅会收到它打开之后发布的消息；保留日志还能从某个位置重放 |
| 确认 | `ack`、带或不带重新入队的 `nack`，以及由总线自己持有的延迟 `nack`，因此 `retry_after` 不会发布副本；每条投递都会报告自己的投递次数 |
| 批 | `&[T]` 处理器，按挂载点的批大小贪婪地凑批 |
| 发布 | 逐条消息的分区键、事务（`TransactionalPublisher`、`OwnedTransactions`）和请求-响应（`RequestReply`） |
| 重放 | 保留日志（`Seekable`、`Positioned`） |
| 容量 | 无上限：总线位于堆上 |
| 测试 | 总线本身就是进程内模式：`TestApp` 原样在它上面运行服务的应用 |

用法见 [Memory broker](../components/memory/index.md) 组件：Broker 本身及其 prelude、可供重放的
[保留日志](../components/memory/retention.md)，以及[按模式订阅](../components/memory/patterns.md)。
