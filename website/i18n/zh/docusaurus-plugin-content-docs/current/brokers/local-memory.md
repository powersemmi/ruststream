---
description: "LocalMemoryBroker 是 embassy 运行时的进程内总线：同一固件各部分之间的队列，分为使用堆的形态和不做任何分配的固定形态。"
---

# 本地内存 Broker

`LocalMemoryBroker` 是一个运行在单个固件内部的完整 Broker。它属于 `ruststream` 的 `memory` feature，
与 `embassy`（或不带 `tokio` 的 `compio`）一同启用。它是 embassy 运行时的总线，正如[内存 Broker](memory.md)
是 tokio 运行时的总线。它的类型不是 `Send`，可以在任意执行器上运行，也不读取时钟。`cargo generate`
的 `embassy-memory` 模板就以它起步。

```toml
ruststream = { version = "0.8", default-features = false, features = ["embassy", "memory", "macros", "postcard"] }
```

## 支持的能力

| 方面 | `LocalMemoryBroker` |
|---|---|
| 订阅 | 按名称订阅主题（`#[subscriber("sensors/temp")]` 或 `LocalMemorySource`） |
| 投递 | 订阅会收到它打开之后发布的消息 |
| 确认 | `ack`，以及带或不带重新入队的 `nack`；每条投递都会报告自己的投递次数；`retry_after` 和死信通过运行时发布的副本完成，副本存放在挂载点的 `.retry_capacity::<CAP>()` 中 |
| 批 | `&[T]` 处理器，大小不超过挂载点的批大小；一批不会超过订阅所能容纳的数量 |
| 发布 | 通过 `LocalMemoryPublish` 进行普通发布和回复 |
| 容量 | 每个订阅容纳 `N` 条消息（Broker 的常量参数），处理器手中的消息也计入；向已满的订阅发布会返回 `LocalMemoryError::Full` |
| 测试 | 总线本身就是进程内模式：嵌入式 `TestApp` 在主机上用它运行服务的应用 |

总线针对框架的每种构建各有一种形态：

| | 有 `alloc` | 无 `alloc` |
|---|---|---|
| 总线所在位置 | 堆 | 由板级代码声明为 `static` 的 `LocalMemoryBus` |
| 构造函数 | `LocalMemoryBroker::<N>::new()` | `LocalMemoryBroker::<N>::new(&BUS)` |
| 负载 | 任意大小 | 最多 `PAYLOAD` 字节；总线未另行指定时为 64 |
| 同时打开的订阅数 | 不限 | 最多 `SUBSCRIPTIONS` 个；总线未另行指定时为 8 |
| 批 | 由投递组成的 `Vec` | 最多容纳 `N` 条投递的固定槽 |
| 分配 | 总线及其通道 | 无：这种构建不需要分配器 |

固定总线位于临界区之后，因此 `critical-section` 的实现由二进制 crate 提供。容不下任何一条消息、
任何一个字节或任何一个订阅的总线无法通过构建。

用法见 [Local memory broker](../components/memory/local.md) 组件、
[Embedded runtime](../components/application/embedded-runtime.md) 页面和
[嵌入式 `TestApp`](../components/application/testing/embedded-app.md)。
