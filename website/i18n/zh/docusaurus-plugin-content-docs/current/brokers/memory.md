---
description: "MemoryBroker 是运行在服务进程内的完整 Broker：不需要服务器，由 ruststream 的 memory feature 提供。"
---

# 内存 Broker

`MemoryBroker` 是一个运行在服务进程内部的完整 Broker。它属于 `ruststream` 的 `memory` feature，
不需要服务器，适合只属于单个应用的队列。默认的 `cargo generate` 模板就以它起步。

```toml
ruststream = { version = "0.7", features = ["macros", "memory", "json"] }
```

用法见 [Memory broker](../components/memory/index.md) 组件：Broker 本身及其 prelude、可供重放的
[保留日志](../components/memory/retention.md)，以及[按模式订阅](../components/memory/patterns.md)。
