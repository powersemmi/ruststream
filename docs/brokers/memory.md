---
description: "MemoryBroker is a complete broker inside the service's process: it needs no server and comes with the memory feature of ruststream."
---

# Memory

`MemoryBroker` is a complete broker that runs inside the service's process. It comes with the
`memory` feature of `ruststream` and needs no server, so it suits a queue that belongs to one
application. The default `cargo generate` template starts on it.

```toml
ruststream = { version = "0.8", features = ["macros", "memory", "json"] }
```

How to use it is in the [Memory broker](../components/memory/index.md) components: the broker and
its prelude, a [retaining log](../components/memory/retention.md) to replay, and
[pattern subscriptions](../components/memory/patterns.md).
