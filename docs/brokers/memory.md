---
description: "MemoryBroker is a complete broker inside the service's process: it needs no server and comes with the memory feature of ruststream."
---

# Memory

`MemoryBroker` is a complete broker that runs inside the service's process. It comes with the
`memory` feature of `ruststream` and needs no server, so it suits a queue that belongs to one
application. The default `cargo generate` template starts on it. It runs on the tokio runtime;
the embassy runtime, and compio without tokio, run on the [local memory broker](local-memory.md).

```toml
ruststream = { version = "0.8", features = ["macros", "memory", "json"] }
```

## What it supports

| Area | `MemoryBroker` |
|---|---|
| Subscriptions | a topic by name (`#[subscriber("orders")]` or `MemorySource`), and a pattern over topic names (`MemoryPattern`) |
| Delivery | a subscription receives what is published after it opened; a retaining log also replays from a position |
| Settlement | `ack`, `nack` with or without requeue, and a delayed `nack` the bus holds itself, so `retry_after` publishes no copy; each delivery reports its delivery count |
| Batches | `&[T]` handlers, collected greedily up to the mount's batch size |
| Publishing | per-message partition keys, transactions (`TransactionalPublisher`, `OwnedTransactions`) and request-reply (`RequestReply`) |
| Replay | a retaining log (`Seekable`, `Positioned`) |
| Capacity | unbounded: the bus lives on the heap |
| Testing | the bus is its own in-process mode: `TestApp` runs the service's app on it unchanged |

How to use it is in the [Memory broker](../components/memory/index.md) components: the broker and
its prelude, a [retaining log](../components/memory/retention.md) to replay, and
[pattern subscriptions](../components/memory/patterns.md).
