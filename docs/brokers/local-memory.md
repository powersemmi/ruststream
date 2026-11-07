---
description: "LocalMemoryBroker is the bus of the embassy runtime: a queue between the parts of one firmware, with a heap form and a fixed form that allocates nothing."
---

# Local memory

`LocalMemoryBroker` is a complete broker that runs inside one firmware. It comes with the `memory`
feature of `ruststream` next to `embassy` (or `compio` without `tokio`), and it is the bus of the
embassy runtime, as the [memory broker](memory.md) is of the tokio one. Its types are not `Send`,
it runs on any executor, and it reads no clock. The `embassy-memory` template of `cargo generate`
starts on it.

```toml
ruststream = { version = "0.8", default-features = false, features = ["embassy", "memory", "macros", "postcard"] }
```

## What it supports

| Area | `LocalMemoryBroker` |
|---|---|
| Subscriptions | a topic by name (`#[subscriber("sensors/temp")]` or `LocalMemorySource`) |
| Delivery | a subscription receives what is published after it opened |
| Settlement | `ack` and `nack` with or without requeue; each delivery reports its delivery count; `retry_after` and dead letters go through copies the runtime publishes, held in the mount's `.retry_capacity::<CAP>()` |
| Batches | `&[T]` handlers, up to the mount's batch size; a batch never holds more than one subscription holds |
| Publishing | plain publishes and replies through `LocalMemoryPublish` |
| Capacity | each subscription holds `N` messages, the broker's const parameter, counting the ones in a handler's hands; a publish to a full subscription returns `LocalMemoryError::Full` |
| Testing | the bus is its own in-process mode: the embedded `TestApp` runs the service's app on it on the host |

The bus has a form for each build of the framework:

| | With `alloc` | Without `alloc` |
|---|---|---|
| Where the bus lives | on the heap | in a `LocalMemoryBus` the board declares as a `static` |
| Constructor | `LocalMemoryBroker::<N>::new()` | `LocalMemoryBroker::<N>::new(&BUS)` |
| Payload | any size | at most `PAYLOAD` bytes, 64 unless the bus names another |
| Subscriptions open at once | any number | at most `SUBSCRIPTIONS`, 8 unless the bus names another |
| A batch | a `Vec` of deliveries | a fixed slot of at most `N` deliveries |
| Allocations | the bus and its channels | none: the build needs no allocator |

The fixed bus sits behind a critical section, so the binary supplies the `critical-section`
implementation. On a core without compare-and-swap, such as a Cortex-M0+, the firmware also gives
`portable-atomic` its `critical-section` feature. A bus that holds no message, no byte or no
subscription does not build.

How to use it is in the [Local memory broker](../components/memory/local.md) component, the
[Embedded runtime](../components/application/embedded-runtime.md) page and the
[embedded `TestApp`](../components/application/testing/embedded-app.md).
