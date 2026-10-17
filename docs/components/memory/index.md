---
title: Memory broker
description: "MemoryBroker is a complete broker inside the service's process, and the reference implementation of the broker contract."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Memory broker

[`MemoryBroker`](api:ruststream::memory::MemoryBroker) is a complete broker inside the service's process, and the reference implementation of the broker contract.

It comes with the `memory` feature and needs no server.
It suits a queue that belongs to one process, a prototype, or local development; the default `cargo generate` template starts on it.
[`MemoryBroker::new`](api:ruststream::memory::MemoryBroker::new) does no I/O, like every broker constructor.
A broker built with it keeps nothing: a message lives until the last subscriber has read it, so memory holds the work the handlers have yet to do.
A broker that keeps the newest messages for replay is built with [`retaining`](./retention.md).
A mount site imports [`ruststream::memory::prelude`](api:ruststream::memory::prelude), one glob with the core prelude, the broker's own types, and its publish policies under the names every broker crate uses: `Publish`, `TransactionalPublish` and `Request`.
`Partitioned` stays out of the glob, because in scope it makes `msg.partition_key()` ambiguous; a service that reads partition keys imports it.
A handler body that names capabilities, not policies, keeps `ruststream::prelude::*`.

A subscription by name reads messages published to that exact name, from the moment it opens.
Every subscription of a name receives every message published to it.
[`MemorySource`](api:ruststream::memory::MemorySource) is the broker's [subscription descriptor](../subscribers/subscription-source.md), and [`MemoryPattern`](./patterns.md) reads every name a pattern matches.
An ack or a drop consumes a delivery; a retry, or a delivery left without a settlement, comes back to the same subscriber.
A [`retry_after`](../subscribers/messages/retry-after.md) delivery comes back to the same subscriber after its delay, and nothing is republished.
Every delivery reports how many times the broker has handed it to that subscriber, so a registration's [attempt cap](../subscribers/retry-cap.md) counts these redeliveries.
`MemoryBroker` is a shared handle: its clones work on one bus.
After shutdown, a publish, a commit or a request through a handle taken earlier returns an error.

## Example

The dependency line:

```toml
ruststream = { version = "0.7", features = ["macros", "memory", "json"] }
```

Two subscriptions to `orders` each receive the order, and one of them replies over the same bus.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/memory_broker.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/memory_broker.rs#app"
```

</TabItem>
</Tabs>

## Capabilities

The broker implements the optional capabilities on its own in-process semantics.

- [Request and reply](../subscribers/publishing/publishers/request-reply.md): `broker.requester()` gives a `MemoryRequester`, which names a unique reply name in the `reply-to` header and waits for the first message delivered there. `Request` is its policy.
- [Batches](../subscribers/batches.md): a batch is the first delivery to arrive plus everything already buffered, up to the size the registration names.
- Transactions: the publisher behind `Publish` implements [`TransactionalPublisher`](../subscribers/publishing/publishers/transactional-publisher.md) and [`OwnedTransactions`](../subscribers/publishing/publishers/owned-transactions.md). A commit delivers the buffered messages at once, in publish order; an abort discards them.
- Partition keys: a `MemoryMessage` reads its key from the `partition-key` header.
- Replay: on a [retaining](./retention.md) broker, a subscription opens at a [start position](../subscribers/replay/start-at.md) and a handler [seeks](../subscribers/replay/seek.md).

The [`TestApp`](../application/testing/index.md) harness runs an application on this broker as it runs one on a networked broker, and its publishes wait on exactly the subscriptions the broker's routing picks.
