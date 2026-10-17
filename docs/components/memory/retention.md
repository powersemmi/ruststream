---
sidebar_position: 1
title: Retention
description: "A retaining MemoryBroker keeps the newest messages of every name within a bound, and its subscriptions replay over them."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Retention

A retaining [`MemoryBroker`](api:ruststream::memory::MemoryBroker) keeps the newest messages of every name within a bound, and its subscriptions replay over them.

[`MemoryBroker::retaining`](api:ruststream::memory::MemoryBroker::retaining) takes a [`Retention`](api:ruststream::memory::Retention) bound, applied to each name on its own.
`Retention::Messages(n)` keeps the newest `n` messages.
`Retention::Bytes(n)` keeps the newest payloads that fit in `n` bytes.
`Retention::MessagesAndBytes { .. }` applies both.
A broker that publishes under a thousand names keeps up to that much for each of them.
The newest message always stays, so a payload larger than a byte bound is kept alone.
The retaining broker is a type of its own, `MemoryBroker<Retaining>`, and only its subscriptions implement [`Seekable`](api:ruststream::Seekable).
A [`start_at`](../subscribers/replay/start-at.md) or a [seek](../subscribers/replay/seek.md) therefore does not compile on `MemoryBroker::new()`, which keeps nothing.
A [`MemoryPosition`](api:ruststream::memory::MemoryPosition) names a place in the log of one name: `start()` is the oldest message still kept, `sequence(n)` the `n`-th message ever published to the name, and `end()` the tip.
A sequence number keeps naming the same message as the bound evicts older ones.
A sequence the bound has already evicted is refused with `MemoryError::PositionEvicted`, which names the oldest position left.
A [pattern subscription](./patterns.md) does not replay, because the log is kept per name.

## Example

The broker keeps two messages of each name; three published before the service starts leave the last two to replay.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/memory_retention.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/memory_retention.rs#app"
```

</TabItem>
</Tabs>
