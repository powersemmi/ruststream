---
sidebar_position: 2
title: seek
description: "A handler moves its own subscription to another position of the log through the broker's seek handle."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# seek

A handler moves its own subscription to another position of the log through the broker's seek handle.

The in-memory broker's per-delivery [context](../context/delivery-context.md) carries the handle under the key [`SeekHandle`](api:ruststream::memory::SeekHandle).
A handler reads it with the [`Ctx<SeekHandle>`](api:ruststream::runtime::Ctx) parameter; the manual form declares `MemoryContext` on its `Handle` and reads `ctx.context(SeekHandle)`.
`seek` takes a [`MemoryPosition`](api:ruststream::memory::MemoryPosition), the same type [`start_at`](./start-at.md) takes.
Once `seek` resolves, the next delivery comes from the new position, and entries already queued before it are skipped.
A sequence the retention bound has already evicted is refused with an error, and the example retries the delivery then.
The handle implements [`Seeker`](api:ruststream::Seeker), the trait a broker with a replayable log provides.
`Ctx<SeekHandle>` does not compile against `MemoryBroker::new()`, which keeps no log.

## Example

The handler meets a poison marker and moves its subscription past the entries behind it.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/seek.rs#seek"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/seek.rs#seek"
```

</TabItem>
</Tabs>
