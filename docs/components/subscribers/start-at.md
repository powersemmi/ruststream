---
sidebar_position: 5
title: Start position and seeking
description: "start_at opens a replayable subscription at a position, and a handler repositions its own subscription through the broker's seek handle."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Start position and seeking

`start_at` opens a replayable subscription at a position, and a handler repositions its own subscription through the broker's seek handle.

## Example

The subscription opens at the start of the log and replays what was published before the service started.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/seek.rs#start_at"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/seek.rs#start_at"
```

</TabItem>
</Tabs>

## Seeking from the body

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

## How it works

A broker over a replayable log owns its position type; for the in-memory broker it is `MemoryPosition`.
`start_at(position)` seeks the subscription before its first delivery, on every startup.
Without it, the subscription opens at the broker's default; the in-memory broker's default is the tip of the log, so earlier entries are not delivered.
`MemoryPosition::start()` is the oldest entry the broker still keeps, `sequence(n)` the `n`-th entry published to the name, counted from zero, and `end()` the tip.
A handler reads the seek handle with the `Ctx<SeekHandle>` parameter; the manual form declares `MemoryContext` on its `Handle` and reads `ctx.context(SeekHandle)`.
Once `seek` resolves, the next delivery comes from the new position, and entries already queued before it are skipped.
A sequence the retention bound has already evicted is refused with an error, and the example retries the delivery then.
The in-memory broker keeps a log only when it is built with `MemoryBroker::retaining`.
`start_at` and `Ctx<SeekHandle>` do not compile against `MemoryBroker::new()`, which keeps nothing.

## See also

- [Mount-site settings](./mount-settings.md): `start_at` at the mount site.
- [Context and `Ctx<K>`](./context/context-and-ctx.md): broker fields as parameters.
- [`SubscriberSettings::start_at`](api:ruststream::runtime::SubscriberSettings::start_at), [`MemoryBroker::retaining`](api:ruststream::memory::MemoryBroker::retaining), [`MemoryPosition`](api:ruststream::memory::MemoryPosition), [`SeekHandle`](api:ruststream::memory::SeekHandle), [`Ctx`](api:ruststream::runtime::Ctx), [`Seekable`](api:ruststream::Seekable), [`Seeker`](api:ruststream::Seeker)
