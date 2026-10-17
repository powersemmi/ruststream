---
sidebar_position: 1
title: start_at
description: "start_at opens a replayable subscription at a position: the start of the log, a sequence number, or the tip."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# start_at

[`start_at`](api:ruststream::runtime::SubscriberSettings::start_at) opens a replayable subscription at a position: the start of the log, a sequence number, or the tip.

A broker over a replayable log owns its position type; for the in-memory broker it is [`MemoryPosition`](api:ruststream::memory::MemoryPosition).
`start_at(position)` seeks the subscription before its first delivery, on every startup.
It is one of the mount-site [settings](../mount-settings.md), and the attribute takes it as a clause too.
Without it, the subscription opens at the broker's default; the in-memory broker's default is the tip of the log, so earlier entries are not delivered.
`MemoryPosition::start()` is the oldest entry the broker still keeps, `sequence(n)` the `n`-th entry published to the name, counted from zero, and `end()` the tip.
The in-memory broker keeps a log only when it is built with [`MemoryBroker::retaining`](api:ruststream::memory::MemoryBroker::retaining).
`start_at` does not compile against `MemoryBroker::new()`, which keeps nothing.
A broker's subscription accepts a position when it implements [`Seekable`](api:ruststream::Seekable).

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
