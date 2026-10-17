---
sidebar_position: 1
title: Workers
description: "workers(n) handles up to n deliveries of one subscription at once; workers(n, by_key) keeps the deliveries of one key in order."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Workers

`workers(n)` handles up to `n` deliveries of one subscription at once; `workers(n, by_key)` keeps the deliveries of one key in order.

The subscription starts `n` long-lived workers, tasks of the runtime the app runs on.
A free worker takes the next delivery.
While `n` deliveries are in flight, the subscription does not read the broker, so back-pressure holds.
Deliveries finish in any order: the subscription's global order is lost.
Workers suit a handler that waits on I/O; a handler that computes belongs on [threads](./threads.md).
The setting is a clause of the attribute, or [`.workers(n)`](api:ruststream::runtime::SubscriberSettings::workers) among the mount-site [settings](../mount-settings.md).
The count is a `NonZeroUsize`, and [`nonzero!`](api:ruststream::nonzero) writes one.
A router chain takes a [`Workers`](api:ruststream::runtime::Workers) value through [`Router::workers`](api:ruststream::runtime::Router::workers).
Handing a delivery to a worker allocates nothing.
A batch handler gets a pool of batches, each batch in a task of its own.
On shutdown, the deliveries in flight finish within the app's [`shutdown_timeout`](api:ruststream::runtime::RustStream::shutdown_timeout).

## Example

Four charges that each wait a second on a payment provider take one second together.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/workers.rs#workers"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/workers.rs#workers"
```

</TabItem>
</Tabs>

## By key

`workers(n, by_key)` makes the same `n` workers into `n` sequential lanes.
A delivery goes to the lane picked by hashing its partition key, which the broker's message reports through [`Partitioned`](api:ruststream::Partitioned).
Deliveries with one key never reorder, and deliveries with different keys run at once.
A delivery without a key rotates over the lanes.
The in-memory broker reads the key from the [`partition-key`](api:ruststream::memory::PARTITION_KEY_HEADER) header.
At the mount site the setting is [`.workers_by_key(n)`](api:ruststream::runtime::SubscriberSettings::workers_by_key).

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/workers.rs#workers_by_key"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/workers.rs#workers_by_key"
```

</TabItem>
</Tabs>
