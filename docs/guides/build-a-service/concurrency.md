---
title: "Concurrency: workers and threads"
description: "Packing computes, so it gets two threads of its own; booking a courier waits on the network, so four bookings run at once."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Concurrency: workers and threads

Packing computes, so it gets two threads of its own; booking a courier waits on the network, so four bookings run at once.

A subscription handles one delivery at a time by default, in the order the broker hands them over.
[Workers](../../components/subscribers/dispatch/workers.md) let it handle several at once on the app's runtime: the right tool for a handler that waits on I/O.
[Threads](../../components/subscribers/dispatch/threads.md) give it threads of its own: the right tool for a handler that computes, since it would otherwise hold up the runtime that serves every other subscription.
Both are settings of the subscription, and the handler's body does not change.
A subscription with workers or threads no longer keeps its deliveries in order; `workers(n, by_key)` keeps the order of each key.

## Packing on threads

`pack` splits a paid order into parcels and answers with the plan.
A paid order past the shop's bounds is dropped before any parcel is planned.
`threads(2)` starts two threads with the subscription; the deliveries go to them in turn.
Decoding, the body, the reply and the settlement all run on the thread that got the delivery.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="shipping.rs" reference="examples/guide/shipping.rs#pack"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="shipping.rs" reference="examples/manual/guide/shipping.rs#pack"
```

</TabItem>
</Tabs>

## Booking on workers

`dispatch` books a courier for a packed order and tells the customer the tracking code.
`workers(4)` keeps up to four bookings in flight; the next delivery waits for a free worker.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="shipping.rs" reference="examples/guide/shipping.rs#dispatch"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="shipping.rs" reference="examples/manual/guide/shipping.rs#dispatch"
```

</TabItem>
</Tabs>

## The router

The shipping router reads the shop's own topics, so it decodes CBOR.
A reply takes the default codec unless its position names one, so `pack`'s reply names CBOR.
A slot takes the router's codec unless it names one, so the notices name JSON.
In the manual form the two settings are steps of the mount chain: [`threads`](api:ruststream::runtime::SubscriberSettings::threads) and [`workers`](api:ruststream::runtime::SubscriberSettings::workers).

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="shipping.rs" reference="examples/guide/shipping.rs#routes"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="shipping.rs" reference="examples/manual/guide/shipping.rs#routes"
```

</TabItem>
</Tabs>

Threads pay off from about ten microseconds of computation per delivery; on a lighter handler they cost more than they save.
[Poll diagnostics](../../components/subscribers/dispatch/poll-diagnostics.md) find the handlers that hold up the runtime.
