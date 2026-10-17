---
sidebar_position: 2
title: Slots
description: "An Out slot hands a handler a live publisher as a parameter: one marker per slot, bound to a publish policy at the mount site."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Slots

An [`Out`](api:ruststream::runtime::Out) slot hands a handler a live publisher as a parameter: one marker per slot, bound to a publish policy at the mount site.

A slot is for a destination chosen at run time, or for several destinations at once, another broker's included.
`Out(out): Out<impl Publisher>` names a capability, never a broker's publisher type, so the same handler mounts on a production broker and on its in-process test transport.
The capability is [`Publisher`](./publishers/publisher.md), or a narrower one: [`TransactionalPublisher`](./publishers/transactional-publisher.md), [`OwnedTransactions`](./publishers/owned-transactions.md) or [`RequestReply`](./publishers/request-reply.md).
A single unnamed slot is [`DefaultSlot`](api:ruststream::runtime::DefaultSlot).
A handler with several slots names a marker per parameter: a unit struct that derives [`OutSlot`](api:ruststream::OutSlot).
Its `#[publishes(..)]` list is what may leave through the slot, and what the generated AsyncAPI document reports for it.
An optional third position, `Out<impl Publisher, Marker, (A, B)>`, narrows what this handler publishes through the slot.

The mount site binds each marker with [`.out(marker, policy)`](api:ruststream::runtime::Mounting::out), in any order, and commits the chain with `.build()`.
A [transform](./transforms/publish-transform.md) written after an `.out(..)` belongs to that slot.
A slot that is never bound, a marker the handler does not declare, and a publish of a type outside the list do not compile.
A body publishes through the slot with the [publish builder](./publish-builder.md).
A slot may also take a reply: one handler returns its answer and publishes through slots beside it.
The runtime's own retry copies leave through one more slot, [`Retry`](../retry-destination.md).

## Example

One unnamed slot.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/out_slots.rs#forward"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/out_slots.rs#forward"
```

</TabItem>
</Tabs>

Two named slots, each with what it may publish.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/out_slots.rs#slots"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/out_slots.rs#slots"
```

</TabItem>
</Tabs>

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/out_slots.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/out_slots.rs#app"
```

</TabItem>
</Tabs>
