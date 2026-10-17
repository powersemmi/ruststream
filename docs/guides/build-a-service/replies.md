---
title: Replies and publishing to other topics
description: "accept answers each order with a payment request and tells the customer through an Out slot; a router says how both leave."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Replies and publishing to other topics

`accept` answers each order with a payment request and tells the customer through an `Out` slot; a router says how both leave.

A subscriber publishes in two ways.
A [reply](../../components/subscribers/publishing/replies.md) is the value the body returns: the runtime encodes it and publishes it after the body succeeds.
An [`Out` slot](../../components/subscribers/publishing/out-slots.md) is a publisher the body receives as a parameter and calls itself, as many times as it needs.
Where a message goes is a property of its type: [`#[outgoing(name = ..)]`](../../components/subscribers/publishing/outgoing.md) fixes the topic, so neither the body nor the mount site repeats it.
A slot lists the types that may leave through it, so publishing anything else does not compile.
The mount site binds each position to a publish policy, here the broker's plain `Publish`.

## The messages

Payments receives a `PaymentRequest`; `Paid` is what it answers in the next chapter.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="payments.rs" reference="examples/guide/payments.rs#request"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="payments.rs" reference="examples/manual/guide/payments.rs#request"
```

</TabItem>
</Tabs>

A `Notice` is what the shop tells a customer, and the [`OutSlot`](api:ruststream::OutSlot) marker `Notices` is the slot it leaves through.
The manual form writes what `#[derive(OutSlot)]` generates: the slot's name, the types it publishes, and their entries in the AsyncAPI document.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="notifications.rs" reference="examples/guide/notifications.rs#notice"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="notifications.rs" reference="examples/manual/guide/notifications.rs#notice"
```

</TabItem>
</Tabs>

## The subscriber

`accept` drops an order with no lines, since there is nothing to charge, and an order past the shop's bounds, since it has no total.
It tells the customer the order arrived, through the slot, and asks for a retry if that publish fails.
Then it returns the payment request, which the runtime publishes as the reply.
`Ok` publishes the reply and acks; `Err` publishes nothing and settles by the outcome.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="orders.rs" reference="examples/guide/orders.rs#accept"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="orders.rs" reference="examples/manual/guide/orders.rs#accept"
```

</TabItem>
</Tabs>

In the manual form the slot is an entry of [`Outs`](api:ruststream::runtime::Outs), and `outs.get(Notices)` hands out its publisher.

## The router

A [`Router`](api:ruststream::runtime::Router) collects one module's subscribers with the settings of their positions.
[`out_reply`](api:ruststream::runtime::Mounting::out_reply) binds the reply to a policy, and `.codec(INTERNAL)` encodes it as CBOR.
[`out`](api:ruststream::runtime::Mounting::out) binds the slot; it keeps the router's codec, which is JSON here.
The router names the broker and the app's state type in [`RouterDef<MemoryBroker, Shop>`](api:ruststream::runtime::RouterDef); chapter 7 introduces `Shop`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="orders.rs" reference="examples/guide/orders.rs#routes"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="orders.rs" reference="examples/manual/guide/orders.rs#routes"
```

</TabItem>
</Tabs>

The manual form binds each subscriber to its topic with [`subscriber(..)`](api:ruststream::runtime::subscriber), and `.describe(..)` gives it the description the doc comment gives the macro.
[`include_router`](api:ruststream::runtime::BrokerScope::include_router) mounts the router on the broker; chapter 5 shows the app that does it.
