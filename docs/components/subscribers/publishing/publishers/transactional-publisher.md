---
sidebar_position: 2
title: TransactionalPublisher
description: "TransactionalPublisher puts several publishes in one broker transaction: every reply of a batch, or a scope a handler opens on its slot."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# TransactionalPublisher

[`TransactionalPublisher`](api:ruststream::TransactionalPublisher) puts several publishes in one broker transaction: every reply of a batch, or a scope a handler opens on its slot.

Messages published inside a transaction become visible together on commit, or not at all.
A [batch handler](../../batches.md) whose reply chain ends with [`.transactional()`](api:ruststream::runtime::Mounting::transactional) publishes every reply of the batch in one transaction, and acks the batch after the commit.
A policy whose live publisher has no transactions does not compile there.
A broker's prelude names its transactional policy `TransactionalPublish`.

By hand, a handler bounds its slot `Out<impl TransactionalPublisher, Marker>` and calls `begin()`.
It returns a [`TransactionScope`](api:ruststream::runtime::TransactionScope) that owns the transaction, and publishes go through the scope's [builder](../publish-builder.md).
`commit()` and `abort()` consume the scope, so a second commit or a publish after settling does not compile.
A scope dropped unsettled logs a warning and leaves the broker transaction open, so a handler settles every scope it opens.
The scope sends straight into the transaction: the slot's [transforms](../transforms/publish-transform.md) and the app's [publish layers](../../middleware/publish-layer.md) do not run on its messages.
This is the borrowed kind of transaction: the publisher carries one broker transaction, and one scope is open on it at a time.
Kafka-like brokers, whose client holds one transaction per producer, offer this kind only.
Brokers whose transactions are client buffers also offer [`OwnedTransactions`](./owned-transactions.md).

## Example

Every confirmation of a batch in one transaction.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/transactional_publisher.rs#batch"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/transactional_publisher.rs#batch"
```

</TabItem>
</Tabs>

A scope opened on a slot: both entries of a payment, or neither.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/transactional_publisher.rs#scope"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/transactional_publisher.rs#scope"
```

</TabItem>
</Tabs>

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/transactional_publisher.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/transactional_publisher.rs#app"
```

</TabItem>
</Tabs>
