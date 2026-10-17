---
sidebar_position: 3
title: OwnedTransactions
description: "OwnedTransactions opens independent transactions that own their buffers, so a handler keeps several open on one slot at once."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# OwnedTransactions

[`OwnedTransactions`](api:ruststream::OwnedTransactions) opens independent transactions that own their buffers, so a handler keeps several open on one slot at once.

A broker whose transactions are client buffers flushed at commit offers this kind: an AMQP confirms buffer, a Redis pipeline, the memory broker.
A handler bounds its slot `Out<impl OwnedTransactions, Marker>` and calls `transaction()`.
Each call returns a [`TypedTransaction`](api:ruststream::runtime::TypedTransaction) of its own, and settling one never touches another.
Publishes go through the transaction's [builder](../publish-builder.md) and stay in its buffer until `commit()` flushes them together.
`commit()` and `abort()` consume the value, so a publish after settling does not compile.
A transaction dropped unsettled discards its buffer, the way an abort does, and logs a warning.
The transaction sends from its own buffer: the slot's [transforms](../transforms/publish-transform.md) and the app's [publish layers](../../middleware/publish-layer.md) do not run on its messages, and a test reads them on the destination record, [`published`](../../../application/testing/records/published.md), rather than the slot's.
Kafka-like brokers offer only the borrowed kind, [`TransactionalPublisher`](./transactional-publisher.md).

## Example

A transfer books its entries in one transaction and its audit record in another, so an empty transfer keeps the audit record and books nothing.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/owned_transactions.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/owned_transactions.rs#handler"
```

</TabItem>
</Tabs>

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/owned_transactions.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/owned_transactions.rs#app"
```

</TabItem>
</Tabs>
