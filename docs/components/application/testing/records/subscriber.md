---
sidebar_position: 1
title: subscriber
description: "BrokerHandle::subscriber opens the record of one subscription: what its handler received and how each delivery settled."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# subscriber

[`subscriber(name)`](api:ruststream::testing::BrokerHandle::subscriber) opens the record of one subscription: what its handler received and how each delivery settled.

The name is the subscription name the handler mounts under, here `"orders"`.
The record holds every call of the handler in order, with its raw input and its settlement.
Its assertions are those of [`SubscriberAssertions`](api:ruststream::testing::SubscriberAssertions), such as [`assert_called_once`](../subscriber-assertions/assert-called-once.md), [`with`](../subscriber-assertions/with.md) and [`settled`](../subscriber-assertions/settled.md).
Each assertion reads the record at the moment it runs, so a test asserts after the publish that drives the reaction.
A name no handler subscribes to has an empty record, so `assert_not_called` passes on a misspelled name.
A destination has its own record, [`published::<T>(name)`](./published.md), and an `Out` slot has [`out::<M>()`](./out.md).

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/testing_records.rs#subscriber"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/testing_records.rs#subscriber"
```

</TabItem>
</Tabs>
