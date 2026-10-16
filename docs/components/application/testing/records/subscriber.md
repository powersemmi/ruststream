---
sidebar_position: 1
title: subscriber(name)
description: "BrokerHandle::subscriber opens the record of one subscription: what its handler received and how each delivery settled."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# `subscriber(name)`

`subscriber(name)` opens the record of one subscription: what its handler received and how each delivery settled.

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

## How it works

The name is the subscription name the handler mounts under, here `"orders"`.
The record holds every call of the handler in order, with its raw input and its settlement.
Each assertion reads the record at the moment it runs, so a test asserts after the publish that drives the reaction.
A name no handler subscribes to has an empty record, so `assert_not_called` passes on a misspelled name.

## See also

- [`published::<T>(name)`](./published.md): the record of a destination.
- [`out::<M>()`](./out.md): the record of one `Out` slot.
- [`assert_called_once`](../subscriber-assertions/assert-called-once.md), [`with`](../subscriber-assertions/with.md), [`settled`](../subscriber-assertions/settled.md): what the record lets a test say.
- [`BrokerHandle::subscriber`](api:ruststream::testing::BrokerHandle::subscriber), [`SubscriberAssertions`](api:ruststream::testing::SubscriberAssertions)
