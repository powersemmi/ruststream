---
sidebar_position: 1
title: broker::<B>()
description: "TestApp::broker addresses one broker of the app by its production type, for publishing to it and reading its records."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# `broker::<B>()`

`TestApp::broker` addresses one broker of the app by its production type, for publishing to it and reading its records.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/testing_driving.rs#broker"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/testing_driving.rs#broker"
```

</TabItem>
</Tabs>

## How it works

`B` is the type the app registered, so `MemoryBroker` and `MemoryBroker<Retaining>` are two different brokers.
The handle it returns publishes onto that broker and opens its records: `subscriber(name)` and `published::<T>(name)`.
A subscription name is local to its broker: the `"orders"` of one broker is not the `"orders"` of the other.
`broker` panics when the app has no broker of type `B` or more than one; two brokers of one type are addressed by label.
In live mode the test's input reaches the broker through its connected form's default publish policy.

## See also

- [`broker_named`](./broker-named.md): address a broker by its label.
- [`message(&v)`](./message.md): what the handle publishes.
- [`subscriber(name)`](../records/subscriber.md), [`published::<T>(name)`](../records/published.md): what the handle reads.
- [`TestApp::broker`](api:ruststream::testing::TestApp::broker), [`BrokerHandle`](api:ruststream::testing::BrokerHandle)
