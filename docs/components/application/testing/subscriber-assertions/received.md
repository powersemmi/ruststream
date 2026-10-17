---
sidebar_position: 7
title: received
description: "received::<T>() returns every payload a subscriber received, decoded with the default codec, for a check of your own."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# received

[`received::<T>()`](api:ruststream::testing::SubscriberAssertions::received) returns every payload a subscriber received, decoded with the default codec, for a check of your own.

The list is flat and in delivery order: a batch contributes each of its elements.
[`batches`](./batches.md) keeps the same payloads grouped by call.
It decodes with `DefaultCodec`; [`received_with(&codec)`](api:ruststream::testing::SubscriberAssertions::received_with) decodes with another codec.
Every recorded payload must decode as `T`, so a delivery that failed to decode makes it panic.
It fits a check that a chain of assertions cannot say: an order, a subset, a property of every element.
[`with`](./with.md) checks the latest payload only, and [`received_raw`](./received-raw.md) returns the same list as bytes.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/testing_subscriber_payloads.rs#received"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/testing_subscriber_payloads.rs#received"
```

</TabItem>
</Tabs>
