---
sidebar_position: 7
title: received
description: "received::<T>() returns every payload a subscriber received, decoded with the default codec, for a check of your own."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# `received`

`received::<T>()` returns every payload a subscriber received, decoded with the default codec, for a check of your own.

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

## How it works

The list is flat and in delivery order: a batch contributes each of its elements.
It decodes with `DefaultCodec`; `received_with(&codec)` decodes with another codec.
Every recorded payload must decode as `T`, so a delivery that failed to decode makes it panic.
It fits a check that a chain of assertions cannot say: an order, a subset, a property of every element.

## See also

- [`with`](./with.md): the latest payload only.
- [`received_raw`](./received-raw.md): the same list as bytes.
- [`batches`](./batches.md): the same payloads grouped by call.
- [`SubscriberAssertions::received`](api:ruststream::testing::SubscriberAssertions::received), [`SubscriberAssertions::received_with`](api:ruststream::testing::SubscriberAssertions::received_with)
