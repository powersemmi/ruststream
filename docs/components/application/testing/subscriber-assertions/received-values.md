---
sidebar_position: 9
title: received_values
description: "received_values::<T>() returns every value a subscriber's deliveries lent its handler, batch elements included."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# received_values

[`received_values::<T>()`](api:ruststream::testing::SubscriberAssertions::received_values) returns every value a subscriber's deliveries lent its handler, batch elements included.

It is the counterpart of [`received`](./received.md) for a broker whose deliveries carry a value ([`Carries<T>`](api:ruststream::Carries)) instead of bytes to decode.
The list is flat and in delivery order.
It panics when a delivery lent no `T`.
The row subscription here is the in-memory stand-in described on the [`with_value`](./with-value.md) page.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/testing_subscriber_carried.rs#received_values"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/testing_subscriber_carried.rs#received_values"
```

</TabItem>
</Tabs>
