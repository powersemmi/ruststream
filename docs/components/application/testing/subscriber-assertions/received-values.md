---
sidebar_position: 9
title: received_values
description: "received_values::<T>() returns every value a subscriber's deliveries lent its handler, batch elements included."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# `received_values`

`received_values::<T>()` returns every value a subscriber's deliveries lent its handler, batch elements included.

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

## How it works

It is the counterpart of `received` for a broker whose deliveries carry a value (`Carries<T>`) instead of bytes to decode.
The list is flat and in delivery order.
It panics when a delivery lent no `T`.
The row subscription here is the in-memory stand-in described on the `with_value` page.

## See also

- [`with_value`](./with-value.md): the latest lent value only.
- [`received`](./received.md): the same list for decoded payloads.
- [`SubscriberAssertions::received_values`](api:ruststream::testing::SubscriberAssertions::received_values), [`Carries`](api:ruststream::Carries)
