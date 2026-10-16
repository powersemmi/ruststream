---
sidebar_position: 15
title: assert_batch_sizes
description: "assert_batch_sizes(&[..]) checks how a subscriber's deliveries were cut into handler calls."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# `assert_batch_sizes`

`assert_batch_sizes(&[..])` checks how a subscriber's deliveries were cut into handler calls.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/testing_subscriber_payloads.rs#assert_batch_sizes"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/testing_subscriber_payloads.rs#assert_batch_sizes"
```

</TabItem>
</Tabs>

## How it works

Each number is the size of one call, in call order.
The mount names the largest size, `batch(nonzero!(2))`; the broker builds the batches, and this reads back what it built.
One batch is one call, so five orders in batches of two make `assert_called(3)`.
A single-message handler is called per delivery, so three deliveries read `[1, 1, 1]`.
The batches come from a replayed log, as on the `batches` page.

## See also

- [`batches`](./batches.md): the content of each batch.
- [`assert_called(n)`](./assert-called.md): the number of calls.
- [`settle`](../driving/settle.md): drive a batch built from a bare publisher.
- [`SubscriberAssertions::assert_batch_sizes`](api:ruststream::testing::SubscriberAssertions::assert_batch_sizes)
