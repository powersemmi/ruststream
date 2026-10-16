---
sidebar_position: 2
title: assert_called(n)
description: "assert_called(n) checks that a subscriber's handler ran exactly n times, counting a batch as one call."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# `assert_called(n)`

`assert_called(n)` checks that a subscriber's handler ran exactly `n` times, counting a batch as one call.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/testing_subscriber_calls.rs#assert_called"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/testing_subscriber_calls.rs#assert_called"
```

</TabItem>
</Tabs>

## How it works

`n` is the number of handler calls.
A single-message handler is called once per delivery, and once more for each redelivery.
A batch handler is called once per batch, so here `n` counts batches; `assert_batch_sizes` shows how big each one was.
A publish through the harness runs to completion before it returns, so three publishes make three calls.
On failure it panics with the number of calls it found.

## See also

- [`assert_called_once`](./assert-called-once.md): the same check for one call.
- [`assert_batch_sizes`](./assert-batch-sizes.md): the size of each call of a batch handler.
- [`outcomes`](./outcomes.md): how each of those calls settled.
- [Published `assert_called(n)`](../published-assertions/assert-called.md): the count of publishes.
- [`SubscriberAssertions::assert_called`](api:ruststream::testing::SubscriberAssertions::assert_called)
