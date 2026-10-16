---
sidebar_position: 1
title: assert_called_once
description: "assert_called_once checks that a subscriber's handler ran exactly once: one delivery, or one batch."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# `assert_called_once`

`assert_called_once` checks that a subscriber's handler ran exactly once: one delivery, or one batch.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/testing_subscriber_calls.rs#assert_called_once"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/testing_subscriber_calls.rs#assert_called_once"
```

</TabItem>
</Tabs>

## How it works

It counts handler calls, not messages: a batch handler called with five orders counts once.
A delivery that did not decode counts too: the record holds it although the body never ran.
It returns the assertions, so `with`, `with_raw` and `settled` chain after it and read that one call.
On failure it panics with the number of calls it found.

## See also

- [`assert_called(n)`](./assert-called.md): the same count for any number.
- [`assert_not_called`](./assert-not-called.md): no call at all.
- [`subscriber(name)`](../records/subscriber.md): the record these assertions read.
- [`SubscriberAssertions::assert_called_once`](api:ruststream::testing::SubscriberAssertions::assert_called_once)
