---
sidebar_position: 3
title: assert_not_called
description: "assert_not_called checks that a subscriber's handler never ran."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# `assert_not_called`

`assert_not_called` checks that a subscriber's handler never ran.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/testing_subscriber_calls.rs#assert_not_called"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/testing_subscriber_calls.rs#assert_not_called"
```

</TabItem>
</Tabs>

## How it works

It passes when the record of that subscription is empty.
A name no handler subscribes to has an empty record as well, so a misspelled name passes.
A positive check on a neighbour guards against that: here `assert_called_once` on `"orders"` shows the order did arrive.
The handler sends only an order without an id on to `"rejected"`, so a valid order leaves the review subscription untouched.
It returns nothing, so it ends a chain.

## See also

- [`assert_called_once`](./assert-called-once.md), [`assert_called(n)`](./assert-called.md): the counts that expect calls.
- [`subscriber(name)`](../records/subscriber.md): the record these assertions read.
- [`SubscriberAssertions::assert_not_called`](api:ruststream::testing::SubscriberAssertions::assert_not_called)
