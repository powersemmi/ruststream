---
sidebar_position: 2
title: assert_called(n)
description: "assert_called(n) checks that exactly n messages were published to the destination."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# `assert_called(n)`

`assert_called(n)` checks that exactly `n` messages were published to the destination.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/testing_published.rs#assert-called"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/testing_published.rs#assert-called"
```

</TabItem>
</Tabs>

## How it works

The count is of published messages, so one delivery that publishes three times counts three.
A subscriber's `assert_called(n)` counts handler calls instead: the example asserts one call and three publishes.
The assertion panics with both numbers when they differ, and returns the record for further checks.

## See also

- [`assert_called_once`](./assert-called-once.md), [`assert_not_called`](./assert-not-called.md)
- [`messages`](./messages.md): the messages behind the count.
- [`PublishedAssertions::assert_called`](api:ruststream::testing::PublishedAssertions::assert_called)
