---
sidebar_position: 2
title: assert_called
description: "assert_called(n) checks that exactly n messages were published to the destination."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# assert_called

[`assert_called(n)`](api:ruststream::testing::PublishedAssertions::assert_called) checks that exactly `n` messages were published to the destination.

The count is of published messages, so one delivery that publishes three times counts three.
A subscriber's [`assert_called(n)`](../subscriber-assertions/assert-called.md) counts handler calls instead: the example asserts one call and three publishes.
The assertion panics with both numbers when they differ, and returns the record for further checks.
[`assert_called_once`](./assert-called-once.md) and [`assert_not_called`](./assert-not-called.md) cover one message and none, and [`messages`](./messages.md) returns the messages behind the count.

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
