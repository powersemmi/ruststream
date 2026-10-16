---
sidebar_position: 3
title: assert_not_called
description: "assert_not_called checks that nothing was published to the destination."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# `assert_not_called`

`assert_not_called` checks that nothing was published to the destination.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/testing_published.rs#assert-not-called"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/testing_published.rs#assert-not-called"
```

</TabItem>
</Tabs>

## How it works

The assertion passes on an empty record and panics with the count otherwise.
It returns nothing, so it ends the chain.
A handler that settles with `HandlerOutcome::drop()` publishes no reply, which is what the example checks.

## See also

- [`assert_called_once`](./assert-called-once.md), [`assert_called(n)`](./assert-called.md)
- [`published::<T>(name)`](../records/published.md): the record this reads.
- [`PublishedAssertions::assert_not_called`](api:ruststream::testing::PublishedAssertions::assert_not_called)
