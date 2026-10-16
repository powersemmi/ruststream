---
sidebar_position: 1
title: assert_called_once
description: "assert_called_once checks that exactly one message was published to the destination."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# `assert_called_once`

`assert_called_once` checks that exactly one message was published to the destination.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/testing_published.rs#assert-called-once"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/testing_published.rs#assert-called-once"
```

</TabItem>
</Tabs>

## How it works

The assertion counts the messages in the record, not the handler calls that produced them.
It panics with the count it found when the record holds none or more than one.
It returns the record, so `with`, `with_raw` and `with_header` chain after it.

## See also

- [`assert_called(n)`](./assert-called.md): any other count.
- [`assert_not_called`](./assert-not-called.md): nothing published.
- [`published::<T>(name)`](../records/published.md): the record this reads.
- [`PublishedAssertions::assert_called_once`](api:ruststream::testing::PublishedAssertions::assert_called_once)
