---
sidebar_position: 2
title: published
description: "BrokerHandle::published opens the record of one destination on a broker: every message published there, with its name and headers."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# published

[`published::<T>(name)`](api:ruststream::testing::BrokerHandle::published) opens the record of one destination on a broker: every message published there, with its name and headers.

`T` is the payload type that [`with`](../published-assertions/with.md) and [`decoded`](../published-assertions/decoded.md) decode to; the count and raw assertions ignore it.
The assertions are those of [`PublishedAssertions`](api:ruststream::testing::PublishedAssertions), such as [`assert_called_once`](../published-assertions/assert-called-once.md).
In process the record is the broker's own publish log, so it holds replies, `Out` slot publishes, retry copies and the test's own publishes.
[`out::<M>()`](./out.md) shows what left through one slot, across brokers, and [`subscriber(name)`](./subscriber.md) is the record of a handler.
Against a running stand it holds the test's publishes and everything a publisher wired by the runtime sent.
The record is a snapshot taken when the method is called: call it after the publish that drives the reaction.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/testing_records.rs#published"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/testing_records.rs#published"
```

</TabItem>
</Tabs>
