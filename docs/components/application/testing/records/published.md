---
sidebar_position: 2
title: published::<T>(name)
description: "BrokerHandle::published opens the record of one destination on a broker: every message published there, with its name and headers."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# `published::<T>(name)`

`published::<T>(name)` opens the record of one destination on a broker: every message published there, with its name and headers.

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

## How it works

`T` is the payload type that `with` and `decoded` decode to; the count and raw assertions ignore it.
In process the record is the broker's own publish log, so it holds replies, `Out` slot publishes, retry copies and the test's own publishes.
Against a running stand it holds the test's publishes and everything a publisher wired by the runtime sent.
The record is a snapshot taken when the method is called: call it after the publish that drives the reaction.

## See also

- [`subscriber(name)`](./subscriber.md): the record of a handler.
- [`out::<M>()`](./out.md): what left through one slot, across brokers.
- [`assert_called_once`](../published-assertions/assert-called-once.md), [`with`](../published-assertions/with.md), [`decoded`](../published-assertions/decoded.md): what the record lets a test say.
- [`BrokerHandle::published`](api:ruststream::testing::BrokerHandle::published), [`PublishedAssertions`](api:ruststream::testing::PublishedAssertions)
