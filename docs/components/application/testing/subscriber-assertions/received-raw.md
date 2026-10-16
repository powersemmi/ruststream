---
sidebar_position: 8
title: received_raw
description: "received_raw() returns the bytes of every payload a subscriber received, in delivery order."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# `received_raw`

`received_raw()` returns the bytes of every payload a subscriber received, in delivery order.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/testing_subscriber_payloads.rs#received_raw"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/testing_subscriber_payloads.rs#received_raw"
```

</TabItem>
</Tabs>

## How it works

It returns one `Bytes` per delivery; a batch contributes each of its elements.
No codec runs, so it reads payloads that are no model: here frames the handler takes as a `Deserialized` view.
The test publishes such bytes through a `#[derive(Outgoing, Serialized)]` newtype, which keeps every codec off them.

## See also

- [`with_raw`](./with-raw.md): the latest payload only.
- [`batches_raw`](./batches-raw.md): the same bytes grouped by call.
- [`SubscriberAssertions::received_raw`](api:ruststream::testing::SubscriberAssertions::received_raw)
