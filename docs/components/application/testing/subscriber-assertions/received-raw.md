---
sidebar_position: 8
title: received_raw
description: "received_raw() returns the bytes of every payload a subscriber received, in delivery order."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# received_raw

[`received_raw()`](api:ruststream::testing::SubscriberAssertions::received_raw) returns the bytes of every payload a subscriber received, in delivery order.

It returns one `Bytes` per delivery; a batch contributes each of its elements.
No codec runs, so it reads payloads that are no model: here frames the handler takes as a `Deserialized` view.
The test publishes such bytes through a `#[derive(Outgoing, Serialized)]` newtype, which keeps every codec off them.
[`with_raw`](./with-raw.md) checks the latest payload only, and [`batches_raw`](./batches-raw.md) groups the same bytes by call.

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
