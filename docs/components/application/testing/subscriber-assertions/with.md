---
sidebar_position: 4
title: with
description: "with(&value) checks that the latest handler call received this value, decoded with the default codec."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# with

[`with(&value)`](api:ruststream::testing::SubscriberAssertions::with) checks that the latest handler call received this value, decoded with the default codec.

It reads the most recent call only; the earlier ones are in [`received`](./received.md).
It decodes the recorded payload with [`DefaultCodec`](api:ruststream::codec::DefaultCodec), the codec the enabled features select (`json` first).
A handler mounted with another codec is checked with [`with_codec(&codec, &value)`](api:ruststream::testing::SubscriberAssertions::with_codec), which decodes with that codec.
The value type needs `Deserialize`, `PartialEq` and `Debug`.
The latest call must hold one delivery: on a batch of several it panics, and the batch is read with `batches`.
[`with_raw`](./with-raw.md) checks the same call as bytes, and [`with_value`](./with-value.md) the value a carrying delivery lent the handler.
A destination's [`with`](../published-assertions/with.md) checks the value a publish carried.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/testing_subscriber_payloads.rs#with"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/testing_subscriber_payloads.rs#with"
```

</TabItem>
</Tabs>
