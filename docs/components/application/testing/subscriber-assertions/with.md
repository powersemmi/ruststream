---
sidebar_position: 4
title: with
description: "with(&value) checks that the latest handler call received this value, decoded with the default codec."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# `with`

`with(&value)` checks that the latest handler call received this value, decoded with the default codec.

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

## How it works

It reads the most recent call only; the earlier ones are in `received`.
It decodes the recorded payload with `DefaultCodec`, the codec the enabled features select (`json` first).
A handler mounted with another codec is checked with `with_codec(&codec, &value)`, which decodes with that codec.
The value type needs `Deserialize`, `PartialEq` and `Debug`.
The latest call must hold one delivery: on a batch of several it panics, and the batch is read with `batches`.

## See also

- [`with_raw`](./with-raw.md): the same call as bytes.
- [`received`](./received.md): every decoded payload, not only the latest.
- [`with_value`](./with-value.md): the value a carrying delivery lent the handler.
- [`SubscriberAssertions::with`](api:ruststream::testing::SubscriberAssertions::with), [`SubscriberAssertions::with_codec`](api:ruststream::testing::SubscriberAssertions::with_codec), [`DefaultCodec`](api:ruststream::codec::DefaultCodec)
