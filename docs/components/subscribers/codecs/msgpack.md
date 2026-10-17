---
sidebar_position: 3
title: MessagePack
description: "MsgpackCodec reads and writes MessagePack through rmp-serde; a struct travels as an array of its field values. It is on with the msgpack feature."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# MessagePack

[`MsgpackCodec`](api:ruststream::codec::MsgpackCodec) reads and writes MessagePack through `rmp-serde`; a struct travels as an array of its field values. It is on with the `msgpack` feature.

A message type derives the same `serde` traits as for JSON.
A struct is written as an array, in the order of its fields, so both ends must declare the fields in the same order.
Decoding accepts a struct written as a map of field names too.
A peer that expects maps, such as a service in a dynamic language, needs a [codec of your own](./custom-codec.md); its example is that codec.
The media type is `application/msgpack`.
The example names the codec on the scope with [`with_broker_codec`](api:ruststream::runtime::RustStream::with_broker_codec); with `msgpack` as the only codec feature, [`DefaultCodec`](api:ruststream::codec::DefaultCodec) is `MsgpackCodec`.
A payload in another format is a decode failure, settled by the subscriber's [failure policy](../failure-policy.md).

## Example

The example builds with MessagePack as the only codec:

```text
cargo test --example codec_msgpack --no-default-features --features testing,macros,memory,msgpack
```

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/codec_msgpack.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/codec_msgpack.rs#app"
```

</TabItem>
</Tabs>
