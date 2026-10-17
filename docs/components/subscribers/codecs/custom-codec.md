---
sidebar_position: 4
title: Codec
description: "A codec of your own implements the Codec trait over any serde format and mounts wherever a built-in codec does."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Codec

A codec of your own implements the [`Codec`](api:ruststream::codec::Codec) trait over any serde format and mounts wherever a built-in codec does.

The trait has two required methods: `encode` writes a value into a new buffer, and `decode` reads a value from the delivery's bytes.
[`encode_into`](api:ruststream::codec::Codec::encode_into) writes into a buffer the caller passes.
The publish path calls it for a broker that only reads the payload, and inside a dispatch loop that buffer is reused, so a codec that implements it allocates nothing per publish.
Its default encodes into a buffer of its own and copies.
Both sides return a [`CodecError`](api:ruststream::codec::CodecError): the format's error goes in `CodecError::Decode` or `CodecError::Encode` as the source.
`CONTENT_TYPE` is the media type the generated AsyncAPI document reports.

A codec generic over another composes with it: the inner codec decides the format, and the wrapper changes the bytes around it, such as a version byte in front.
Both methods are synchronous, so a codec can only use what it holds and the bytes at hand.
Work that needs I/O, such as a schema registry lookup, goes around the codec: in the broker's delivery path on the way in, and in a [`PublishLayer`](../middleware/publish-layer.md) on the way out.

The example writes MessagePack with field names: a struct travels as a map, the form a peer in a dynamic language reads, where the built-in [MessagePack](./msgpack.md) codec writes an array.
It uses `rmp-serde`, the crate behind the `msgpack` feature.

## Example

The example builds with `msgpack` as the only codec feature:

```text
cargo test --example custom_codec --no-default-features --features testing,macros,memory,msgpack
```

{/* single-form: a Codec impl is plain Rust; the macros do not write one */}
```rust reference="examples/custom_codec.rs#codec"
```

The codec mounts at the scope and at the reply position, like a built-in one.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/custom_codec.rs#mount"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/custom_codec.rs#mount"
```

</TabItem>
</Tabs>
