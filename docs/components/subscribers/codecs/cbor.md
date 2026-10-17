---
sidebar_position: 2
title: CBOR
description: "CborCodec reads and writes CBOR through ciborium, a compact binary format with the data model of JSON; it is on with the cbor feature."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# CBOR

[`CborCodec`](api:ruststream::codec::CborCodec) reads and writes CBOR through `ciborium`, a compact binary format with the data model of JSON; it is on with the `cbor` feature.

A message type derives the same `serde` traits as for JSON, and a struct keeps its field names on the wire.
The media type is `application/cbor`.
The example names the codec on the scope with [`with_broker_codec`](api:ruststream::runtime::RustStream::with_broker_codec), so it holds whichever other codec features are on.
With `cbor` as the only codec feature, [`DefaultCodec`](api:ruststream::codec::DefaultCodec) is `CborCodec`, and a mounting needs to name nothing.
A payload in another format is a decode failure, settled by the subscriber's [failure policy](../failure-policy.md).

## Example

The example builds with CBOR as the only codec:

```text
cargo test --example codec_cbor --no-default-features --features testing,macros,memory,cbor
```

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/codec_cbor.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/codec_cbor.rs#app"
```

</TabItem>
</Tabs>
