---
sidebar_position: 1
title: JSON
description: "JsonCodec reads and writes JSON through serde_json; it is the default codec, on with the json feature."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# JSON

[`JsonCodec`](api:ruststream::codec::JsonCodec) reads and writes JSON through `serde_json`; it is the default codec, on with the `json` feature.

The `json` feature is a default feature of the crate.
While it is on, [`DefaultCodec`](api:ruststream::codec::DefaultCodec) is `JsonCodec`, so a handler mounted with no codec named decodes JSON, and a reply is encoded in JSON.
A message type derives `serde::Deserialize`; a type the handler publishes derives `Serialize` as well.
The media type is `application/json`, and the generated AsyncAPI document reports it for each message.
A payload that is not JSON, or JSON of another shape, is a decode failure, settled by the subscriber's [failure policy](../failure-policy.md).
To name JSON where another codec is the scope's, mount through a router or a registration that names it, as on [Codecs](./index.md).

On a device without the standard library (`target_os = "none"`) the type and the wire format are the same.
Decoding stays `serde_json`, on the allocator, and allocates only scratch for a string with escapes.
Encoding goes through `serde-json-slice`, which writes the bytes `serde_json` writes straight into the encode buffer, with no allocation of its own.
The buffer grows as a `BytesMut` does, so a message is not cut at a size.
The same service source builds with either.

## Example

The example builds with JSON as the only codec:

```text
cargo test --example codec_json --no-default-features --features testing,macros,memory,json
```

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/codec_json.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/codec_json.rs#app"
```

</TabItem>
</Tabs>
