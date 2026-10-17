---
sidebar_position: 4
title: Content
description: "Each message names the media type of the codec that reads or writes it; the root defaultContentType appears only when all of them agree."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Content

Each message names the media type of the codec that reads or writes it; the root `defaultContentType` appears only when all of them agree.

The media type is the codec's [`CONTENT_TYPE`](api:ruststream::codec::Codec::CONTENT_TYPE): `application/json`, `application/cbor` or `application/msgpack`.
It follows the [codec](../../subscribers/codecs/index.md) chosen for the mounting, at whichever level chose it.
A message that no codec reads or writes names none: a type that carries its own bytes, on a [byte lane](../../subscribers/codecs/bytes.md).
A reader applies the root `defaultContentType` to each message that names no media type.
So it stands only while every message names the same one; a second format, or a message without one, leaves it out.
A message shared by several registrations takes the media type of the first.

## Example

The example needs the `cbor` feature:

```text
cargo test --example asyncapi_content_type --features macros,memory,asyncapi,cbor
```

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/asyncapi_content_type.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/asyncapi_content_type.rs#app"
```

</TabItem>
</Tabs>
