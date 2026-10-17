---
sidebar_position: 5
title: Schemas
description: "A message type that derives JsonSchema gives its message a payload schema in the document."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Schemas

A message type that derives `JsonSchema` gives its message a payload schema in the document.

The crate re-exports `schemars` as `ruststream::schemars`, so a service needs no dependency of its own for the derive.
The schema is JSON Schema draft 07, the draft the AsyncAPI Schema Object extends.
Doc comments on the type and its fields become descriptions in the schema, and the type's doc comment is the message's description.
`serde` attributes shape the schema as they shape the wire: a renamed field or an `Option` reads the same in both.
On the attribute path the derive is optional.
A type without it still mounts, and its message carries no payload schema.
[`build_spec`](./build-spec.md) then logs a warning naming the subscription and the type, and [`messages_without_schema`](./messages-without-schema.md) lists the message.
A `subscriber(..)` registration demands the derive at compile time; [`undocumented`](./undocumented.md) lifts the demand from one registration.
A type that carries its own bytes, on a [byte lane](../../subscribers/codecs/bytes.md), has no schema by design and is not listed as missing one.
The example runs with `cargo test --example asyncapi_schemas --features macros,memory,asyncapi`.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/asyncapi_schemas.rs#schema"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/asyncapi_schemas.rs#schema"
```

</TabItem>
</Tabs>
