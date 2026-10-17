---
sidebar_position: 7
title: messages_without_schema
description: "messages_without_schema lists the messages the document describes with no payload schema, for a check to fail on."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# messages_without_schema

[`messages_without_schema`](api:ruststream::asyncapi::Spec::messages_without_schema) lists the messages the document describes with no payload schema, for a check to fail on.

A message without a schema documents its channel but not its shape, and a client generator has no type to produce from it.
`build_spec` logs one warning per gap when it builds the document; this method returns the same list as component names.
A test or a release check asserts on it.
Messages whose bytes are their own format, on a [byte lane](../../subscribers/codecs/bytes.md), are left out: they have no schema by design.
On the attribute path a gap is a message type without a [`JsonSchema`](./schemas.md) derive.
A `subscriber(..)` registration has one only after [`undocumented`](./undocumented.md).
The example runs with `cargo test --example asyncapi_schemas --features macros,memory,asyncapi`.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/asyncapi_schemas.rs#gaps"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/asyncapi_schemas.rs#gaps"
```

</TabItem>
</Tabs>
