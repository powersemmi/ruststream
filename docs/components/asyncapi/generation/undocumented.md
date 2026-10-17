---
sidebar_position: 9
title: undocumented
description: ".undocumented() takes the schemas of one manual registration out of the document and lifts its JsonSchema demand."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# undocumented

`.undocumented()` takes the schemas of one manual registration out of the document and lifts its `JsonSchema` demand.

Under the `asyncapi` feature a `subscriber(..)` registration is documented by default.
Mounting it demands `JsonSchema` of its message types, and a type without the derive is a compile error at the mount.
[`undocumented`](api:ruststream::runtime::SubscriberBuilder::undocumented) is the exit for one registration, such as one whose message type comes from a crate that ships no schema.
The registration stays in the document: its channel and its operation remain, and its messages carry no payload schema.
[`messages_without_schema`](./messages-without-schema.md) lists those messages.
The step is written once; a second one does not compile.
On the attribute path a type without the derive already mounts and contributes no schema, so the attribute has no such step.
Without the `asyncapi` feature a registration demands nothing.
The example runs with `cargo test --example asyncapi_schemas --features macros,memory,asyncapi`.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/asyncapi_schemas.rs#audit"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/asyncapi_schemas.rs#undocumented"
```

</TabItem>
</Tabs>
