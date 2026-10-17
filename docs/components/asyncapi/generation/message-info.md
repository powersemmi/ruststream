---
sidebar_position: 6
title: MessageInfo
description: "MessageInfo names the component of a message in the document and describes it, whatever the Rust type is called."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# MessageInfo

`MessageInfo` names the component of a message in the document and describes it, whatever the Rust type is called.

Channels and operations refer to a message by its component name.
Without `MessageInfo` that name is the schema's title, which `schemars` takes from the type's name, so renaming the type renames the published message.
An impl of [`MessageInfo`](api:ruststream::MessageInfo) keeps the name: `NAME` is the component's name and `DESCRIPTION` its description.
`#[derive(MessageInfo)]` writes the impl from the type's name and doc comment.
With `#[message(headers(Meta))]` the derive also declares the type's [header contract](../reports/header-schemas.md).
`#[derive(Outgoing)]` implements `MessageInfo` for the types it derives.
Without the macros a registration names the component by the schema's title, so `#[schemars(rename = "..")]` keeps the published name there.
The example runs with `cargo test --example asyncapi_schemas --features macros,memory,asyncapi`.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/asyncapi_schemas.rs#message_info"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/asyncapi_schemas.rs#message_info"
```

</TabItem>
</Tabs>
