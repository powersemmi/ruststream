---
sidebar_position: 1
title: build_spec
description: "build_spec reads the AsyncAPI document of a service from its app value, the same value main runs."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# build_spec

[`build_spec`](api:ruststream::asyncapi::build_spec) reads the AsyncAPI document of a service from its app value, the same value `main` runs.

The document follows AsyncAPI 3.1.
It is read from what the app registers, so it describes the handlers the service mounts and nothing else.
Each subscriber becomes a channel named after its subscription, a `receive` operation, and a message component for its input type.
The handler's doc comment is the operation's description, and the input type's doc comment is the message's.
A [reply](../reports/replies.md) rides the `receive` operation, and each message an `Out` slot declares gets a `send` operation.
The result is a [`Spec`](api:ruststream::asyncapi::Spec): plain data with public fields, which a test reads and a tool may complete before it is [serialized](./to-json-yaml.md).
The document is built when the function is called, so it costs the running service nothing.
The `asyncapi` feature turns it on.
The example runs with `cargo test --example asyncapi_generation --features macros,memory,asyncapi`.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/asyncapi_generation.rs#spec"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/asyncapi_generation.rs#spec"
```

</TabItem>
</Tabs>
