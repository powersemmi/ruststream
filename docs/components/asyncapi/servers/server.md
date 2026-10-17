---
sidebar_position: 2
title: Server
description: "RustStream::server declares a server the document lists, from a ServerSpec that says how clients reach the broker."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Server

`RustStream::server` declares a server the document lists, from a `ServerSpec` that says how clients reach the broker.

[`server`](api:ruststream::runtime::RustStream::server) is for a broker no registration of the process describes, or to replace what a [labeled broker](./with-broker-labeled.md) says.
The name is the server's key in the document.
[`ServerSpec::new`](api:ruststream::ServerSpec::new) takes the host and the protocol.
[`ServerSpec::from_url`](api:ruststream::ServerSpec::from_url) takes a broker URL and keeps only its host and port, so credentials in the URL never reach the document, which is published.
[`protocol_version`](api:ruststream::ServerSpec::protocol_version) names the version clients speak, where one protocol name covers incompatible versions, as AMQP 0.9.1 and 1.0 do.
[`description`](api:ruststream::ServerSpec::description) adds a note for the reader.
A server spec holds coordinates, never configuration.
How clients authenticate is [security](./security-schemes.md), and what the protocol adds is [bindings](./bindings.md).
The example runs with `cargo test --example asyncapi_servers --features macros,memory,asyncapi`.

## Example

{/* single-form: a server spec is a plain value, the same with and without the macros */}
```rust reference="examples/asyncapi_servers.rs#server"
```

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/asyncapi_servers.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/asyncapi_servers.rs#app"
```

</TabItem>
</Tabs>
