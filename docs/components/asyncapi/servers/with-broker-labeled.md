---
sidebar_position: 1
title: with_broker_labeled
description: "with_broker_labeled registers a broker under a label that is its server name in the document, and the broker describes the server."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# with_broker_labeled

`with_broker_labeled` registers a broker under a label that is its server name in the document, and the broker describes the server.

[`with_broker_labeled`](api:ruststream::runtime::RustStream::with_broker_labeled) takes a broker that implements [`DescribeServer`](api:ruststream::DescribeServer); every shipped broker does.
The broker fills the server entry in its protocol's vocabulary: host, protocol, protocol version and [bindings](./bindings.md).
The in-memory broker describes an in-process server over the `memory` protocol, with no host.
The label is also the broker's identity in the service; a test addresses the broker by it with [`broker_named`](../../application/testing/driving/broker-named.md).
The registration's channels list the server, and its operations carry the server's protocol as a tag, so a document with several brokers reads by transport.
A document with one server puts every channel on it.
With several, the channels of an unlabeled registration list none, which AsyncAPI reads as "on every server".
An explicit [`server`](./server.md) under the same label takes precedence over what the broker says.
[`with_broker_labeled_codec`](api:ruststream::runtime::RustStream::with_broker_labeled_codec) also names the scope's codec.
The example runs with `cargo test --example asyncapi_servers --features macros,memory,asyncapi`.

## Example

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
