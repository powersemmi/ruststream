---
sidebar_position: 3
title: message
description: "message(&v) starts an input with the publish builder the service itself publishes through: destination, headers and codec."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# message

[`message(&v)`](api:ruststream::testing::TestApp::message) starts an input with the publish builder the service itself publishes through: destination, headers and codec.

The value is an `Outgoing` type, and the type decides whether the builder still asks for a destination.
A type that names its destination publishes there; a type that names none takes `to(name)`.
`with_headers(&meta)` adds a typed header contract, and `with_codec(codec)` encodes with another codec than the default one.
[`publish()`](./publish.md) sends the input and drives the reaction, and [`with`](../subscriber-assertions/with.md) checks the value the handler received.
`TestApp::message` publishes onto the app's only broker.
With more than one broker it has no single target, so the publish fails with `TestError::Ambiguous`.
`tb.broker::<B>().message(..)`, the [`BrokerHandle::message`](api:ruststream::testing::BrokerHandle::message) of a broker chosen with [`broker::<B>()`](./broker.md) or [`broker_named`](./broker-named.md), publishes onto that broker.
A value whose type the test does not own goes through the [shortcuts](./publish-shortcuts.md).

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/testing_driving.rs#message"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/testing_driving.rs#message"
```

</TabItem>
</Tabs>
