---
sidebar_position: 3
title: message(&v)
description: "message(&v) starts an input with the publish builder the service itself publishes through: destination, headers and codec."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# `message(&v)`

`message(&v)` starts an input with the publish builder the service itself publishes through: destination, headers and codec.

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

## How it works

The value is an `Outgoing` type, and the type decides whether the builder still asks for a destination.
A type that names its destination publishes there; a type that names none takes `to(name)`.
`with_headers(&meta)` adds a typed header contract, and `with_codec(codec)` encodes with another codec than the default one.
`TestApp::message` publishes onto the app's only broker.
With more than one broker it has no single target, so the publish fails with `TestError::Ambiguous`.
`tb.broker::<B>().message(..)` publishes onto the broker the test chose.

## See also

- [`publish()`](./publish.md): send the input and drive the reaction.
- [Publish shortcuts](./publish-shortcuts.md): a one-call publish of a value the test does not own.
- [`broker::<B>()`](./broker.md), [`broker_named`](./broker-named.md): choose the broker.
- [`TestApp::message`](api:ruststream::testing::TestApp::message), [`BrokerHandle::message`](api:ruststream::testing::BrokerHandle::message)
