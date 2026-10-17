---
sidebar_position: 2
title: broker_named
description: "TestApp::broker_named addresses a broker by the label it was registered under, which tells apart two brokers of one type."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# broker_named

[`TestApp::broker_named`](api:ruststream::testing::TestApp::broker_named) addresses a broker by the label it was registered under, which tells apart two brokers of one type.

The label is the one [`with_broker_labeled`](api:ruststream::runtime::RustStream::with_broker_labeled) gives the broker in the app.
The handle is the one [`broker::<B>()`](./broker.md) returns: it publishes onto that broker and reads its records, such as [`subscriber(name)`](../records/subscriber.md).
A publish onto one broker reaches only that broker's subscriptions.
In the example [`with`](../subscriber-assertions/with.md) shows which handler received the value.
`broker_named` panics when no broker carries the label.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/testing_driving.rs#broker-named"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/testing_driving.rs#broker-named"
```

</TabItem>
</Tabs>
