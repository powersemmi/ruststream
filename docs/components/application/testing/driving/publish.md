---
sidebar_position: 4
title: publish
description: "publish() sends the input onto the broker and returns once the whole reaction to it has settled, so the next line can assert."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# publish

`publish()` sends the input onto the broker and returns once the whole reaction to it has settled, so the next line can assert.

The input, built with [`message(&v)`](./message.md), enters the broker the way an external producer's message would.
The reaction covers the handlers it reaches, their replies and slot publishes, and the handlers those reach, across brokers.
In the example the receipt the first handler replies with is filed before `publish` returns, and [`with`](../subscriber-assertions/with.md) reads the value the handler received.
A reaction that does not settle within 10,000 dispatched deliveries fails the publish with [`TestError::NotQuiescent`](api:ruststream::testing::TestError), which catches an endless requeue.
A publish after a fail-fast failure stopped the [`TestApp`](api:ruststream::testing::TestApp) fails with `TestError::ShutDown`.
[`settle`](./settle.md) drives a reaction the test started some other way, and [`advance`](./advance.md) lets a delayed redelivery come due.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/testing_driving.rs#publish"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/testing_driving.rs#publish"
```

</TabItem>
</Tabs>
