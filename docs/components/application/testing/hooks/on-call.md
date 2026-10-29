---
sidebar_position: 1
title: on_call
description: "on_call runs an async closure each time a subscription's handler is called, handing it the subscription's assertions as they stand at that moment."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# on_call

[`on_call(hook)`](api:ruststream::testing::SubscriberAssertions::on_call) runs an async closure each time a subscription's handler is called, handing it the subscription's assertions as they stand at that moment.

The hook runs once per delivery, or once per batch, after the call settled.
It gets the same [`SubscriberAssertions`](api:ruststream::testing::SubscriberAssertions) a test reads, with the new call already in the record, so [`assert_called`](../subscriber-assertions/assert-called.md) and [`settled`](../subscriber-assertions/settled.md) work inside it.
The publish that caused the call returns only after the hook finished.
A hook that never finishes therefore hangs that publish; the test's timeout is the guard.
A failed assertion inside the hook fails the publish, `settle` or `advance` that drove the call.
A hook applies from the moment it is registered, and several hooks on one subscription run in the order they were registered.
It runs the same way under [`start_live`](../starting-and-stopping/start-live.md).

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/testing_hooks.rs#on-call"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/testing_hooks.rs#on-call"
```

</TabItem>
</Tabs>
