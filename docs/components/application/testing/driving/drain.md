---
sidebar_position: 8
title: drain
description: "TestApp::drain waits for the post-settle continuations of everything settled so far, for a test that asserts on their effects."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# drain

[`TestApp::drain`](api:ruststream::testing::TestApp::drain) waits for the post-settle continuations of everything settled so far, for a test that asserts on their effects.

A continuation is work a handler attaches to its outcome, [`ack().and_after(..)`](api:ruststream::runtime::HandlerOutcome::and_after), or to its context with `after(..)` or `after_settle(..)`.
It runs on its own task after the delivery settles, so a [`publish`](./publish.md) can return before it has finished.
A settlement registers its continuations before it releases the delivery, so `drain` after a publish finds them all.
An effect of the handler body itself needs no `drain`: the publish has already settled it.
In the example the test builds the state with [`with_state`](../starting-and-stopping/with-state.md) to read the mail client's record afterwards.
[`shutdown`](../starting-and-stopping/shutdown.md) waits for continuations too.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/testing_driving.rs#drain"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/testing_driving.rs#drain"
```

</TabItem>
</Tabs>
