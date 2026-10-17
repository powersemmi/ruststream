---
sidebar_position: 6
title: settle
description: "TestApp::settle drives a reaction the test started without the harness to a standstill, without publishing anything new."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# settle

[`TestApp::settle`](api:ruststream::testing::TestApp::settle) drives a reaction the test started without the harness to a standstill, without publishing anything new.

A [publish](./publish.md) through the harness settles for you; `settle` is for input that arrived some other way, such as a bare publisher of the broker.
A batch handler needs it: each harness publish settles alone, so the batch it reaches holds one message.
Two messages sent through a bare publisher wait together, and `settle` delivers them as one batch, which [`assert_batch_sizes`](../subscriber-assertions/assert-batch-sizes.md) reads back.
In process `settle` waits until the transports report nothing in flight.
In live mode it waits until every subscription has handled what the harness saw published to it, up to the deadline of `start_live`.
[`advance`](./advance.md) settles after a delay has passed.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/testing_driving.rs#settle"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/testing_driving.rs#settle"
```

</TabItem>
</Tabs>
