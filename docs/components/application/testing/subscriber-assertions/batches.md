---
sidebar_position: 10
title: batches
description: "batches::<T>() returns what a batch handler received, one decoded list per call."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# batches

[`batches::<T>()`](api:ruststream::testing::SubscriberAssertions::batches) returns what a batch handler received, one decoded list per call.

Each inner list is one handler call, in call order, so the batch boundaries that [`received`](./received.md) drops stay.
It decodes with `DefaultCodec`; [`batches_with(&codec)`](api:ruststream::testing::SubscriberAssertions::batches_with) decodes with another codec.
A publish through the harness runs each message to completion on its own, so it reaches a batch handler as a batch of one.
Fuller batches come from a log that already holds the messages: the test fills a retaining broker, the subscription opens at the start of its log, and [`tb.settle()`](api:ruststream::testing::TestApp::settle) drives the replay.
The [`settle`](../driving/settle.md) page shows a batch built from a bare publisher.
The mount names the size, `batch(nonzero!(2))`, so three orders arrive as two and one.
[`batches_raw`](./batches-raw.md) returns the same batches as bytes, and [`assert_batch_sizes`](./assert-batch-sizes.md) checks only the sizes.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/testing_subscriber_payloads.rs#batches"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/testing_subscriber_payloads.rs#batches"
```

</TabItem>
</Tabs>
