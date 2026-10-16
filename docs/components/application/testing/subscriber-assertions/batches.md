---
sidebar_position: 10
title: batches
description: "batches::<T>() returns what a batch handler received, one decoded list per call."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# `batches`

`batches::<T>()` returns what a batch handler received, one decoded list per call.

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

## How it works

Each inner list is one handler call, in call order, so the batch boundaries that `received` drops stay.
It decodes with `DefaultCodec`; `batches_with(&codec)` decodes with another codec.
A publish through the harness runs each message to completion on its own, so it reaches a batch handler as a batch of one.
Fuller batches come from a log that already holds the messages: the test fills a retaining broker, the subscription opens at the start of its log, and `tb.settle()` drives the replay.
The mount names the size, `batch(nonzero!(2))`, so three orders arrive as two and one.

## See also

- [`batches_raw`](./batches-raw.md): the same batches as bytes.
- [`assert_batch_sizes`](./assert-batch-sizes.md): only the sizes.
- [`received`](./received.md): the same payloads as one flat list.
- [`SubscriberAssertions::batches`](api:ruststream::testing::SubscriberAssertions::batches), [`SubscriberAssertions::batches_with`](api:ruststream::testing::SubscriberAssertions::batches_with), [`TestApp::settle`](api:ruststream::testing::TestApp::settle)
