---
sidebar_position: 4
title: Batches
description: "A handler over &[T] runs once per batch the broker delivers; the mount site names the batch size, and the body settles the batch or each element."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Batches

A handler over `&[T]` runs once per batch the broker delivers; the mount site names the batch size, and the body settles the batch or each element.

The slice parameter is the whole declaration: a `&[T]` handler is a batch handler, and a `&T` handler is the single-message form of [`#[subscriber]`](./index.md).
[`batch(n)`](api:ruststream::runtime::SubscriberSettings::batch), one of the mount-site [settings](./mount-settings.md), caps each batch at `n` messages, and a batch handler does not mount without it.
The broker builds the batches, natively or through the framework's client-side buffer, and the body sees each batch as delivered.
A batch may hold fewer than `n` messages.
The batch's return value settles every element with one of the [outcomes](./messages/handler-outcome.md): [`HandlerOutcome`](api:ruststream::runtime::HandlerOutcome), `()`, `Result<(), E>` or `Result<HandlerOutcome, E>`.
A `Vec<HandlerOutcome>` settles each element on its own, [`retry_after`](./messages/retry-after.md) included; a vector of the wrong length is logged, and the elements it leaves unsettled are retried.
An element that fails to decode is settled by the decode failure policy and never reaches the body.
The body gets one context per batch, with the broker's subscription-scoped fields only.
Per-message headers come with the elements when the parameter is [`&[Message<H, T>]`](api:ruststream::runtime::Message).
App-wide and router middleware wrap single-message handlers and do not apply to batch handlers.
In the manual form the body is an `impl Handle<[T]>`; `Ok(())` acks the batch and `Err(outcomes)` settles element by element.
A test reads the batches with [`batches`](../application/testing/subscriber-assertions/batches.md) and checks their sizes with [`assert_batch_sizes`](../application/testing/subscriber-assertions/assert-batch-sizes.md).

## Example

One outcome settles the whole batch.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/subscriber_batch.rs#batch"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/subscriber_batch.rs#batch"
```

</TabItem>
</Tabs>

## One outcome per element

A `Vec<HandlerOutcome>` settles element `i` with outcome `i`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/subscriber_batch.rs#per_element"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/subscriber_batch.rs#per_element"
```

</TabItem>
</Tabs>
