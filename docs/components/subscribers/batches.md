---
sidebar_position: 4
title: Batch handlers
description: "A handler over &[T] runs once per batch the broker delivers; the mount site names the batch size, and the body settles the batch or each element."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Batch handlers

A handler over `&[T]` runs once per batch the broker delivers; the mount site names the batch size, and the body settles the batch or each element.

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

## How it works

The slice parameter is the whole declaration: a `&[T]` handler is a batch handler.
`batch(n)` at the mount site caps each batch at `n` messages, and a batch handler does not mount without it.
The broker builds the batches, natively or through the framework's client-side buffer, and the body sees each batch as delivered.
A batch may hold fewer than `n` messages.
The batch's return value settles every element: `HandlerOutcome`, `()`, `Result<(), E>` or `Result<HandlerOutcome, E>`.
A `Vec<HandlerOutcome>` settles each element on its own; a vector of the wrong length is logged, and the elements it leaves unsettled are retried.
An element that fails to decode is settled by the decode failure policy and never reaches the body.
The body gets one context per batch, with the broker's subscription-scoped fields only.
Per-message headers come with the elements when the parameter is `&[Message<H, T>]`.
App-wide and router middleware wrap single-message handlers and do not apply to batch handlers.
In the manual form the body is an `impl Handle<[T]>`; `Ok(())` acks the batch and `Err(outcomes)` settles element by element.

## See also

- [`#[subscriber]`](./subscriber.md): the single-message form.
- [Mount-site settings](./mount-settings.md): `batch` among the other settings.
- [Ack, retry, drop](./messages/handler-outcome.md) and [`retry_after`](./messages/retry-after.md): the outcomes a batch returns.
- [`assert_batch_sizes`](../application/testing/subscriber-assertions/assert-batch-sizes.md) and [`batches`](../application/testing/subscriber-assertions/batches.md): reading batches in a test.
- [`SubscriberSettings::batch`](api:ruststream::runtime::SubscriberSettings::batch), [`HandlerOutcome`](api:ruststream::runtime::HandlerOutcome), [`Message`](api:ruststream::runtime::Message)
