---
sidebar_position: 1
title: "Ack, retry, drop"
description: "What the handler body returns settles the delivery: a HandlerOutcome, nothing at all, or a Result."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Ack, retry, drop

What the handler body returns settles the delivery: a `HandlerOutcome`, nothing at all, or a `Result`.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/handler_outcome.rs#outcome"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/handler_outcome.rs#outcome"
```

</TabItem>
</Tabs>

## Nothing to decide

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/handler_outcome.rs#unit"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/handler_outcome.rs#unit"
```

</TabItem>
</Tabs>

## An error drops the delivery

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/handler_outcome.rs#result"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/handler_outcome.rs#result"
```

</TabItem>
</Tabs>

## An outcome or an error

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/handler_outcome.rs#result_outcome"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/handler_outcome.rs#result_outcome"
```

</TabItem>
</Tabs>

## How it works

`HandlerOutcome::ack()` acknowledges the delivery, and the broker forgets it.
`HandlerOutcome::retry()` asks for it again at once; on the in-memory broker it returns to the same subscriber.
`HandlerOutcome::drop()` rejects it without a redelivery; a broker with a dead-letter policy of its own applies it.
`HandlerOutcome::retry_after(delay)` is the fourth settlement, described on its own page.
A body that returns `()` acks every delivery.
A body that returns `Result<(), E>` acks on `Ok` and drops on `Err`; the error value is discarded, so log it in the body when it matters.
A body that returns `Result<HandlerOutcome, E>` settles by the inner outcome on `Ok` and drops on `Err`.
A batch body answers the same way, with one `HandlerOutcome` for the whole slice or a `Vec<HandlerOutcome>` with one per element.
A manual body always returns `Result<(), HandlerOutcome>`: `Ok(())` acks, and `Err(outcome)` settles by that outcome.
A `retry()` with no cap declared comes back for as long as the body keeps asking; [Attempt cap and dead letter](../retry-cap.md) ends that.

## See also

- [Subscribers](../index.md): the handler these return types belong to.
- [`retry_after`](./retry-after.md): the delivery back after a delay.
- [`and_after` and post-settle hooks](./and-after.md): work that runs once the settlement is through.
- [Failure policy](../failure-policy.md): the settlement of a panic or of a payload that does not decode.
- [`outcomes`](../../application/testing/subscriber-assertions/outcomes.md): the settlements a test reads.
- [`HandlerOutcome`](api:ruststream::runtime::HandlerOutcome), [`HandlerOutcome::ack`](api:ruststream::runtime::HandlerOutcome::ack), [`HandlerOutcome::retry`](api:ruststream::runtime::HandlerOutcome::retry), [`HandlerOutcome::drop`](api:ruststream::runtime::HandlerOutcome::drop)
