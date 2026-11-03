---
sidebar_position: 2
title: retry_after
description: "retry_after(delay) asks for the delivery back no sooner than the delay, for a condition that will pass."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# retry_after

[`retry_after(delay)`](api:ruststream::runtime::HandlerOutcome::retry_after) asks for the delivery back no sooner than the delay, for a condition that will pass.

It is one of the [outcomes](./handler-outcome.md), next to an immediate `retry()`.
A broker with native delayed redelivery holds the delivery and returns it after the delay.
The in-memory broker is one: the delivery comes back to the same subscriber, and nothing is published.
On a broker without it, the runtime drops the delivery and publishes a copy back to the subscription after the delay, through the [`Retry`](../retry-destination.md) slot.
That copy carries the [`RETRY_COUNT_HEADER`](api:ruststream::runtime::RETRY_COUNT_HEADER) (`x-ruststream-retry-count`), incremented on every copy.
The copy is at most once over the delay: a process that exits before the timer fires loses it.
A graceful shutdown waits for pending copies within the app's [`shutdown_timeout`](api:ruststream::runtime::RustStream::shutdown_timeout).
A zero delay redelivers at once, as `retry()` does; write `retry()` when no delay is meant.
A batch body sets a delay per element, and only those elements come back.
A test runs on a paused clock (`#[tokio::test(start_paused = true)]`), and [`tb.advance(delay)`](api:ruststream::testing::TestApp::advance) brings the delivery back, as the [advance](../../application/testing/driving/advance.md) page shows.
How many times a delivery may come back is the registration's declaration, [`max_attempts`](../retry-cap.md).

On the embedded runner a copy waits in a store the mount sizes with `retry_capacity::<N>()`.
When a delayed retry finds every record taken, the copy has nowhere to wait.
That is a failed publish like any other, with its own error ([`NoFreeRecordError`](api:ruststream::runtime::NoFreeRecordError)): the handler's [`.error`](./error.md) is told, and without one the failure is logged and the copy is lost.
The original is settled either way, so the handler never runs again because the store was full.
A copy that never waits (a dead letter, a zero delay) takes no record, so it is prepared in buffers of its own, which allocates with the heap; without `alloc` the buffers are fixed and nothing is allocated.
The reference is the documentation of the [embedded runner](api:ruststream::runtime::EmbeddedApp).

A copy the runtime fails to publish, for any cause, is the handler's to learn of through [`.error`](./error.md).

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="crates/ruststream/examples/retry.rs#retry_after"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="crates/ruststream/examples/manual/retry.rs#retry_after"
```

</TabItem>
</Tabs>

## Per element of a batch

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="crates/ruststream/examples/retry.rs#batch_retry_after"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="crates/ruststream/examples/manual/retry.rs#batch_retry_after"
```

</TabItem>
</Tabs>
