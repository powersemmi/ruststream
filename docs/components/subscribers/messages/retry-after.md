---
sidebar_position: 2
title: retry_after
description: "retry_after(delay) asks for the delivery back no sooner than the delay, for a condition that will pass."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# `retry_after`

`retry_after(delay)` asks for the delivery back no sooner than the delay, for a condition that will pass.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/retry.rs#retry_after"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/retry.rs#retry_after"
```

</TabItem>
</Tabs>

## Per element of a batch

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/retry.rs#batch_retry_after"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/retry.rs#batch_retry_after"
```

</TabItem>
</Tabs>

## How it works

A broker with native delayed redelivery holds the delivery and returns it after the delay.
The in-memory broker is one: the delivery comes back to the same subscriber, and nothing is published.
On a broker without it, the runtime drops the delivery and publishes a copy back to the subscription after the delay.
That copy carries the `RETRY_COUNT_HEADER` (`x-ruststream-retry-count`), incremented on every copy.
The copy is at most once over the delay: a process that exits before the timer fires loses it.
A graceful shutdown waits for pending copies within the app's `shutdown_timeout`.
A zero delay redelivers at once, as `retry()` does; write `retry()` when no delay is meant.
A batch body sets a delay per element, and only those elements come back.
A test runs on a paused clock (`#[tokio::test(start_paused = true)]`), and `tb.advance(delay)` brings the delivery back.
How many times a delivery may come back is the registration's declaration, `max_attempts`.

## See also

- [Ack, retry, drop](./handler-outcome.md): the other settlements.
- [Attempt cap and dead letter](../retry-cap.md): end a message that keeps coming back.
- [Where a retry copy goes](../retry-destination.md): the publisher and the destination of the copy.
- [`advance`](../../application/testing/driving/advance.md): let the delay pass in a test.
- [`HandlerOutcome::retry_after`](api:ruststream::runtime::HandlerOutcome::retry_after), [`RETRY_COUNT_HEADER`](api:ruststream::runtime::RETRY_COUNT_HEADER), [`RustStream::shutdown_timeout`](api:ruststream::runtime::RustStream::shutdown_timeout), [`TestApp::advance`](api:ruststream::testing::TestApp::advance)
