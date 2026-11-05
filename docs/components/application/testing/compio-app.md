---
sidebar_position: 9
title: Testing a compio app
description: "TestApp runs a compio service on the test's own compio runtime, with the same broker, settle, advance and Out slot assertions as on tokio."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Testing a compio app

`testing::compio::TestApp` runs a compio service's app on the test's own compio runtime, with every broker connected in process.
The test hands it the production app, unchanged, and drives it with the calls a tokio test makes: [`broker`](./driving/broker.md), [`settle`](./driving/settle.md), [`advance`](./driving/advance.md) and `shutdown`.
The logger the app selects with `.logger(..)` is applied at `start`, so a test reads the diagnostics the production app emits.
The test is a plain `#[test]` that blocks on one future of a `compio::runtime::Runtime`.

The harness runs the app as the one core of the test's runtime: it drives every subscription and worker there, a `threads(n)` mount as `n` workers of it, and does not reproduce the cores, the pinning or the hand-off between cores.
`Core::current()` is core 0 of 1 in a test.

One `TestApp` runs at a time on a thread, because the clock it holds is one per thread.

## Time

The harness holds the runner's virtual clock from `start` until it is dropped.
Retries, batch deadlines and [`ruststream::runtime::compio::time::sleep`](api:ruststream::runtime::compio::time::sleep) wait until `advance` moves the clock, and the app then handles what fell due.
A handler that calls `compio::time::sleep` directly waits on compio's own timers, and `advance` does not reach it.

`settle` returns once everything delivered is handled, up to what waits on the clock.
A publish through a broker handle settles on its own.

## Example

A service with one handler, and the test that publishes to it and reads what it received:

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="crates/testing/tests/compio_handler.rs#service"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="crates/testing/tests/compio_manual.rs#handler-service"
```

</TabItem>
</Tabs>

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="crates/testing/tests/compio_handler.rs#test"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="crates/testing/tests/compio_manual.rs#handler-test"
```

</TabItem>
</Tabs>

### Replies

A reply is recorded on its destination, and `published::<M>(name)` reads it:

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="crates/testing/tests/compio_reply.rs#service"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="crates/testing/tests/compio_manual.rs#reply-service"
```

</TabItem>
</Tabs>

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="crates/testing/tests/compio_reply.rs#published"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="crates/testing/tests/compio_manual.rs#published"
```

</TabItem>
</Tabs>

### Out slots

`tb.out::<M>()` opens the record of the `Out` slot marked `M`, as the [`out`](./records/out.md) page describes:

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="crates/testing/tests/compio_reply.rs#out"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="crates/testing/tests/compio_manual.rs#out"
```

</TabItem>
</Tabs>

### A retry on the virtual clock

A delayed retry is redelivered once `advance` has moved the clock past its delay:

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="crates/testing/tests/compio_retry.rs#service"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="crates/testing/tests/compio_manual.rs#retry-service"
```

</TabItem>
</Tabs>

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="crates/testing/tests/compio_retry.rs#advance"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="crates/testing/tests/compio_manual.rs#advance"
```

</TabItem>
</Tabs>
