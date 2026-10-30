---
sidebar_position: 8
title: Testing an embedded app
description: "TestApp runs a device service's EmbeddedApp on the host: it records what went through each Out slot, runs a test's hooks inside the test's own future and starts the app with a state the test builds."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Testing an embedded app

On the `embassy` runtime, `TestApp` runs a device service's `EmbeddedApp` on the host, with every broker connected in process and embassy-time's mock clock in the test's hands.
The test hands the app function its brokers, as the device's task does, and drives the app with the same calls a tokio test makes: [`broker`](./driving/broker.md), [`settle`](./driving/settle.md), [`advance`](./driving/advance.md) and `shutdown`.
The app runs inside the test's own future, so a test is a plain `#[test]` that blocks on one future, and one `TestApp` runs at a time in a process.

## Out slot records

`tb.out::<M>()` opens the record of the `Out` slot marked `M`: every message a handler sent through it, with its destination and headers.
It reads the same way as on tokio, as the [`out`](./records/out.md) page describes.
A slot publish is also recorded on its destination, so [`published::<T>(name)`](./records/published.md) on the broker it went to sees it.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="crates/ruststream-testing/tests/embedded.rs#out"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="crates/ruststream-testing/tests/embedded_manual.rs#out"
```

</TabItem>
</Tabs>

## Hooks

[`on_call`](./hooks/on-call.md) and [`on_publish`](./hooks/on-publish.md) take the same async closures as on tokio.
A hook runs inside the test's own future: the dispatch that recorded the event waits for it, and the step that drives the app polls it beside the app.
A hook runs on the test's own thread, and a hook that waits on the clock finishes once the test advances it.
A panic inside a hook, a failed assertion included, fails the publish, `settle`, `advance` or `shutdown` that ran it.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="crates/ruststream-testing/tests/embedded.rs#on-publish"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="crates/ruststream-testing/tests/embedded_manual.rs#on-publish"
```

</TabItem>
</Tabs>

## A state the test builds

`TestApp::with_state(app, build)` starts the app with the state `build` returns in place of the one the app was built with: a value read from the board's flash, a driver of its hardware.
The app function names its state in its type, `EmbeddedApp<impl Mounts<State = Limits>>`, so the test can build one.
The harness connects every broker first, then calls `build` with the brokers, so `brokers.broker::<B>()` returns the connected form of the app's broker of type `B`, as on [tokio](./starting-and-stopping/with-state.md).
Everything else of startup runs as in `TestApp::start`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="crates/ruststream-testing/tests/embedded.rs#with-state"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="crates/ruststream-testing/tests/embedded_manual.rs#with-state"
```

</TabItem>
</Tabs>
