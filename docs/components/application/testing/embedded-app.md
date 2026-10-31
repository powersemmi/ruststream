---
sidebar_position: 8
title: Testing an embedded app
description: "TestApp runs a device service's EmbeddedApp on the host: Out slot records, hooks in the test's own future, and a state the test builds."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Testing an embedded app

On the `embassy` runtime, `TestApp` runs a device service's `EmbeddedApp` on the host, with every broker connected in process and the clock the app reads in the test's hands.
The test hands the app function its brokers, as the device's task does, and drives the app with the same calls a tokio test makes: [`broker`](./driving/broker.md), [`settle`](./driving/settle.md), [`advance`](./driving/advance.md) and `shutdown`.
The app runs inside the test's own future, so a test is a plain `#[test]` that blocks on one future, and one `TestApp` runs at a time in a process.

The harness is the test build's embassy-time driver: the clock starts at zero and moves only when the test moves it.
embassy-time's own `mock-driver` is a second driver of the same clock, and a build links only one of the two.

`TestApp::start` returns once the app is running.
A broker that waits on the clock to connect, or a subscription that waits to open, is waited for: the clock moves from one armed deadline to the next, and a startup nothing wakes returns `TestError::Stalled`.

`shutdown` lets the work the app holds run to its end: the clock moves from one armed deadline to the next until the app has stopped.
A handler or a hook that waits on a timer finishes, the shutdown hooks run and every broker shuts down.
Work that waits on something nothing wakes makes `shutdown` return `TestError::Stalled`, and an app that stopped on its own error returns it as `TestError::Failed`.

## What the harness adds

### Out slot records

`tb.out::<M>()` opens the record of the `Out` slot marked `M`: every message a handler sent through it, with its destination and headers.
It reads the same way as on tokio, as the [`out`](./records/out.md) page describes.
A slot publish is also recorded on its destination, so [`published::<T>(name)`](./records/published.md) on the broker it went to sees it.

### Hooks

[`on_call`](./hooks/on-call.md) and [`on_publish`](./hooks/on-publish.md) take the same async closures as on tokio.
A hook runs inside the test's own future: the dispatch that recorded the event waits for it, and the step that drives the app polls it beside the app.
A hook runs on the test's own thread, and a hook that waits on the clock finishes once the test advances it or shuts the app down.
A hook on a destination the test itself publishes to runs in that publish, up to where it waits on the clock.
A panic inside a hook, a failed assertion included, fails the publish, `settle`, `advance` or `shutdown` that ran it.

### A state the test builds

`TestApp::with_state(app, build)` starts the app with the state `build` returns in place of the one the app was built with: a value read from the board's flash, a driver of its hardware.
The app function names its state in its type, `EmbeddedApp<impl Mounts<State = Limits>>`, so the test can build one.
The harness connects every broker first, then calls `build` with the brokers, so `brokers.broker::<B>()` returns the connected form of the app's broker of type `B`, as on [tokio](./starting-and-stopping/with-state.md).
Everything else of startup runs as in `TestApp::start`.

## Example

### Out slot records

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

### Hooks

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

### A state the test builds

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
