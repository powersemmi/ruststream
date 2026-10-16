---
sidebar_position: 1
title: TestApp::start
description: "TestApp::start runs the app main builds, unchanged, with every broker connected in process and no server."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# `TestApp::start`

`TestApp::start` runs the app `main` builds, unchanged, with every broker connected in process and no server.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/testing_starting.rs#start"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/testing_starting.rs#start"
```

</TabItem>
</Tabs>

## How it works

The harness starts the app in the service's own order: `on_startup`, then each broker's connect, then the subscriptions, then `after_startup`.
Each broker connects through its in-process transition, which performs no I/O and yields the broker's production connected form.
`MemoryBroker` has no server, so its in-process mode is its ordinary `connect`.
A networked broker provides the in-process mode under its crate's `testing` feature, which a service enables in `[dev-dependencies]`.
A broker without it makes `start` fail with `TestError::NoTransport` naming the broker.
A failing `on_startup` fails `start` with `TestError::Startup` before any broker connects.

## See also

- [`start_live`](./start-live.md): the same app against a running stand.
- [`with_state`](./with-state.md): start with a state the test builds instead of `on_startup`.
- [`shutdown`](./shutdown.md): the teardown at the end of a test.
- [`TestApp::start`](api:ruststream::testing::TestApp::start), [`TestError`](api:ruststream::testing::TestError)
