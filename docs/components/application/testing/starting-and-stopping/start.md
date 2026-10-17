---
sidebar_position: 1
title: start
description: "TestApp::start runs the app main builds, unchanged, with every broker connected in process and no server."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# start

[`TestApp::start`](api:ruststream::testing::TestApp::start) runs the app `main` builds, unchanged, with every broker connected in process and no server.

The harness starts the app in the service's own order: `on_startup`, then each broker's connect, then the subscriptions, then `after_startup`.
Each broker connects through its in-process transition, which performs no I/O and yields the broker's production connected form.
`MemoryBroker` has no server, so its in-process mode is its ordinary `connect`.
A networked broker provides the in-process mode under its crate's `testing` feature, which a service enables in `[dev-dependencies]`.
A broker without it makes `start` fail with [`TestError::NoTransport`](api:ruststream::testing::TestError) naming the broker.
A failing `on_startup` fails `start` with `TestError::Startup` before any broker connects.
[`start_live`](./start-live.md) runs the same app against a running stand, and [`with_state`](./with-state.md) starts it with a state the test builds.
A test ends with [`shutdown`](./shutdown.md).

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
