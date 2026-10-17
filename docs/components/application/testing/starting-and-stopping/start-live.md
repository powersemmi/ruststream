---
sidebar_position: 2
title: start_live
description: "start_live connects the brokers of the same app to a running stand, so one test body checks the service against the real broker."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# start_live

[`start_live`](api:ruststream::testing::TestApp::start_live) connects the brokers of the same app to a running stand, so one test body checks the service against the real broker.

It is for a stand of a networked broker: a Kafka or NATS server the test suite starts beside the tests.
Each broker connects through its ordinary `connect`, and the rest of startup is the production one.
The test body is the one an in-process test runs; only the start call differs from [`TestApp::start`](./start.md).
`MemoryBroker` connects without a server, so the example runs in process in every mode.
A publish, [`settle`](../driving/settle.md) and [`advance`](../driving/advance.md) wait on the real clock until every subscription has handled what was published to it.
The wait lasts ten seconds at most; [`start_live_within`](api:ruststream::testing::TestApp::start_live_within) sets another bound, and past it the call fails with `TestError::NotSettled` naming the subscription.
A live test runs on the real clock: `start_live` on a paused clock fails with `TestError::PausedClock`.
The test's input reaches the broker through its connected form's default publish policy.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/testing_starting.rs#start-live"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/testing_starting.rs#start-live"
```

</TabItem>
</Tabs>
