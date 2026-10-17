---
sidebar_position: 3
title: with_state
description: "TestApp::with_state starts the app with a state the test builds in place of on_startup, wired to the brokers of the app."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# with_state

[`TestApp::with_state`](api:ruststream::testing::TestApp::with_state) starts the app with a state the test builds in place of `on_startup`, wired to the brokers of the app.

It is for a state that reaches outside the service: a catalog, a database, a mail client the test cannot or should not call.
The harness connects each broker in process first, then calls the builder, so the state's publishers pair against connected brokers.
[`TestBrokers::broker::<B>()`](api:ruststream::testing::TestBrokers::broker) returns the connected form of the app's broker of type `B`.
A publisher built from it publishes onto the bus the assertions read, such as [`published::<T>(name)`](../records/published.md) and its [`with`](../published-assertions/with.md).
The app's `on_startup` does not run; `after_startup` and everything else of startup do, as in [`TestApp::start`](./start.md).
`TestBrokers::broker` panics when the app has no broker of type `B` or more than one.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/testing_starting.rs#with-state"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/testing_starting.rs#with-state"
```

</TabItem>
</Tabs>
