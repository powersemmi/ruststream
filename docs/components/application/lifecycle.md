---
title: Lifecycle hooks
sidebar_label: Lifecycle
sidebar_position: 3
description: "Four hooks run around the brokers' connection: on_startup, after_startup, on_shutdown and after_shutdown."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Lifecycle

Four hooks run around the brokers' connection: [`on_startup`](api:ruststream::runtime::RustStream::on_startup), [`after_startup`](api:ruststream::runtime::RustStream::after_startup), [`on_shutdown`](api:ruststream::runtime::RustStream::on_shutdown) and [`after_shutdown`](api:ruststream::runtime::RustStream::after_shutdown).

`on_startup` runs before any broker connects.
The value it returns becomes the application [state](./state/any-struct.md), and its error aborts startup with no broker dialled.
`on_startup` comes first in the builder: a hook registered before it makes the builder panic.
`after_startup` runs once every subscription is open; its error aborts startup and shuts down the brokers that connected.
`on_shutdown` runs when [shutdown](./running/shutdown.md) begins, while the brokers are still connected.
`after_shutdown` runs after the brokers have shut down.
The shutdown hooks' errors are logged and do not stop the shutdown.
Every hook after `on_startup` receives the state as `Arc<State>`.
[`shutdown_timeout`](api:ruststream::runtime::RustStream::shutdown_timeout) bounds how long shutdown waits for handlers still running; past it, they are aborted.
Without it, shutdown waits for them indefinitely.
A broker scope's [`after_startup`](api:ruststream::runtime::BrokerScope::after_startup) is a step of its mount chain, written before the `include`s, and receives a live publisher of that broker, paired from the policy it names.
[`TestApp`](./testing/starting-and-stopping/start.md) runs every hook in the service's order: the startup hooks on `start`, the shutdown hooks on [`shutdown`](./testing/starting-and-stopping/shutdown.md).

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/app_lifecycle.rs#hooks"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/app_lifecycle.rs#hooks"
```

</TabItem>
</Tabs>

## A hook of one broker

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/app_lifecycle.rs#announce"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/app_lifecycle.rs#announce"
```

</TabItem>
</Tabs>
