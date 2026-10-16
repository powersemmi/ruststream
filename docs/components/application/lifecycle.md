---
sidebar_position: 3
title: Lifecycle hooks
description: "Four hooks run around the brokers' connection: on_startup, after_startup, on_shutdown and after_shutdown."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Lifecycle hooks

Four hooks run around the brokers' connection: `on_startup`, `after_startup`, `on_shutdown` and `after_shutdown`.

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

## How it works

`on_startup` runs before any broker connects.
The value it returns becomes the application state, and its error aborts startup with no broker dialled.
`on_startup` comes first in the builder: a hook registered before it makes the builder panic.
`after_startup` runs once every subscription is open; its error aborts startup and shuts down the brokers that connected.
`on_shutdown` runs when shutdown begins, while the brokers are still connected.
`after_shutdown` runs after the brokers have shut down.
The shutdown hooks' errors are logged and do not stop the shutdown.
Every hook after `on_startup` receives the state as `Arc<State>`.
`shutdown_timeout` bounds how long shutdown waits for handlers still running; past it, they are aborted.
Without it, shutdown waits for them indefinitely.
A broker scope's `after_startup` receives a live publisher of that broker, paired from the policy it names.
`TestApp` runs `on_startup` and `after_startup`; the shutdown hooks run on `RunningApp::shutdown`, so the first example drives `start` and `shutdown`.

## See also

- [Running the service](./running.md): what starts and stops the service.
- [Any struct as state](./state/any-struct.md): the value `on_startup` produces.
- [`RustStream::on_startup`](api:ruststream::runtime::RustStream::on_startup), [`RustStream::after_startup`](api:ruststream::runtime::RustStream::after_startup), [`RustStream::on_shutdown`](api:ruststream::runtime::RustStream::on_shutdown), [`RustStream::after_shutdown`](api:ruststream::runtime::RustStream::after_shutdown), [`RustStream::shutdown_timeout`](api:ruststream::runtime::RustStream::shutdown_timeout), [`BrokerScope::after_startup`](api:ruststream::runtime::BrokerScope::after_startup)
