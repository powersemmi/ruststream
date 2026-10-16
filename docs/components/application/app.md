---
sidebar_position: 1
title: Application
description: "A RustStream value names the service, holds its brokers and mounts each handler on the broker it subscribes to."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Application

A `RustStream` value names the service, holds its brokers and mounts each handler on the broker it subscribes to.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/quickstart.rs#handler"
```

```rust reference="examples/quickstart.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/quickstart.rs#handler"
```

```rust reference="examples/manual/quickstart.rs#app"
```

</TabItem>
</Tabs>

## How it works

`AppInfo::new` takes the service's title and version.
They name the service in its logs and in its AsyncAPI document.
`with_broker` registers one broker and mounts handlers on it inside the closure.
A service with several brokers calls it once per broker.
A broker constructor does no I/O: the application connects every broker when it starts.
Handlers mounted with `b.include(..)` decode with the default codec; `with_broker_codec` names another one for the whole scope.
`register_broker` adds a broker that only publishes and has no handlers.
A builder function returns `impl App`, so its signature stays the same as layers and state are added.

## See also

- [`#[ruststream::app]`](./app-macro.md): the generated `main`.
- [Lifecycle hooks](./lifecycle.md): code that runs around the brokers' connection.
- [Running the service](./running.md): `run`, `run_until` and `start`.
- [Any struct as state](./state/any-struct.md): one value every handler reads.
- [`RustStream`](api:ruststream::runtime::RustStream), [`AppInfo`](api:ruststream::runtime::AppInfo), [`App`](api:ruststream::runtime::App), [`RustStream::with_broker`](api:ruststream::runtime::RustStream::with_broker), [`BrokerScope`](api:ruststream::runtime::BrokerScope)
