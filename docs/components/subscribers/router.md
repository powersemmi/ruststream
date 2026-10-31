---
sidebar_position: 6
title: Routers
description: "A Router groups one module's handlers with their own middleware and codec, and include_router mounts the group on a broker."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Routers

A [`Router`](api:ruststream::runtime::Router) groups one module's handlers with their own middleware and codec, and [`include_router`](api:ruststream::runtime::BrokerScope::include_router) mounts the group on a broker.

A router is a group of registrations bound to a broker type, not to a broker value, so a module builds it without a connection.
`include` adds a [`#[subscriber]`](./index.md) handler to the router the same way it adds one to a broker scope, settings included.
A router's full type names every registration in a [`RouterDef`](api:ruststream::runtime::RouterDef), so a builder function returns `impl RouterDef<B>`, or `Router<B, impl RouterDef<B>>` when the caller still merges it.
[`merge`](api:ruststream::runtime::Router::merge) appends another router's registrations, and each router keeps its own codec and layers.
`include_router` on a broker scope of the [application](../application/index.md) mounts the group, as a step of the scope's chain before its `include`s.
[`layer`](api:ruststream::runtime::Router::layer) adds [middleware](./middleware/index.md) and is called before the router's first `include`; after one, `.layer(..)` applies to that registration alone.
A router's layer must be a [`BlanketLayer`](api:ruststream::runtime::BlanketLayer), because the router hides its handlers' types.
The app's own layers wrap outside the router's layers.
Router layers wrap single-message handlers; batch handlers are not wrapped.
[`with_codec`](api:ruststream::runtime::Router::with_codec) switches the [codec](./codecs/index.md) mid-chain: registrations already added keep the codec they were added with.
Without it, a router decodes with the default codec, JSON when the `json` feature is on.

## Example

Each module returns its router, and the service merges them and mounts the result.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/routing.rs#router"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/routing.rs#router"
```

</TabItem>
</Tabs>

## Middleware of one router

`Router::layer` wraps every handler of the router, and nothing mounted outside it.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/routing.rs#layer"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/routing.rs#layer"
```

</TabItem>
</Tabs>

## A codec of one router

`Router::with_codec` sets the codec the registrations after it decode with.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/routing.rs#codec"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/routing.rs#codec"
```

</TabItem>
</Tabs>
