---
sidebar_position: 1
title: Layer
description: "A layer wraps handlers in code of your own: every handler of the app, every handler of a router, or one registration."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Layer

A layer wraps handlers in code of your own: every handler of the app, every handler of a router, or one registration.

A [`Layer<H>`](api:ruststream::runtime::Layer) turns a handler `H` into a wrapper of its own type, and that type implements [`Handler`](api:ruststream::runtime::Handler).
The wrapper gets the input and the same `&mut Context` the handler gets, and calls the handler it wraps.
It runs code before and after the call, and may return an outcome without making it.
Three scopes take a layer.
[`RustStream::layer`](api:ruststream::runtime::RustStream::layer), before `with_broker`, wraps every handler the app mounts, router handlers included.
[`Router::layer`](api:ruststream::runtime::Router::layer), before the router's first `include`, wraps every handler of that [router](../router.md).
`.layer(..)` right after an `include` on a router wraps that registration alone, outside its decode step.
The app and a router hide their handlers' concrete types, so a layer for those scopes also implements [`BlanketLayer`](api:ruststream::runtime::BlanketLayer); a registration keeps its type, and `Layer<H>` is enough there.
The app's layers wrap outside a router's.
In the app and in a router, the first layer added runs outermost; on a registration, the last one added does.
The chain is one type, so a layer adds nothing to the hot path beyond its own code.
App and router layers wrap single-message handlers; [batch handlers](../batches.md) are not wrapped.
A layer has no macro form: the same impl serves both forms, and only the mounting differs.
A chain chosen when the app starts goes into a [`DynStack`](./dyn-stack.md).

## Example

The layer drops a delivery that names no tenant before the handler sees it.

{/* single-form: a Layer impl has no macro form; the same code serves the macro and the manual service */}
```rust reference="examples/layer.rs#layer"
```

Mounted on the app, it wraps every handler.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/layer.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/layer.rs#app"
```

</TabItem>
</Tabs>

## A router and a registration

On a fresh router the layer wraps every handler of the router.
After an `include` it wraps that registration alone.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/layer.rs#scopes"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/layer.rs#scopes"
```

</TabItem>
</Tabs>
