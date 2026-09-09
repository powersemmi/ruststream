# Middleware

Middleware wraps handlers with cross-cutting logic: tracing, metrics, auth, retries. RustStream has
two middleware scopes. Both are built on the same `Layer` machinery and apply at different points in
the dispatch path.

## Middleware scopes

The two scopes compose: the application stack is the outer one, a router's own stack sits inside
it.

**Application scope.** `RustStream::layer` adds a layer to the whole application, before
`with_broker`. Every handler registered after it is wrapped: both handlers registered directly on a
broker scope and handlers from a router mounted with `include_router`. The order is enforced at
compile time: the first `with_broker` moves the builder to a phase without `layer`, `publish_layer`
and `on_startup`. A layer that could not wrap the already-registered handlers is a compile error,
not a silent no-op:

=== "Macros"

    ```rust
    --8<-- "examples/middleware_app_scope.rs:app_scope"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/middleware_app_scope.rs:app_scope"
    ```

**Router scope.** `Router::layer` gives a router its own middleware: it wraps every handler on that
router when the router is mounted (see [Routing](routing.md#router-middleware)). Handlers mounted
directly on the broker scope stay outside it:

=== "Macros"

    ```rust
    --8<-- "examples/middleware_router_scope.rs:router_scope"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/middleware_router_scope.rs:router_scope"
    ```

The two programs are
[`middleware_app_scope.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/middleware_app_scope.rs)
and
[`middleware_router_scope.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/middleware_router_scope.rs).
`LogLayer` is the hand-written layer from the next section. The built-in `layers::TracingLayer`
mounts the same way.

The first layer added is the outermost. Both stacks are static: zero runtime dispatch cost, and the
stack's type grows as you call `layer`.

!!! note "Reaching router handlers requires a `BlanketLayer`"
    A layer that wraps router handlers (the app stack at `include_router`, or `Router::layer`)
    must implement `BlanketLayer` - one generic method that wraps any handler. The bundled layers
    implement it; for a custom layer it is a few lines next to its `Layer` impl (see `LogLayer` in
    the examples above).

## Writing a layer

A layer transforms one handler into another. Implement `Layer<H>`:

```rust
use ruststream::runtime::{Context, Handler, HandlerOutcome, Layer};

--8<-- "examples/middleware.rs:layer_impl"
```

`Identity` is the no-op layer. It is the default global stack. `Stack<Inner, Outer>` composes two
layers. The `ctx` here is the same per-delivery [`Context`](context.md) the handler receives, so a
layer can add values to the [headers working copy](context.md#the-headers-working-copy) before the
handler reads it.

## Per-registration middleware

You can put a layer on a single registration instead of the whole application. `.layer(..)` after an
`include` on a router applies to that registration, exactly as the other steps of the chain apply to
the position named before them.

<!-- inline-rust: the call shape; the LogLayer impl it composes is compiled in middleware.rs:layer_impl, shown above -->
```rust
let router = Router::<MemoryBroker>::new().include(handle).layer(LogLayer);
```

A single registration is the right tool when only some handlers need a layer. It is the only place
a layer that is not a `BlanketLayer` can go: the registration's handler type is still concrete
here, so an ordinary `Layer<H>` is enough. The layer sits outside the decode step, so it sees the
broker's raw message. It composes with the app-wide and router-wide stacks.

## What a layer costs

Static layers are free on the hot path. Dynamic layers pay per message: keep static layers by
default, and reach for dynamic ones when the chain is assembled at runtime.

## Dynamic middleware

Sometimes the chain is known only at runtime: config toggles the layers, or they are held behind
`dyn`. For those handlers, take the dynamic stack: `DynStack`, `DynMiddleware`, and `Next`. A
`DynMiddleware` receives the input and the context, then either calls `next.run(..)` to continue the
chain or ends it with its own result. You write out its return type explicitly:

```rust
use std::future::Future;
use std::pin::Pin;

use ruststream::runtime::{Context, DynMiddleware, HandlerOutcome, Next};

--8<-- "examples/middleware.rs:dyn_middleware"
```

Only the *list* is dynamic. Build it at runtime and pass it to a `DynStack`: the result is an
ordinary static `Layer`, bound to a single input type. So it goes on one registration with
`.layer(..)`, not into the application stack, which accepts only layers that implement
`BlanketLayer`. The rest of the dispatch chain stays static; only the stack itself pays:

=== "Macros"

    ```rust
    use std::sync::Arc;

    use ruststream::memory::MemoryMessage;
    use ruststream::runtime::DynStack;

    --8<-- "examples/middleware.rs:dyn_stack"
    ```

=== "Manual"

    ```rust
    use std::sync::Arc;

    use ruststream::memory::{MemoryBroker, MemoryMessage};
    use ruststream::prelude::*;
    use ruststream::runtime::{DynMiddleware, DynStack};

    --8<-- "examples/manual/middleware.rs:dyn_stack"
    ```

The full program, with the chain toggled by an environment variable, is
[`examples/middleware.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/middleware.rs).

`DynStack<I>` is generic over the input it wraps. On a registration it wraps the whole decoding
handler, so it is built over the broker's raw message type (`DynStack<MemoryMessage>` above) and
runs before decoding. Middleware generic over `I`, like `Audit`, works with any input type.
Middleware in the same `DynStack` runs in list order, outermost first.

## Publish-side middleware { #publish-side-middleware }

The middleware above runs on the consume path (incoming messages). The publish path has its own
pipeline; see [Publishing and replies](publishing.md#the-publish-pipeline).

## Built-in layers

- `layers::TracingLayer` emits a tracing event per message (DEBUG on arrival, INFO on ack, WARN on
  nack). To render those events on the console, enable the `logging` feature; see
  [Logging](logging.md).
- The `metrics` feature ships a layer that records Prometheus counters and a duration histogram; see
  [Metrics](metrics.md).
