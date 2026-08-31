# Middleware

Middleware wraps handlers with cross-cutting logic: tracing, metrics, auth, retries. RustStream has
two middleware scopes, both built on the same `Layer` machinery, applied at different points in the
dispatch path.

## Middleware scopes

The two scopes compose: the application stack is the outer one, a router's own stack sits inside
it.

**Application scope.** Add a layer to the whole application with `RustStream::layer`, before
`with_broker`. Every handler registered after it is wrapped - both handlers registered directly on
a broker scope and handlers a router brings in via `include_router`. The order is enforced at
compile time: the first `with_broker` moves the builder to a phase where `layer` (and
`publish_layer`, and `on_startup`) no longer exist, so a layer that could not wrap the
already-registered handlers is a compile error, not a silent no-op:

=== "Macros"

    ```rust
    --8<-- "examples/middleware_app_scope.rs:app_scope"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/middleware_app_scope.rs:app_scope"
    ```

**Router scope.** Give a router its own middleware with `Router::layer`, which wraps every handler
on that router when it is mounted (see [Routing](routing.md#router-middleware)). Handlers mounted
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
[`middleware_router_scope.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/middleware_router_scope.rs);
`LogLayer` is the hand-written layer from the next section, and the built-in
`layers::TracingLayer` mounts the same way.

The first layer added is the outermost. Both stacks are static: zero runtime dispatch cost, and
the type grows as you call `layer`.

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

`Identity` is the no-op layer (the default global stack), and `Stack<Inner, Outer>` composes two.
The `ctx` here is the same per-delivery [`Context`](context.md) the handler receives, so a layer
can enrich the [headers working copy](context.md#the-headers-working-copy) before the handler
reads it.

## Per-handler middleware

Wrap a single handler with `HandlerExt::with` instead of the whole application:

<!-- inline-rust: HandlerExt::with API-shape fragment with placeholder handler and layer; the LogLayer impl it composes is compiled in middleware.rs:layer_impl, shown above -->
```rust
use ruststream::runtime::HandlerExt;

let handler = base_handler.with(LogLayer);
```

This is the right tool when only some handlers need a layer. It composes with the global stack.

## What a layer costs

Static layers are free on the hot path. Dynamic layers pay per message, so reach for them when the
chain is assembled at runtime.

## Dynamic middleware

When the chain is decided at runtime (layers toggled by config, or held behind `dyn`), opt
into the dynamic stack for exactly those handlers: `DynStack`, `DynMiddleware`, and `Next`. A
`DynMiddleware` has an around/next signature - it inspects the input and context, then either calls
`next.run(..)` to continue or short-circuits with its own result. It spells its return type out
explicitly:

```rust
use std::future::Future;
use std::pin::Pin;

use ruststream::runtime::{Context, DynMiddleware, HandlerOutcome, Next};

--8<-- "examples/middleware.rs:dyn_middleware"
```

Only the *list* is dynamic. Build it at runtime, freeze it into a `DynStack`, and the result is an
ordinary static `Layer` - compose it into the application stack with `layer`, exactly like a
hand-written one. The rest of the dispatch chain stays static; only the stack itself pays:

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

`DynStack<I>` is generic over the input it wraps. In the application stack it wraps the whole
decoding handler, so it is built over the broker's raw message type (`DynStack<MemoryMessage>`
above) and runs before decoding - a middleware generic over `I`, like `Audit`, works at either
level. To run on the decoded value instead, build a `DynStack<Order>` and apply it to the inner
typed handler with `with` (the manual registration form). Middleware in the same `DynStack` runs
in list order, outermost first. Keep the static chain as the default and reach for `DynStack` only
where runtime composition earns it.

## Publish-side middleware { #publish-side-middleware }

The middleware above runs on the consume path (incoming messages). The publish path has its own
pipeline; see [Publishing and replies](publishing.md#the-publish-pipeline).

## Built-in layers

- `layers::TracingLayer` emits a tracing event per message (DEBUG on arrival, INFO on ack, WARN on
  nack). To render those events on the console, enable the `logging` feature; see
  [Logging](logging.md).
- The `metrics` feature ships a layer that records Prometheus counters and a duration histogram; see
  [Metrics](metrics.md).
