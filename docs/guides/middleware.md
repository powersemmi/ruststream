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

```rust
--8<-- "examples/middleware_app_scope.rs:app_scope"
```

**Router scope.** Give a router its own middleware with `Router::layer`, which wraps every handler
on that router when it is mounted (see [Routing](routing.md#router-middleware)). Handlers mounted
directly on the broker scope stay outside it:

```rust
--8<-- "examples/middleware_router_scope.rs:router_scope"
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
    A router hides its handlers' concrete types, so a layer that wraps them (the app stack at
    `include_router`, or `Router::layer`) must implement `BlanketLayer` - one generic method that
    wraps any handler. The bundled layers implement it; for a custom layer it is a few lines next
    to its `Layer` impl (see `LogLayer` in the examples above).

## Writing a layer

A layer transforms one handler into another. Implement `Layer<H>`:

```rust
use ruststream::runtime::{Context, Handler, HandlerResult, Layer};

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

## Why middleware is static by default

The layers above are resolved at compile time: `with`/`layer` build a concrete, nested handler type
(`Logged<Typed<..>>`), and `Handler::handle` returns an `impl Future` whose type is known. The
compiler monomorphizes the whole chain into one state machine and inlines across the layer
boundaries, so a static layer adds no dispatch cost and no allocation - it is a zero-cost
abstraction.

Making every middleware dynamic (`dyn`) would throw that away. `Handler::handle` is an `async fn in
trait`, so its future is an anonymous `impl Future` - and a trait with an `impl Trait` return is not
object-safe. To store middleware behind `dyn`, the future has to be boxed (`Pin<Box<dyn Future>>`):
one heap allocation per layer per message, and the call can no longer be inlined or specialized
across the `dyn` boundary. `dyn` + `async` does not optimize, so paying that cost on every handler -
when the chain is almost always known at compile time - would be the wrong default.

## Dynamic middleware

When the chain genuinely is decided at runtime (layers toggled by config, or held behind `dyn`), opt
into the dynamic stack for exactly those handlers: `DynStack`, `DynMiddleware`, and `Next`. A
`DynMiddleware` has an around/next signature - it inspects the input and context, then either calls
`next.run(..)` to continue or short-circuits with its own result. Because it is object-safe, it
returns a boxed future explicitly:

```rust
use std::future::Future;
use std::pin::Pin;

use ruststream::runtime::{Context, DynMiddleware, HandlerResult, Next};

--8<-- "examples/middleware.rs:dyn_middleware"
```

Only the *list* is dynamic. Build it at runtime, freeze it into a `DynStack`, and the result is an
ordinary static `Layer` - compose it into the application stack with `layer`, exactly like a
hand-written one. The rest of the dispatch chain stays static; the boxing cost is paid only inside
the stack:

```rust
use std::sync::Arc;

use ruststream::memory::MemoryMessage;
use ruststream::runtime::DynStack;

--8<-- "examples/middleware.rs:dyn_stack"
```

The full program, with the chain toggled by an environment variable, is
[`examples/middleware.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/middleware.rs).

`DynStack<I>` is generic over the input it wraps. In the application stack it wraps the whole
decoding handler, so it is built over the broker's raw message type (`DynStack<MemoryMessage>`
above) and runs before decoding - a middleware generic over `I`, like `Audit`, works at either
level. To run on the decoded value instead, build a `DynStack<Order>` and apply it to the inner
typed handler with `with` (the manual registration form). Middleware in the same `DynStack` runs
in list order, outermost first. Each dynamic layer costs one boxed future per call, against zero
for the static layers, so keep the static chain as the default and reach for `DynStack` only where
runtime composition earns it.

## Publish-side middleware

The middleware above runs on the consume path (incoming messages). The publish path has its own
pipeline; see [Publishing and replies](publishing.md#the-publish-pipeline).

## Built-in layers

- `layers::TracingLayer` emits a tracing event per message (DEBUG on arrival, INFO on ack, WARN on
  nack). To render those events on the console, enable the `logging` feature; see
  [Logging](logging.md).
- The `metrics` feature ships a layer that records Prometheus counters and a duration histogram; see
  [Metrics](metrics.md).
