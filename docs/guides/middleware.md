# Middleware

Middleware wraps handlers with cross-cutting logic: tracing, metrics, auth, retries. RustStream has
two middleware levels, both built on the same `Layer` machinery, applied at different points in the
dispatch path.

## Global middleware

Add a layer to the whole application with `layer`, before `with_broker`. Every handler registered on
a broker scope is wrapped with it.

```rust
use ruststream::runtime::layers::TracingLayer;

let app = RustStream::new(info)
    .layer(TracingLayer::default())
    .with_broker(broker, |b| b.include(handle));
```

The first layer added is the outermost. The global stack is static: it has zero runtime dispatch
cost, and its type grows as you call `layer`.

!!! note "Routers carry their own stack"
    The global stack does not wrap handlers brought in via `include_router`, because a router is
    finalized independently. Give the router its own middleware with `Router::layer`, which wraps
    every handler registered after it:

    ```rust
    let mut router = Router::new().layer(TracingLayer::default());
    router.include(handle);
    b.include_router(router);
    ```

## Writing a layer

A layer transforms one handler into another. Implement `Layer<H>`:

```rust
use ruststream::runtime::{Context, Handler, HandlerResult, Layer};

struct LogLayer;

struct Logged<H>(H);

impl<H> Layer<H> for LogLayer {
    type Handler = Logged<H>;
    fn layer(&self, inner: H) -> Logged<H> {
        Logged(inner)
    }
}

impl<M, H: Handler<M>> Handler<M> for Logged<H> {
    async fn handle(&self, msg: &M, ctx: &mut Context<'_>) -> HandlerResult {
        // pre
        let result = self.0.handle(msg, ctx).await;
        // post
        result
    }
}
```

`Identity` is the no-op layer (the default global stack), and `Stack<Inner, Outer>` composes two.

## Per-handler middleware

Wrap a single handler with `HandlerExt::with` instead of the whole application:

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

struct Audit {
    service: String,
}

impl<I: Send + Sync> DynMiddleware<I> for Audit {
    fn handle<'a>(
        &'a self,
        input: &'a I,
        ctx: &'a mut Context<'_>,
        next: Next<'a, I>,
    ) -> Pin<Box<dyn Future<Output = HandlerResult> + Send + 'a>> {
        Box::pin(async move {
            tracing::info!(service = %self.service, channel = ctx.name(), "handling");
            next.run(input, ctx).await
        })
    }
}
```

Build the chain at runtime and fold it into a single `DynStack`, which is itself an ordinary
`Layer` - apply it with `with`, `Router::layer`, or the global `layer`. Here it wraps one handler,
running on the decoded `Order`:

```rust
use std::sync::Arc;

use ruststream::codec::JsonCodec;
use ruststream::runtime::{DynStack, HandlerExt, HandlerMetadata, typed};

// inside with_broker(...):
let subscriber = b.broker().subscribe("orders");

let mut middleware: Vec<Arc<dyn DynMiddleware<Order>>> = Vec::new();
if config.audit {
    middleware.push(Arc::new(Audit { service: "orders".to_owned() }));
}
let stack = DynStack::new(middleware); // empty list -> a no-op layer

let handler = (|order: &Order, _ctx: &mut Context| async { HandlerResult::Ack }).with(stack);
b.handle(subscriber, typed(JsonCodec, handler), HandlerMetadata::typed::<Order>("orders"));
```

`DynStack<I>` is generic over the input it wraps: build it over the decoded type (`DynStack<Order>`)
to run after decoding, or over the broker's raw message type to run before. Middleware in the same
`DynStack` runs in list order, outermost first. Each dynamic layer costs one boxed future per call,
against zero for the static layers, so keep the static chain as the default and reach for `DynStack`
only where runtime composition earns it.

## Publish-side middleware

The middleware above runs on the consume path (incoming messages). The publish path has its own
pipeline; see [Publishing and replies](publishing.md#the-publish-pipeline).

## Built-in layers

- `layers::TracingLayer` emits a tracing event per message (DEBUG on arrival, INFO on ack, WARN on
  nack). To render those events on the console, enable the `logging` feature; see
  [Logging](logging.md).
- The `metrics` feature ships a layer that records Prometheus counters and a duration histogram; see
  [Metrics](metrics.md).
