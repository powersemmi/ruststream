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
    .with_broker(broker, |b| b.include(handle, JsonCodec));
```

The first layer added is the outermost. The global stack is static: it has zero runtime dispatch
cost, and its type grows as you call `layer`.

!!! note "Routers opt out"
    The global stack does not wrap handlers brought in via `include_router`, because a router is
    finalized independently. Wrap those handlers inside the router if you need the same behaviour.

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

## Dynamic middleware

The static layers above are resolved at compile time. When you need a runtime-built chain (a list of
layers decided from config, or layers stored behind `dyn`), use the dynamic stack: `DynStack`,
`DynMiddleware`, and `Next`. A `DynMiddleware` has an around/next signature, so it can short-circuit
or wrap the call:

```rust
use ruststream::runtime::{DynMiddleware, Next};
```

Each dynamic layer costs one boxed future per call, against zero for the static layers, so prefer
static layers unless you genuinely need runtime composition.

## Publish-side middleware

The middleware above runs on the consume path (incoming messages). The publish path has its own
pipeline; see [Publishing and replies](publishing.md#the-publish-pipeline).

## Built-in layers

- `layers::TracingLayer` emits a tracing span per message.
- The `metrics` feature ships a layer that records Prometheus counters and a duration histogram; see
  [Metrics](metrics.md).
