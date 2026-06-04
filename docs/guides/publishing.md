# Publishing and replies

There are two ways to publish: return a reply from a handler, or publish explicitly from inside a
handler through a named publisher. Both run through the publish pipeline.

## Replying from a handler

Name a reply destination with `publish(..)` and return the reply value. The runtime encodes it and
sends it:

```rust
use ruststream::subscriber;

#[subscriber("requests", publish("responses"))]
async fn respond(req: &Request) -> Response {
    Response { ok: true }
}
```

Mount it with `include_publishing`, handing it a [`TypedPublisher`] that carries the broker
connection and the reply codec:

```rust
use ruststream::codec::JsonCodec;
use ruststream::runtime::TypedPublisher;

RustStream::new(info).with_broker(broker, |b| {
    let replies = TypedPublisher::new(b.broker().publisher(), JsonCodec);
    b.include_publishing(respond, JsonCodec, replies);
});
```

The first `JsonCodec` decodes the incoming request; the `TypedPublisher`'s codec encodes the reply.
They are separate seams, so the request and reply formats can differ.

## Publishing from inside a handler

To publish to a destination other than a single reply (fan-out, side effects, routing to a different
broker), register a named publisher on the application and resolve it from the context.

```rust
// register at build time
let app = RustStream::new(info)
    .publisher("egress", egress_publisher)
    .with_broker(broker, |b| b.include(forward, JsonCodec));
```

```rust
use ruststream::runtime::{Context, HandlerResult, Outgoing};

#[subscriber("ingress")]
async fn forward(event: &Event, ctx: &mut Context<'_>) -> HandlerResult {
    if let Some(publisher) = ctx.publisher("egress") {
        let out = Outgoing::new("egress", serde_json::to_vec(event).unwrap());
        let _ = publisher.publish(out).await;
    }
    HandlerResult::Ack
}
```

!!! note "Handlers that publish must own their context"
    A closure handler cannot return a future that borrows `&mut Context`. Use a `#[subscriber]`
    handler (as above) or a struct handler with `async fn handle`, both of which own the borrow
    across awaits.

## The publish pipeline

Two kinds of transform run before a message leaves the process, and they compose:

- **Static `PublishLayer`** on a `TypedPublisher`, added with `.layer(..)`. Zero-cost,
  per-destination transforms (an envelope, a fixed content type). They run first, closest to the
  value.
- **Dynamic publish middleware** on the application, added with `.publish_layer(..)`. Cross-cutting
  concerns (publish metrics, a dead-letter wrapper) applied to every published message. They run
  outside the static layers, then the message is sent.

```rust
// static, per-publisher
let replies = TypedPublisher::new(b.broker().publisher(), JsonCodec)
    .layer(EnvelopeLayer);

// dynamic, app-wide
let app = RustStream::new(info)
    .publish_layer(metrics.publish_layer())
    .with_broker(broker, |b| b.include_publishing(respond, JsonCodec, replies));
```

A static `PublishLayer` implements `apply(&mut Outgoing)`. A dynamic middleware implements
`PublishMiddleware` with an around/next signature, so it can short-circuit, retry, or observe.
Manual publishes through `ctx.publisher(..)` run through the dynamic pipeline (the static layer is a
property of a specific `TypedPublisher`).

## Batch publishing

There is no batch-publish API. For most brokers (NATS, Kafka) the client already coalesces writes,
so a per-message `publish` loop achieves the same throughput. Where a broker has a genuine pipeline
primitive (Redis), the broker crate exposes it as a broker-specific capability.
