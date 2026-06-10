# Publishing and replies

There are two ways to publish: return a reply from a handler, or publish explicitly from inside a
handler through a named publisher. Both run through the publish pipeline.

## Replying from a handler

Name a reply destination with `publish(..)` and return the reply value. The runtime encodes it and
sends it:

```rust
use ruststream::subscriber;

--8<-- "examples/publishing.rs:reply"
```

Mount it with `include_publishing`, handing it a [`TypedPublisher`] that carries the broker
connection and the reply codec (`TypedPublisher::new` uses the default codec; name one with
`TypedPublisher::with_codec`). `include_publishing` reuses that codec to decode the request too:

```rust
use ruststream::runtime::TypedPublisher;

RustStream::new(info).with_broker(broker, |b| {
    let replies = TypedPublisher::new(b.broker().publisher());
    b.include_publishing(respond, replies);
});
```

The `TypedPublisher`'s codec encodes the reply, and `include_publishing` reuses it to decode the
incoming request. To decode the request with a different codec, mount with `include_publishing_on`
and name it explicitly; see [Codecs](codecs.md#the-publish-side).

## Publishing from inside a handler

To publish to a destination other than a single reply (fan-out, side effects, routing to a different
broker), register a named publisher on the application and resolve it from the context.

```rust
// register at build time
let app = RustStream::new(info)
    .publisher("egress", egress_publisher)
    .with_broker(broker, |b| b.include(forward));
```

```rust
use ruststream::runtime::{HandlerResult, Outgoing};

--8<-- "examples/publishing.rs:forward"
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

A static `PublishLayer` implements `apply(&mut Outgoing)`:

```rust
--8<-- "examples/publishing.rs:static_layer"
```

A dynamic middleware implements `PublishMiddleware` with an around/next signature, so it can
short-circuit, retry, or observe:

```rust
--8<-- "examples/publishing.rs:dynamic_middleware"
```

Both levels compose on the application:

```rust
--8<-- "examples/publishing.rs:pipeline"
```

Manual publishes through `ctx.publisher(..)` run through the dynamic pipeline (the static layer is a
property of a specific `TypedPublisher`). The full program is
[`examples/publishing.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/publishing.rs).

## Batch publishing

There is no batch-publish API. For most brokers (NATS, Kafka) the client already coalesces writes,
so a per-message `publish` loop achieves the same throughput. Where a broker has a genuine pipeline
primitive (Redis), the broker crate exposes it as a broker-specific capability.
