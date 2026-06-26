# Publishing and replies

There are two ways to publish: return a reply from a handler, or publish explicitly from inside a
handler through a publisher held in the typed application state.

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

<!-- inline-rust: minimal mount fragment for a reply publisher; the full build wiring is compiled in publishing.rs:pipeline, pulled in later on this page -->
```rust
use ruststream::runtime::TypedPublisher;

RustStream::new(info).with_broker(broker, |b| {
    let replies = TypedPublisher::new(b.broker().publisher());
    b.include_publishing(respond, replies);
});
```

The `TypedPublisher`'s codec encodes the reply, and `include_publishing` reuses it to decode the
incoming request. To decode the request with a different codec, set a scope default with
`with_broker_codec` (or `Router::with_codec` on a router chain); see
[Codecs](codecs.md#the-publish-side).

## Controlling the acknowledgement

A plain reply form always publishes and acks. Return `Result<Reply, HandlerResult>` instead to
take control: `Ok(reply)` publishes and acks, `Err(result)` publishes nothing and the dispatcher
acts on the returned `HandlerResult` (`HandlerResult::drop()` to dead-letter,
`HandlerResult::retry()` to ask for redelivery):

```rust
--8<-- "examples/publishing.rs:reply_result"
```

The `Result` form is detected from the written signature, so spell it out (a type alias hiding the
`Result` is treated as a plain reply type). Like any handler, a publishing handler may declare an
optional second `&mut Context` parameter to read app state or publish manually.

If the reply publish itself fails (broker rejected it, connection lost), the incoming message is
nacked with `requeue = true`: the broker redelivers it instead of the reply being silently lost.
Make publishing handlers idempotent under redelivery.

## Publishing from inside a handler

To publish to a destination other than a single reply (fan-out, side effects, routing to a different
broker), put the publisher in the [typed application state](lifespan.md) and reach it from the
handler with `ctx.state()`. A publisher is a value like any other shared resource; in the state it
stays typed, so the handler uses its own API directly, with no registry and no runtime lookup.

```rust
use ruststream::codec::{Codec, JsonCodec};
use ruststream::{OutgoingMessage, Publisher};

--8<-- "examples/publishing.rs:forward"
```

The publisher is produced in `on_startup` and stored in the state struct, the same as a database
pool or HTTP client (see [Lifespan](lifespan.md)).

!!! note "Handlers that publish must own their context"
    A closure handler cannot return a future that borrows `&mut Context`. Use a `#[subscriber]`
    handler (as above) or a struct handler with `async fn handle`, both of which own the borrow
    across awaits.

## The publish pipeline

Two kinds of transform run before a message leaves the process, and they compose:

- **Static `PublishLayer`** on a `TypedPublisher`, added with `.layer(..)`. Zero-cost,
  per-destination transforms (an envelope, a fixed content type, or stamping the delivery's trace /
  correlation id onto the reply). They run first, closest to the value.
- **Static `PublishMiddleware`** on the application, added with `.publish_layer(..)`. Cross-cutting
  concerns (publish metrics, a dead-letter wrapper) applied to every published message, around the
  send so they can observe its result. The chain composes into a concrete type, mirroring the
  consume-side stack: the default (no middleware) is a direct send. For a middleware set decided at
  runtime, wrap it in a `PublishDynStack` (the publish counterpart of `DynStack`) and add that.

A static `PublishLayer` implements `apply(&mut Outgoing<'_>, &PublishContext<'_, C>)`; the
`PublishContext` is a read-only view of the delivery that produced the reply (its channel, the
incoming headers, and the broker's typed per-delivery context by `Field` key), so a layer can carry
a value from the incoming message onto the reply:

```rust
--8<-- "examples/publishing.rs:static_layer"
```

A batch handler's replies skip the per-message `.layer(..)` stack; add a transform there with
`.batch_layer(..)`, reusing a per-message `PublishLayer` via `for_batch(layer)`.

A dynamic middleware implements `PublishMiddleware` with an around/next signature, so it can
short-circuit, retry, or observe:

```rust
--8<-- "examples/publishing.rs:dynamic_middleware"
```

Both levels compose on the application:

```rust
--8<-- "examples/publishing.rs:pipeline"
```

The pipeline runs on the reply path (the `publish(..)` form). A publisher held in the state is used
directly, so compose any per-publisher transforms onto it with `TypedPublisher::layer` when you
build it. The full program is
[`examples/publishing.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/publishing.rs).

## Batch replies and transactions

A `#[subscriber(batch(..), publish(..))]` handler consumes a whole decoded batch and returns the
replies for it - the consume-transform-produce pattern. `Ok(replies)` publishes every reply to
the reply name and acks the batch; `Err(result)` publishes nothing and settles the whole batch
with `result` (all-or-nothing: selective per-element outcomes do not compose with a
transaction):

```rust
--8<-- "examples/publishing.rs:batch_publishing"
```

Mount it with `include_batch_publishing`, handing it the reply wiring:

```rust
--8<-- "examples/publishing.rs:batch_publishing_mount"
```

With a plain `TypedPublisher`, each reply publishes independently; a mid-batch failure retries
the whole batch, so the earlier replies may be published again on redelivery (at-least-once).
Calling `.transactional()` on the `TypedPublisher` switches the wiring to one broker transaction
per batch: the runtime begins a transaction, publishes every reply, commits, and only then acks
the incoming batch; any failure aborts, so replies are never half-visible. The method exists
only when the underlying publisher implements the `TransactionalPublisher` capability - for
brokers without transactions the compiler rejects it. The single-message `include_publishing`
forms keep taking a plain `TypedPublisher`: a one-message transaction adds broker round-trips
for no atomicity gain.

## Batch publishing

There is no direct batch-publish API on `Publisher`. For most brokers (NATS, Kafka) the client
already coalesces writes, so a per-message `publish` loop achieves the same throughput. Where a
broker has a genuine pipeline primitive (Redis), the broker crate exposes it as a broker-specific
capability.
