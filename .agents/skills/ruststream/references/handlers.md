# Handlers: outcomes, settings, extractors

What a handler receives, how it settles, and the settings a mount site fills in. Component pages: [Subscribers](https://powersemmi.github.io/ruststream/latest/components/subscribers/), [Messages](https://powersemmi.github.io/ruststream/latest/components/subscribers/messages/), [Context and extractors](https://powersemmi.github.io/ruststream/latest/components/subscribers/context/), [Dispatch](https://powersemmi.github.io/ruststream/latest/components/subscribers/dispatch/).

## Contents

- Handler outcomes
- Post-settle work (`and_after`)
- Settings the mount site fills in
- Raw payloads
- Broker context fields (`Ctx<K>`)
- Typed headers (`Headers<T>`)
- Context (optional explicit parameter)

## Handler outcomes

Every settlement is one of four constructors: a handler then reads the same whether it acks or
nacks, and every outcome chains into `and_after`.

```rust
HandlerOutcome::ack()                 // ack
HandlerOutcome::retry()               // nack, requeue
HandlerOutcome::drop()                // nack, no requeue
HandlerOutcome::retry_after(delay)    // nack with a delay - not ready yet, do not spin

// `()` always acks; `Result<(), E>` acks on Ok, nacks on Err
async fn handle(order: &Order) -> Result<(), MyError> { Ok(()) }
```

The broker status inside is not a public enum: read an outcome with the predicates `is_ack()`,
`is_retry()`, `is_drop()` and `retry_delay()`. On a broker without native delayed redelivery the
runtime honours `retry_after` by publishing a copy after the delay. Every registration whose copies
the runtime publishes already has a publisher for them, from the broker's default publish policy;
`.out_retry(policy)` (`.out(Retry, policy)` spelled out) replaces it. The position is an `Out`
slot, so `.codec(..)` and `.transform(..)` after it apply to the copy - which carries the delivery's
own bytes, so nothing re-encodes it.

A registration caps its retries at the mount site, right after `include` and before
`out_retry(..)`: `b.include(reconcile).max_attempts(nonzero!(5)).dead_letter("orders.dead")`.
Where the subscription's descriptor addresses no copies (a wildcard, an MQTT filter, a pattern),
`.out_retry(policy).to(name)` names where they go, or a transform on the position names one per
delivery; a registration that names neither is a compile error on a `Router` chain and refuses to
start on a scope. Each step is declared once. Below the cap a copy goes back to the subscription; at the cap it goes to
the dead-letter destination, or the delivery is rejected where none is named. A destination
declared without a cap takes over every copy. The count is the broker's own where the transport
keeps one (`IncomingMessage::redelivery_count`) and the framework's `x-ruststream-retry-count`
header otherwise, never both, and an immediate `retry()` obeys the same cap. A broker with native
delayed redelivery gets the delay only below the cap: the count is read first, and a delivery at
the cap is carried away instead of coming back - on a transport that counts its own deliveries and
on one whose crate honours the delay by publishing a copy that carries the header. The generated
`AsyncAPI` document reports the dead-letter destination as a channel the registration publishes
to.

## Post-settle work (`and_after`)

`and_after` attaches a continuation that runs **after** the message is settled and returns the
`HandlerOutcome` it was called on (so the handler's return type stays `HandlerOutcome`, or
`Vec<HandlerOutcome>` on a batch). Use it for a side effect that must not gate the settlement
decision or affect redelivery: a notification, slow follow-up work, a cache warm-up.

```rust
use ruststream::prelude::*;

#[subscriber("orders")]
async fn handle(order: &Order) -> HandlerOutcome {
    let id = order.id;
    HandlerOutcome::ack().and_after(async move {
        tracing::info!(id, "acked; notifying downstream");
    })
}

// per element on a batch: the continuation rides with its own message
#[subscriber("orders")]
async fn handle_batch(orders: &[Order]) -> Vec<HandlerOutcome> {
    orders
        .iter()
        .map(|order| {
            if order.id == 0 {
                HandlerOutcome::retry() // a plain outcome, no continuation
            } else {
                let id = order.id;
                HandlerOutcome::ack().and_after(async move { follow_up(id).await })
            }
        })
        .collect()
}
```

It chains off any outcome, `drop().and_after(..)` included. The continuation is **at-most-once**:
the message is already settled when it runs, so a panic or a crash never redelivers it. Work whose
loss must redeliver the message does not go here - settle by outcome and let the broker retry.
Graceful shutdown drains continuations still in flight. A continuation runs where the delivery ran:
on a `threads(n)` subscription, on the delivery's thread.

## Settings the mount site fills in

A `#[subscriber]` with no source in the attribute leaves the name to the mount; the definition
value carries a builder for the declarative settings, applied before `include` takes it.

```rust
use std::time::Duration;

// FailurePolicies, FailurePolicy and nonzero! ride the prelude; no second import
use ruststream::memory::prelude::*;

#[subscriber]
async fn bill(order: &Order) -> HandlerOutcome {
    HandlerOutcome::ack()
}

// in with_broker(broker, |b| { ... }):
b.include(
    bill.name("orders")
        .workers(nonzero!(4))
        .on_failure(FailurePolicies::default().with_decode(FailurePolicy::Skip)),
);

// a batch body: `batch(n)` is the batch size, the one batch parameter the framework carries, and it
// is mandatory - a batch handler mounted without it does not compile. The size goes to the broker,
// which builds its batches to it; the body sees exactly what the broker delivered. It applies to
// every batch shape and composes with a start position
b.include(settle.name("orders").batch(nonzero!(64)));
b.include(
    drain
        .name("orders")
        .batch(nonzero!(128))
        .start_at(MemoryPosition::start()),
);

// a broker's own batching options ride its subscription source, after the core's size:
// b.include(settle.name("orders").batch(nonzero!(6)).block(Duration::from_secs(5)));

// where the subscription opens, on a broker with a replayable log (Seekable)
b.include(audit.name("orders").start_at(MemoryPosition::start()));
```

`workers(n)`, `threads(n)`, `on_failure(..)` and `start_at(..)` can also be written as attribute
clauses (`#[subscriber("orders", workers(16, by_key))]`, `#[subscriber("images", threads(8))]`,
`#[subscriber("orders", on_failure(decode = retry))]`,
`#[subscriber("audit", start_at(MemoryPosition::start()))]`); those five (`reply` is the fifth)
are the only clauses, and `batch(n)` is mount-site only. `workers(n)` is concurrency for I/O on
the app's runtime; a handler that computes takes `threads(n)` (or `threads(n, by_key)`): `n`
dedicated threads of the subscription's own, each a current-thread runtime, where the handler
runs from the first poll to the end. The two fill one position, so a subscription names one.
What a `threads(n)` delivery leaves behind (a `tokio::spawn`, an `and_after` continuation, an
`after(..)` hook, a `retry_after` timer) stays on its thread; work goes to the app's runtime only
through `main.spawn(fut)` or `main.run(fut).await` on `Ctx(main): Ctx<MainRuntime>` (or
`ctx.main_runtime()`). A service with
`threads(n)` sizes the app's runtime for its I/O: `#[ruststream::app(flavor = "current_thread")]`
or `#[ruststream::app(worker_threads = 2)]`. The `poll-diagnostics` feature finds the handlers
that belong on `threads(n)`: every subscription samples its handler's time in `poll` and warns
when the average crosses the threshold; `.poll_diagnostics(PollDiagnostics::new().threshold(..))`
on `RustStream`, before `with_broker`, tunes it, and `metrics.observe_poll_diagnostics(&d)` or
`otel.observe_poll_diagnostics(&d)` exports it. `start_at` forces the position on
every startup and takes a value of the broker's own position type; without it the subscription
opens at the broker's default. A conditional default (apply only when no stored cursor exists for
the group) belongs on the broker's own subscription descriptor.

Seeking needs a log to seek in. `MemoryBroker::new()` keeps nothing, so the two forms are different
types: `MemoryBroker::retaining(Retention::Messages(nonzero!(64)))` is the one whose subscriptions
are `Seekable`, and a `start_at(..)` or a `SeekHandle` read only compiles against it. A handler
repositions its own subscription through the broker's context keys
(`Ctx(seeker): Ctx<SeekHandle>`, then `seeker.seek(MemoryPosition::sequence(3))`); a position the
retention bound already evicted reports `MemoryError::PositionEvicted`.

## Raw payloads

When the payload is not a serialized value at all (a binary frame, a foreign wire format you parse
yourself), the payload type deserializes itself and the codec stays out of the path. The mnemonic
is in the trait names: `Deserialize` means the framework's codec builds the value, `Deserialized`
means the type builds itself. A scope codec does not apply, and this is the one subscriber form
available with no codec feature enabled at all. A bare `&[u8]` parameter is a compile error naming
the derive.

```rust
#[derive(Deserialized)]
struct Frame<'a>(&'a [u8]);

#[subscriber("frames")]
async fn ingest(frame: &Frame<'_>) -> HandlerOutcome {
    parse(frame.0);
    HandlerOutcome::ack()
}

// the form rule does not change with the lane - `&[Frame<'_>]` is the batch, the spelling
// comes with the derive, and the elements are borrowed from the batch's own messages for
// the duration of the call, so nothing is copied
#[subscriber("frames")]
async fn ingest_batch(frames: &[Frame<'_>]) -> HandlerOutcome {
    HandlerOutcome::ack()
}
```

`on_failure(decode = ..)` applies here too: a constructor that validates (a flatbuffers root, a
length check) returns `Err` from `from_payload` and that policy settles the delivery, the same rung
a codec decode failure lands on. Extractors, `&mut Context`, `workers(..)`,
`on_failure(panic = ..)` and the injected `Out` parameters work unchanged on the single-delivery
shape; a batch of frames mounts in the plain form, with no reply and no `Out`.

There is one `reply("dest")` clause, and the reply *type* picks the wire: a `serde::Serialize`
reply encodes through the reply codec, a `#[derive(Serialized)]` newtype leaves byte-for-byte. The
two lanes are independent, so raw in / encoded out and decoded in / raw out (the gateway shape)
both compile. A `Vec<u8>` reply encodes like any `Serialize` value, and a byte reply is a single
reply: a batch's replies always go through the reply codec.

```rust
#[derive(Outgoing, Serialized)]
struct Export(Vec<u8>);

#[subscriber("relay-in", reply("relay-out"))]
async fn relay(frame: &Frame<'_>) -> Export {
    Export(frame.0.to_vec())
}
```

A `Serialized` reply names its publish policy at the include site the same way an encoded one does
(`b.include(relay).out_reply(Publish)`, or nothing for the broker's default); what it does not take
is the wiring steps after it, since its bytes leave with no codec and no transform stack.

A binary protocol (Protobuf, Cap'n Proto, FlatBuffers) is not a codec: the generated message *is*
its encoding, so it rides the same lanes and the model type stays visible at the mount site, in a
slot's dictionary and in the document. Three derives and one attribute put a whole schema there,
and a generator writes them for you (`prost_build`'s `message_attribute`):

```rust
#[derive(Clone, PartialEq, prost::Message, Outgoing, Serialized, Deserialized)]
#[outgoing(name = "orders")]
#[wire(prost)]
struct Order {
    #[prost(uint64, tag = "1")]
    id: u64,
}
```

`#[wire(prost)]` is shorthand for one generator's pair of functions; the general form names them,
`#[wire(encode = <path>, decode = <path>)]`, where `encode` is a `fn(&Self, &mut BytesMut)` and
`decode` a `fn(&[u8]) -> Result<Self, E>`. Neither lane resolves a codec, so such a service
compiles with no codec feature at all. For a custom serialization format you want *typed* handlers
for, implement `Codec` instead and keep the typed path.

## Broker context fields (`Ctx<K>`)

`Ctx<K>` injects one field of the broker's per-delivery context, the way `State<T>` injects a
state component. The key `K` is a broker-provided `ContextField` type; a handler using only `Ctx`
extractors needs no `&mut Context` parameter at all.

```rust
use ruststream_rdkafka::prelude::*; // the context keys ride the broker's prelude too

#[subscriber("orders")]
async fn handle(order: &Order, Ctx(partition): Ctx<Partition>) -> HandlerOutcome {
    tracing::info!(partition, "delivery");
    HandlerOutcome::ack()
}
```

## Typed headers (`Headers<T>`)

`Headers<T>` parses the delivery headers into a flat contract struct before the body runs; a
missing or unparsable header settles the delivery by `on_failure(decode = ..)` (drop by default).
On a batch the batch pairs each element with its contract: take `&[Message<H, T>]` and read
`element.headers`. The untyped `name -> bytes` map itself is `HeaderMap` (`ctx.headers()`), and
`Headers` is only the extractor.

```rust
use ruststream::prelude::*;

#[subscriber("chunks")]
async fn convert(chunk: &Chunk, Headers(meta): Headers<ChunkMeta>) -> HandlerOutcome {
    tracing::info!(meta.task_id, "converting");
    HandlerOutcome::ack()
}
```

## Context (optional explicit parameter)

When you need the raw delivery objects, take `ctx: &mut Context<'_, C, S>` as the second parameter,
right after the payload (`C` = broker context type, `S` = application state):

- `ctx.name()` - the subject/channel the message arrived on.
- `ctx.headers()` / `ctx.headers_mut()` - the working copy of the message headers (`HeaderMap`).
- `ctx.state()` - the shared application state built by `on_startup`.
- `ctx.context(KEY)` - one broker field by compile-time key, the `Ctx<K>` extractor's other half.
- `ctx.set(KEY, value)` - a per-delivery scratch value, for middleware.
- `ctx.after(outcome).then(fut)`, `ctx.after_ack(fut)`, `ctx.after_settle(fut)` - a post-settle
  hook selected by how the delivery settles.

State is typed (`State<T>` / `ctx.state()`), and publishing goes through a `reply` clause or an
`Out` parameter.

On a batch handler `C` is the broker's *subscription-scoped* context, not its per-delivery one:
name it (`ctx: &mut Context<'_, MemoryBatchContext>`) to read what the whole subscription shares -
a seek handle, say - which the runtime builds once per batch. A position or a header belongs to
one delivery, so it rides the batch's elements (`&[Message<H, T>]`) instead, and a batch body
naming the per-delivery context type does not compile.
