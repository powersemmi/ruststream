# Subscribers

A subscriber binds a handler to one subscription. The `#[subscriber]` macro is the ergonomic way to
declare one; this guide covers the handler contract, the macro forms, and how handlers are mounted.
Grouping handlers into modules is covered in [Routing](routing.md), and how payloads are decoded in
[Codecs](codecs.md).

## The handler contract

A handler is an `async fn` whose first parameter is a reference to the decoded payload:

```rust
use ruststream::runtime::HandlerResult;
use ruststream::subscriber;

--8<-- "examples/subscribers.rs:contract"
```

The macro turns the function into a value named after it (here `handle`) that implements the
mounting contract. You pass that value to `include`.

### Accepting the context

Declare an optional second parameter, `&mut Context`, to read headers, the subscription name, and
shared state, or to publish from inside the handler:

```rust
--8<-- "examples/subscribers.rs:context"
```

The macro resolves the context type itself, so the `Context` name needs no import when it appears
only in `#[subscriber]` signatures. The full context surface - the headers working copy, state
access, broker per-delivery fields - is covered in [Context and state](context.md).

### Extractor parameters

Any further parameter, after the message and the optional `&mut Context`, is an **extractor**:
the runtime resolves it from the delivery before the body runs, and a failed extraction settles
the delivery without running the body. Three kinds can appear:

- `State<T>` - a field of the application state (derive `FromRef` on the state type).
- `Ctx<K>` - a broker per-delivery field, read by its key.
- any type implementing `FromContext` - a custom extractor (an auth guard, a request-scoped
  resolver).

The mechanics live in
[Injecting dependencies](context.md#injecting-dependencies-extractor-parameters) and
[Context fields as parameters](context.md#context-fields-as-parameters).

One more parameter shape is not an extractor but an **injection**: `Out(out): Out<P>` receives
a live publisher paired by the runtime from the source attached at the include site
(`b.include(handler).publisher(..)`). See
[Publishing from inside a handler](publishing.md#publishing-from-inside-a-handler).

### Acking

The return type is anything that converts into a [`Settle`] (the settlement unit: an outcome plus
an optional post-settle continuation):

| Return value | Result |
|---|---|
| `HandlerResult::ack()` (or `HandlerResult::Ack`) | acknowledge; the broker removes the message |
| `HandlerResult::retry()` | nack with requeue (redeliver later) |
| `HandlerResult::retry_after(delay)` | nack asking for redelivery no sooner than `delay` |
| `HandlerResult::drop()` | nack without requeue (discard or dead-letter) |
| `()` | always `Ack` |
| `Result<(), E>` | `Ack` on `Ok`, `drop` on `Err` |
| `Result<HandlerResult, E>` | the inner outcome on `Ok`, `drop` on `Err` |
| `Settle` (any of the above `.and_after(..)`) | settle by the outcome, then run the continuation |

On the message itself, ack consumes `self`, so the type system prevents acking twice.

### Post-settle continuations

`HandlerResult::ack().and_after(fut)` attaches a continuation to the returned outcome - a
non-critical notification, slow follow-up work, a cache warm-up. Any outcome works
(`drop().and_after(..)` is valid; the neutral reading is "after settle"):

```rust
--8<-- "examples/post_settle.rs:single"
```

The continuation follows the shared post-settle semantics (at-most-once, runs only after the
ack or nack settles, drained on graceful shutdown); see
[Post-settle hooks](context.md#post-settle-hooks).

In a batch each element settles individually, so the continuation rides per element - a capability
the per-message context hook cannot offer:

```rust
--8<-- "examples/post_settle.rs:batch"
```

Batch *publishing* (`batch(..) + publish(..)`) settles all-or-nothing under one transaction, so
per-element `and_after` does not compose there; it applies to plain batch and single forms only.

### Delayed redelivery

`retry_after` covers the not-ready-yet case (a dependency has not arrived, an upstream is
rate-limited), where an immediate redelivery would just spin:

```rust
--8<-- "examples/retry.rs:retry_after"
```

Under the hood, the runtime honours the delay:

- A broker with native delayed redelivery (the memory broker re-delivers on a timer; a
  NATS JetStream broker could `NAK` with delay) hands off to the transport directly.
- On a broker without native support, the runtime schedules a **deferred re-publish** of the
  message to its own source subject after `delay`, then drops the original. The re-published
  copy carries the framework retry-count header
  ([`RETRY_COUNT_HEADER`](https://docs.rs/ruststream/latest/ruststream/runtime/constant.RETRY_COUNT_HEADER.html))
  incremented; a handler can read it to cap redeliveries.

  Opt in per scope with
  [`BrokerScope::retry_via(publisher)`](https://docs.rs/ruststream/latest/ruststream/runtime/struct.BrokerScope.html#method.retry_via)
  (the publisher must target the same broker). Without a publisher the delay is dropped and the
  message is requeued immediately. The deferred re-publish is **at-most-once** over the delay
  window: if the process exits before the timer fires the copy is lost.

The `batch_retry_after` form composes with
[selective batch outcomes](#selective-acknowledgement): a `Vec<HandlerResult>` carries
per-element delays, so pending entries back off without holding up the rest of the page:

```rust
--8<-- "examples/retry.rs:batch_retry_after"
```

## Choosing the subscription source

### By name

`#[subscriber("orders")]` subscribes by name. It works with any broker that implements the
`Subscribe` capability, which covers the common case.

### Broker-specific descriptors

When a subscription needs broker-specific options (a consumer group, a durable name, a delivery
policy), the broker crate exposes a descriptor type. Use its constructor directly in the decorator:

<!-- inline-rust: illustrative descriptor sketch; OrdersStream is a stand-in for a broker crate's SubscriptionSource type, which lives in another crate and has no in-repo compiled home (the real NATS form is pulled in just below) -->
```rust
#[subscriber(OrdersStream::new("orders", "workers"))]
async fn handle(order: &Order) -> HandlerResult {
    HandlerResult::Ack
}
```

The macro reads the descriptor type out of the constructor call, so the compiler checks the
descriptor against the broker it is mounted on. A descriptor is any type that implements
`SubscriptionSource<B>`; see [Broker authors](../broker-authors/index.md#subscription-sources).

The source may also be a builder chain on that constructor, so fluent options stay inline. For
example, a broker that ships an options builder lets a handler name a specific stream and consumer
right in the decorator:

<!-- inline-rust: illustrative builder-chain source; the concrete options type lives in a broker crate, so there is no in-repo compiled home -->
```rust
#[subscriber(StreamOptions::new("orders").durable("audit"))]
async fn handle(order: &Order) -> HandlerResult {
    HandlerResult::Ack
}
```

The macro follows the chain down to the base `Type::new(..)` to name the source type, so each method
in the chain must return `Self`. Free functions are rejected, since their type is not visible to the
macro.

## Mounting handlers

Inside `with_broker`, mount a definition with `include`:

<!-- inline-rust: minimal include mount fragment with placeholder info/broker; the full compiled program is examples/subscribers.rs (its app is pulled in via other anchors on this page) -->
```rust
RustStream::new(info).with_broker(broker, |b| {
    b.include(handle);
});
```

`include` decodes the payload with the codec resolved from the most specific level you set - per
handler, per scope, or the feature-selected default. See
[where the codec comes from](codecs.md#where-the-decode-codec-comes-from).

To group handlers per module and mount them all at once, collect them into a `Router`; see
[Routing](routing.md).

## Batch subscribers

Wrapping the source in `batch(..)` switches the handler to whole-batch consumption: it takes the
decoded batch as a slice and runs once per batch the broker delivers - one database round-trip,
one bulk API call.

```rust
--8<-- "examples/subscribers.rs:batch"
```

Mount it with `include_batch` (the batch counterpart of `include`):

```rust
--8<-- "examples/subscribers.rs:batch_mount"
```

The source's subscriber must implement the `BatchSubscriber` capability. Brokers whose clients
batch natively (Kafka poll, JetStream pull consumers) expose it directly, and batch sizing lives
in their subscription options; the in-memory broker batches natively too. For any other source,
the `Buffered` adapter buffers single deliveries client-side, closing a batch by size or by a
deadline after its first delivery:

```rust
--8<-- "examples/subscribers.rs:batch_buffered"
```

The semantics differ from single-message handlers in a few ways:

- Elements that fail to decode are nacked individually (per the decode-failure policy) and never
  reach the handler; the rest arrive as one slice.
- The returned value settles the batch. A single `HandlerResult` (or `()` / `Result<_, E>`)
  settles **every** message uniformly: `Ack` acks them all, `retry()` requeues them all.
- Per-message headers are not accessible in the `&[T]` form, and the context starts with empty
  headers.
- App-global and router middleware wrap per-message handlers and do not apply to batch
  registrations.

### Selective acknowledgement

A common case is partial readiness: some messages of the page are processed, others are not
ready yet and should be redelivered without retrying the ones that succeeded. Return
`Vec<HandlerResult>` to settle element `i` of the slice with outcome `i`:

```rust
--8<-- "examples/subscribers.rs:batch_selective"
```

Broker semantics are exactly those of per-message `nack(requeue = true)`: brokers with
per-message redelivery honour selective retry natively; a positional broker degrades the same
way it does for a single-message nack (the crate of that broker documents it). Returning a
vector whose length does not match the batch is a bug in the handler: the unmatched remainder is
retried (an extra redelivery beats a silently lost message) and the mismatch is logged.

## Seeking

Brokers whose transport is a replayable log (Kafka, Redis streams, the in-memory broker's
publish log) implement the `Seekable` capability: a live subscription can be moved to another
position - replaying a stream after fixing a handler bug, reprocessing from a known point, or
skipping forward past a poison region - without dropping the subscription. Brokers without a
replayable log simply do not implement it, and the mount below fails to compile against them
instead of failing at runtime.

A handler repositions its own subscription through a `Seek` parameter. The runtime mints the
seeker off the subscription right after it opens, so the handler always holds a live handle;
nothing is attached at the include site:

```rust
--8<-- "examples/seek.rs:handler"
```

```rust
--8<-- "examples/seek.rs:mount"
```

A seek from inside the handler settles the current message as usual; deliveries queued before
the target are dropped, and the stream resumes at the target position.

Positions are broker-owned types (`MemoryPosition` here; a Kafka position carries partition
offsets, a Redis position an entry id) and come from two places with different guarantees: a
position captured from a delivered message (the `Positioned` capability on the message) carries
a pinned contract - seeking to it redelivers exactly that message, then the rest of the log in
order - while a position built with the broker's own constructors (earliest, a sequence number,
a timestamp) keeps the semantics that broker documents.

### Starting position

A subscription can also open at a chosen position instead of the broker's default: the
`start_at(<position>)` clause seeks before the first delivery, so "start from the latest on
deploy" or "replay the whole log into a fresh subscription" is part of the subscriber's
declaration, not an operational action afterwards:

```rust
--8<-- "examples/seek.rs:start_at"
```

The clause forces the position on every startup; without it the subscription simply opens at
the broker's default. A conditional default - apply only when the broker holds no stored
cursor for the group (Kafka's offset reset, a JetStream deliver policy) - stays on the
broker's own subscription descriptor, which expresses it natively.

What one seek covers differs per broker - repositioning a consumer instance (Kafka) moves that
instance only, repositioning a shared group cursor (Redis streams) moves the whole group - and a
reposition invalidates any ack bookkeeping the broker keeps for the subscription; the broker
crate documents both. Broker authors prove the contract with the
[`capabilities::seeking` conformance suite](../broker-authors/conformance.md#capability-suites).

## Raw subscribers

When the payload is not a serialized value at all (a binary frame, a foreign wire format you
parse yourself), the `raw` clause takes the codec out of the path entirely: the handler receives
each delivery's bytes exactly as the broker handed them over.

```rust
--8<-- "tests/raw_subscriber.rs:raw"
```

The message parameter must be `&[u8]` - a serde-typed parameter under `raw` is a compile error,
as is `raw` combined with `batch(..)`, an injected `Out` publisher, or an
`on_failure(decode = ..)` policy (there is no decode step to fail). Extractors, `&mut Context`,
`workers(..)`, and `on_failure(panic = ..)` work unchanged, and a raw subscriber mounts with the
same `include` as every other definition - a scope codec, when one is set, simply does not apply
to it. Because no codec is involved, raw subscribers are also the one subscriber form available
with no codec feature enabled at all. For a custom serialization format you want *typed*
handlers for, implement [`Codec`](codecs.md) instead and keep the typed path.

A raw subscriber can also reply in kind: the `publish_raw("dest")` clause publishes the
returned bytes (`-> Vec<u8>`, or `-> Result<Vec<u8>, HandlerResult>` for the same explicit ack
control as the typed reply form) as-is to the reply name, through the bare publisher attached
at the include site (`b.include(relay).publisher(policy)`, or the broker's default publish
policy without the call) - no codec on either side, and a failed reply publish nacks the
delivery with requeue:

```rust
--8<-- "tests/raw_subscriber.rs:raw_reply"
```

`publish_raw` is not tied to a raw input: on a typed handler it makes only the *reply* raw - the
input still decodes with the scope codec (and keeps the decode failure policy), while the
returned bytes go out unencoded. That is the gateway shape, consuming structured messages and
emitting a wire format the handler produced itself:

```rust
--8<-- "tests/raw_subscriber.rs:raw_reply_typed"
```

The encoded `publish(..)` clause under `raw` is rejected (a raw handler's reply is bytes -
`publish_raw` is the fix the error names), as is combining both reply clauses on one handler.

## Worker pools

The dispatch loop is sequential per subscriber: one delivery is handled and settled before the
next is pulled, so one slow handler stalls the whole subscription. A `workers(n)` clause
processes up to `n` deliveries of this subscriber concurrently, each in its own task on the
multi-thread runtime:

```rust
--8<-- "examples/subscribers.rs:workers"
```

Back-pressure holds: the stream is not polled while `n` deliveries are in flight, which plays
well with broker-side limits like JetStream `max_ack_pending`. **Global processing order is lost
by design** - if any ordering matters, either stay sequential or use keyed lanes:

```rust
--8<-- "examples/subscribers.rs:workers_by_key"
```

`workers(n, by_key)` runs `n` sequential lanes. A delivery goes to the lane its partition key
hashes to, so messages sharing a key never overlap or reorder - the in-process analogue of
Kafka partition semantics. The key comes from the broker message's `partition_key()` (brokers
whose messages implement the `Partitioned` capability expose it; the in-memory broker reads the
`partition-key` header). Messages without a key rotate over the lanes. `by_key` applies to
single-message subscribers; batch forms take a plain `workers(n)` pool of batches.

On shutdown, the subscriber stops pulling new deliveries and in-flight workers drain under the
app's `shutdown_timeout`.

## Composition rules

The subscriber features compose; these are the rules at each intersection, each pinned by an
integration test.

| Combination | Rule |
|---|---|
| `workers(n)` × `batch(..)` | The pool holds up to `n` **batches** in flight. `by_key` does not apply to batch forms: lanes order single messages per key, and the macro rejects the combination at compile time. |
| `retry()` / `retry_after` × `workers(n)` | Retried deliveries re-enter the pool and complete like any other delivery. |
| `retry()` / `retry_after` × `workers(n, by_key)` | Retries complete, but per-key ordering across a retry is **not** promised: a requeued message rejoins the stream from the back. If a key's messages must stay ordered even through failures, the handler has to absorb the failure instead of nacking. |
| `.transactional()` × `workers(n)` | One transaction per batch, exactly as in the sequential loop. Concurrent batches run concurrent, independent transactions; each stays atomic (commit-then-ack per batch). |
| `Buffered` × `workers(n)` | Batches still close by `max_size` / `max_wait` only; the pool bounds how many closed batches are processed at once and never affects batch boundaries. |
| `publish(..)` × `workers(n)` | Replies are produced concurrently, so reply order across deliveries is not promised. A failed reply publish retries only its own delivery. |
| middleware × `batch(..)` | App-global and router layers wrap per-message handlers and do not apply to batch registrations (a per-message layer cannot wrap a whole-batch handler). |

## Macro or manual

`#[subscriber]` is sugar over a generic API. The macro generates a typed handler and its metadata;
you can write the same registration by hand with `typed` (which decodes the payload), a closure or
struct handler, and `HandlerMetadata`. Both forms below register the same handler.

=== "Macro"

    ```rust
    use ruststream::subscriber;

    --8<-- "examples/subscribers.rs:contract"

    // inside with_broker(...):
    b.include(handle);
    ```

=== "Manual"

    ```rust
    use ruststream::Name;
    use ruststream::codec::JsonCodec;
    use ruststream::runtime::{Context, HandlerMetadata, HandlerResult, typed};

    // inside with_broker(...):
    --8<-- "examples/subscribers.rs:manual"
    ```

Reach for the manual form when a handler needs state the macro cannot express (a struct handler with
fields), or to set a non-default [decode-failure policy](codecs.md#decode-failures). Otherwise the
macro is less to maintain.

## Publishers

A handler that produces a reply is a publisher. See [Publishing and replies](publishing.md).
