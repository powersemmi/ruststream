# Subscribers

A subscriber runs one of your `async fn`s on every message a subscription delivers. Declare one with
the `#[subscriber]` macro: it reads the handler's signature and generates the definition you mount.
Grouping handlers into modules is [Routing](routing.md); decoding their payloads is
[Codecs](codecs.md).

## The handler contract

A handler is an `async fn` whose first parameter is a reference to the decoded payload:

=== "Macros"

    ```rust
    use ruststream::runtime::HandlerOutcome;
    use ruststream::subscriber;

    --8<-- "examples/subscribers.rs:contract"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/subscribers.rs:contract"
    ```

The macro turns the function into a value named after it (here `handle`) that implements the
mounting contract. You pass that value to `include`.

### Accepting the context

Declare an optional second parameter, `&mut Context`, to read headers, the subscription name, and
shared state, or to publish from inside the handler:

=== "Macros"

    ```rust
    --8<-- "examples/subscribers.rs:context"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/subscribers.rs:context"
    ```

The macro resolves the context type itself, so the `Context` name needs no import when it appears
only in `#[subscriber]` signatures. The full context surface - the headers working copy, state
access, broker per-delivery fields - is covered in [Context and state](context.md).

### Extractor parameters

Any further parameter, after the message and the optional `&mut Context`, is an **extractor**:
the runtime resolves it from the delivery before the body runs, and a failed extraction settles
the delivery without running the body. Four kinds can appear:

- `State<T>` - a field of the application state (derive `FromRef` on the state type).
- `Ctx<K>` - a broker per-delivery field, read by its key.
- `Headers<T>` - the delivery headers parsed into a typed contract; a violation settles by
  the `on_failure(decode = ..)` policy (see [typed headers](headers.md)).
- any type implementing `FromContext` - a custom extractor (an auth guard, a request-scoped
  resolver).

The mechanics live in
[Injecting dependencies](context.md#injecting-dependencies-extractor-parameters) and
[Context fields as parameters](context.md#context-fields-as-parameters).

One more parameter shape is not an extractor but an **injection**: `Out(out): Out<impl
Publisher>` receives a live publisher paired by the runtime from the policy attached at the
include site (`b.include(handler).publisher(..)`, or `.out(marker, ..)` per named slot); the
concrete publisher type never appears in the signature. An optional third position declares
the message set the handler publishes - `Out<impl Publisher, Marker, (A, B)>` - enabling the
dictionary-driven typed publish path ([typed headers](headers.md)). See
[Publishing from inside a handler](publishing.md#publishing-from-inside-a-handler).

### Acking

The return type is anything that converts into a
[`HandlerOutcome`](https://docs.rs/ruststream/latest/ruststream/runtime/struct.HandlerOutcome.html)
(the settlement unit: a broker status plus an optional post-settle continuation):

| Return value | Result |
|---|---|
| `HandlerOutcome::ack()` | acknowledge; the broker removes the message |
| `HandlerOutcome::retry()` | nack with requeue (redeliver later) |
| `HandlerOutcome::retry_after(delay)` | nack asking for redelivery no sooner than `delay` |
| `HandlerOutcome::drop()` | nack without requeue (discard or dead-letter) |
| `()` | always acks |
| `Result<(), E>` | an ack on `Ok`, a drop on `Err` |
| `Result<HandlerOutcome, E>` | the inner outcome on `Ok`, a drop on `Err` |
| `HandlerOutcome::ack().and_after(..)` (any outcome) | settle by the outcome, then run the continuation |

On the message itself, ack consumes `self`, so the type system prevents acking twice.

### Post-settle continuations

`HandlerOutcome::ack().and_after(fut)` attaches a continuation to the returned outcome - a
non-critical notification, slow follow-up work, a cache warm-up. Any outcome works
(`drop().and_after(..)` is valid; the neutral reading is "after settle"):

=== "Macros"

    ```rust
    --8<-- "examples/post_settle.rs:single"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/post_settle.rs:single"
    ```

The continuation follows the shared post-settle semantics (at-most-once, runs only after the
ack or nack settles, drained on graceful shutdown); see
[Post-settle hooks](context.md#post-settle-hooks).

In a batch each element settles individually, so the continuation rides per element - a capability
the per-message context hook cannot offer:

=== "Macros"

    ```rust
    --8<-- "examples/post_settle.rs:batch"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/post_settle.rs:batch"
    ```

Batch *publishing* (a batch handler with `publish(..)`) settles all-or-nothing under one
transaction, so per-element `and_after` does not compose there; it applies to plain batch and
single forms only.

### Delayed redelivery

`retry_after` covers the not-ready-yet case (a dependency has not arrived, an upstream is
rate-limited), where an immediate redelivery would spin without progress:

=== "Macros"

    ```rust
    --8<-- "examples/retry.rs:retry_after"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/retry.rs:retry_after"
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
[selective batch outcomes](#selective-acknowledgement): a `Vec<HandlerOutcome>` carries
per-element delays, so pending entries back off without holding up the rest of the batch:

=== "Macros"

    ```rust
    --8<-- "examples/retry.rs:batch_retry_after"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/retry.rs:batch_retry_after"
    ```

## Choosing the subscription source

The attribute always fixes the *kind* of subscription - a subject, a JetStream consumer, a Redis
stream, a pub/sub channel and a list are different types. What can be left out is the *value*
that fills it, which the mount site then supplies. There are four forms, shortest first:

| Form | The kind | The value |
|---|---|---|
| `#[subscriber]` | the by-name source | from the mount site |
| `#[subscriber(RedisStream)]` | named here | from the mount site |
| `#[subscriber("orders")]` | the by-name source | fixed here |
| `#[subscriber(RedisStream::new("orders").group("w"))]` | named here | fixed here |

### By name

`#[subscriber("orders")]` subscribes by name. It works with any broker that implements the
`Subscribe` capability, which every broker crate in the family does: a name is mapped to the
subscription kind that broker considers its default, and the configuration that kind needs beyond
the name is set once on the broker.

`#[subscriber]` is the same source with its value left out - a name the service only knows while
it wires itself up, a subject built from a shard number, a topic read from configuration:

=== "Macros"

    ```rust
    --8<-- "examples/subscribers.rs:deferred_name"
    ```

    ```rust
    --8<-- "examples/subscribers.rs:name_mount"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/subscribers.rs:deferred_name"
    ```

    ```rust
    --8<-- "examples/manual/subscribers.rs:name_mount"
    ```

Where a kind genuinely needs more than a name to exist - a Pulsar source takes a topic *and* a
subscription name - it does not implement `FromName`, and this form does not compile for it. Write
those kinds out in full.

### Broker-specific descriptors

When a subscription needs broker-specific options (a consumer group, a durable name, a delivery
policy), the broker crate exposes a descriptor type. Use its constructor directly in the decorator:

<!-- inline-rust: illustrative descriptor sketch; OrdersStream is a stand-in for a broker crate's SubscriptionSource type, which lives in another crate and has no in-repo compiled home (the real NATS form is pulled in just below) -->
```rust
#[subscriber(OrdersStream::new("orders", "workers"))]
async fn handle(order: &Order) -> HandlerOutcome {
    HandlerOutcome::ack()
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
async fn handle(order: &Order) -> HandlerOutcome {
    HandlerOutcome::ack()
}
```

The macro follows the chain down to the base `Type::new(..)` to name the source type, so each method
in the chain must return `Self`. Free functions are rejected, since their type is not visible to the
macro.

A source built this way is rebuilt for each mount, so a broker's descriptor type is `Clone`. One
definition can mount on two brokers.

## Settings at the mount site

Name, worker policy, failure policies and the start position are values, so each can be given in
the attribute, at the mount site, or partly in each. The attribute expands into exactly the calls
you would write yourself:

=== "Macros"

    ```rust
    --8<-- "examples/subscribers.rs:builder_settings"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/subscribers.rs:builder_settings"
    ```

A setting the attribute named is fixed in the definition's type, so the mount site cannot name it
again - there is no precedence rule to remember:

<!-- inline-rust: two compile-fail one-liners; a compiling example cannot host code that must not compile (the pinned diagnostics live in tests/ui) -->
```rust
#[subscriber("orders", workers(4))]
async fn handle(order: &Order) -> HandlerOutcome { HandlerOutcome::ack() }

b.include(handle.name("other"));    // does not compile: the name is already given
b.include(handle.on_failure(..));   // fine: the attribute said nothing about failures
```

The methods come from the `SubscriberSettings` trait, which every generated definition implements;
import it (or the
[prelude](https://docs.rs/ruststream/latest/ruststream/prelude/index.html)) to reach them.

Broker-specific settings arrive the same way, in the broker's own vocabulary. Core cannot know that
a subscription has a JetStream stream or a durable consumer name, so it exposes one hook - a
transform over the source it is building - and a broker crate layers its own trait on top, bound to
its own source type; see
[Broker authors](../broker-authors/index.md#subscription-sources). The order in a chain follows from
what each step does: the name comes first because it constructs the source, the broker settings then
transform it, and the buffer below wraps it last.

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

A handler that takes a slice consumes whole batches: it runs once per batch the broker delivers -
one database round-trip, one bulk API call. The shape is read off the signature, so nothing in the
attribute says it.

=== "Macros"

    ```rust
    --8<-- "examples/subscribers.rs:batch"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/subscribers.rs:batch"
    ```

Mount it with `include`, like any other form - the definition carries the batch shape:

=== "Macros"

    ```rust
    --8<-- "examples/subscribers.rs:batch_mount"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/subscribers.rs:batch_mount"
    ```

The signature says the handler wants several messages at once; whether they arrive that way is a
property of the broker, so it is settled where the definition is mounted. The subscription's
subscriber must implement the `BatchSubscriber` capability: brokers whose clients batch natively
(Kafka poll, JetStream pull consumers) expose it directly, and batch sizing lives in their
subscription options; the in-memory broker batches natively too. Where the subscription does not
batch, the compiler asks for the framework's buffer and the mount supplies it, closing a batch by
size or by a deadline after its first delivery:

=== "Macros"

    ```rust
    --8<-- "examples/subscribers.rs:batch_buffered"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/subscribers.rs:batch_buffered"
    ```

Batches come either from the broker (configured by the broker's own settings) or from this wrap;
the setting is named after the adapter to keep the two apart. The wrap changes the subscription
type, so it goes last - broker settings bound to the unwrapped type stop applying past it.

The semantics differ from single-message handlers in a few ways:

- Elements that fail to decode are nacked individually (per the decode-failure policy) and never
  reach the handler; the rest arrive as one slice.
- The returned value settles the batch. A single `HandlerOutcome` (or `()` / `Result<_, E>`)
  settles **every** message uniformly: `ack()` acks them all, `retry()` requeues them all.
- Per-message headers are not accessible in the `&[T]` form, and the context starts with empty
  headers.
- The context is one per page, and the broker fields on it are the *subscription-scoped* ones: a
  page body names the broker's batch context type (`ctx: &mut Context<'_, MemoryBatchContext>`
  for the in-memory broker) and reads its keys with `ctx.context(..)`. A broker with nothing
  subscription-scoped to offer leaves pages on the `()` default.
- Per-delivery data has no place there, because a page spans many deliveries: a position or a
  header rides the elements instead, read off a `&[Message<H, T>]` page element by element. The
  two are separate types, so a page body asking for the broker's per-delivery context does not
  compile.
- App-global and router middleware wrap per-message handlers and do not apply to batch
  registrations.

### Selective acknowledgement

A common case is partial readiness: some messages of the batch are processed, others are not
ready yet and should be redelivered without retrying the ones that succeeded. Return
`Vec<HandlerOutcome>` to settle element `i` of the slice with outcome `i`:

=== "Macros"

    ```rust
    --8<-- "examples/subscribers.rs:batch_selective"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/subscribers.rs:batch_selective"
    ```

Broker semantics are exactly those of per-message `nack(requeue = true)`: brokers with
per-message redelivery honour selective retry natively; a positional broker degrades the same
way it does for a single-message nack (the crate of that broker documents it). Returning a
vector whose length does not match the batch is a bug in the handler: the unmatched remainder is
retried and the mismatch is logged.

## Seeking

Replaying a stream after fixing a handler bug, reprocessing from a known point, skipping forward
past a poison region: each moves a live subscription to another position without dropping it.
Brokers whose transport is a replayable log (Kafka, Redis streams, the in-memory broker's publish
log) implement the `Seekable` capability and publish seek keys over their per-delivery context.
Brokers without a replayable log ship no such keys, and the mount below fails to compile against
them instead of failing at runtime.

A handler repositions its own subscription through the broker's context keys: the delivery's
context carries the position and a live seek handle (resolved once, when the subscription opens),
and the handler reads them by key - the `Ctx` extractor on the attribute path, `ctx.context(..)`
against the broker's context type on the manual one. Nothing is attached at the include site:

=== "Macros"

    ```rust
    --8<-- "examples/seek.rs:handler"
    ```

    ```rust
    --8<-- "examples/seek.rs:mount"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/seek.rs:handler"
    ```

    ```rust
    --8<-- "examples/manual/seek.rs:mount"
    ```

The clause forces the position on every startup; without it the subscription opens at the
broker's default. A conditional default - apply only when the broker holds no stored
cursor for the group (Kafka's offset reset, a JetStream deliver policy) - stays on the
broker's own subscription descriptor, which expresses it natively.

A page body repositions its subscription the same way, one level up: the seek handle is
subscription-scoped, so it rides the broker's batch context, while the target - a position the
producer asked the consumer to resume from - rides the page's own elements.

What one seek covers differs per broker - repositioning a consumer instance (Kafka) moves that
instance only, repositioning a shared group cursor (Redis streams) moves the whole group - and a
reposition invalidates any ack bookkeeping the broker keeps for the subscription; the broker
crate documents both. Broker authors prove the contract with the
[`capabilities::seeking` conformance suite](../broker-authors/conformance.md#capability-suites).

## Raw subscribers

Sometimes the payload is not a serialized value at all: a binary frame, a foreign wire format
you parse yourself. A codec would only stand in the way, so the payload type takes that stage
out of the path:

```text
decoded:  broker -> bytes -> codec -> &Order     -> handler
raw:      broker -> bytes ->          &Frame<'_> -> handler
```

Which lane a payload rides is chosen by its type, and the trait names are the mnemonic:
`Deserialize`/`Serialize` - the framework's codec does it; `Deserialized`/`Serialized` - the
type already did. A `Deserialized` type is a named `&[u8]` - one field, nothing copied:
`#[derive(Deserialized)]` on a newtype over `&'a [u8]` is the whole declaration, and a
`&Frame<'_>` parameter is what puts a handler on the lane. The bytes arrive exactly as the
broker handed them over, borrowed from its buffer.

=== "Macros"

    ```rust
    --8<-- "tests/raw_subscriber.rs:raw"
    ```

=== "Manual"

    ```rust
    --8<-- "tests/manual_raw_subscriber.rs:raw"
    ```

A bare `&[u8]` parameter does not compile: a payload always arrives behind a named type of the
service's own, and the compile error names the derive as the fix. The Manual tab shows the pair
of impls the derive writes - the construction, and the spelling that routes the type onto the
lane.

The form rule does not change with the lane: `&T` is one message, `&[T]` a page. A page of
frames is therefore `&[Frame<'_>]`, and the page spelling comes with the derive - a page body
asks for no second impl. Its elements borrow the batch's own messages for the duration of the
call, so nothing is copied there either, and the settlement rules are the batch path's.

=== "Macros"

    ```rust
    --8<-- "examples/subscribers.rs:raw_batch"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/subscribers.rs:raw_batch"
    ```

A construction that validates - a flatbuffers root, a capnp reader, a length check - reports a
bad payload by returning `Err` from `from_payload`, and `on_failure(decode = ..)` settles that
delivery: the same rung a codec decode failure and a typed `Headers` violation land on.
Everything else composes as usual: extractors, `&mut Context`, `workers(..)`,
`on_failure(panic = ..)` and the injected `Out` parameters work unchanged on the
single-delivery shape (a page of frames takes no `Out` parameter), and the subscriber mounts
with the same `include` as every other definition. A scope codec does not apply to it - the
lane never calls one - which also makes this the subscriber form that works with no codec
feature enabled at all. For a custom serialization format you want *typed* handlers for,
implement [`Codec`](codecs.md) instead and keep the typed path.

A handler on this lane replies through the same `publish("dest")` clause every reply form uses,
and the reply *type* picks the wire by the same mnemonic: a `serde::Serialize` reply encodes
through the reply codec, a `#[derive(Serialized)]` newtype carries its own bytes and leaves
byte-for-byte, exactly as the handler returned it. Return the reply directly, or as
`Result<Export, HandlerOutcome>` for the same explicit ack control the encoded form has.

The publisher comes from the include site: a `Serialized` reply attaches a plain publish policy
(`b.include(relay).publisher(Publish)`), an encoded one wraps the policy in
`TypedPublisher::new(..)`, and with no call at all the broker's default publish policy carries
the reply. A failed reply publish nacks the delivery with requeue, exactly as on the encoded
path:

=== "Macros"

    ```rust
    --8<-- "tests/raw_subscriber.rs:raw_reply"
    ```

=== "Manual"

    ```rust
    --8<-- "tests/manual_raw_subscriber.rs:raw_reply"
    ```

Neither side constrains the other: the input type picks the decode, the reply type picks the
encode, and the two diagonals compose freely. A decoded input with a `Serialized` reply is the
gateway shape - structured messages in, a wire format the handler produced itself out - where
the input still decodes with the scope codec and keeps its decode failure policy:

=== "Macros"

    ```rust
    --8<-- "tests/raw_subscriber.rs:raw_reply_typed"
    ```

=== "Manual"

    ```rust
    --8<-- "tests/manual_raw_subscriber.rs:raw_reply_typed"
    ```

The other diagonal reads the same way: a `Frame<'_>` input with a `Serialize` reply encodes the
answer through the reply codec while the input never touches one. Two things do not follow the
type, though. A `Vec<u8>` reply is not a byte reply - it is an ordinary `Serialize` value, so it
goes out encoded, and a payload that must leave untouched needs the newtype. And a page reply
always publishes through the reply codec - the `Serialized` wire applies to single replies.

## Worker pools

The dispatch loop is sequential per subscriber: one delivery is handled and settled before the
next is pulled, so one slow handler stalls the whole subscription. A `workers(n)` clause
processes up to `n` deliveries of this subscriber concurrently, each in its own task on the
multi-thread runtime:

=== "Macros"

    ```rust
    --8<-- "examples/subscribers.rs:workers"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/subscribers.rs:workers"
    ```

Back-pressure holds: the stream is not polled while `n` deliveries are in flight, which plays
well with broker-side limits like JetStream `max_ack_pending`. **Global processing order is lost
by design** - if any ordering matters, either stay sequential or use keyed lanes:

=== "Macros"

    ```rust
    --8<-- "examples/subscribers.rs:workers_by_key"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/subscribers.rs:workers_by_key"
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
| `workers(n)` × a batch handler | The pool holds up to `n` **batches** in flight. `by_key` does not apply to batch forms: lanes order single messages per key, and the macro rejects the combination at compile time. |
| `retry()` / `retry_after` × `workers(n)` | Retried deliveries re-enter the pool and complete like any other delivery. |
| `retry()` / `retry_after` × `workers(n, by_key)` | Retries complete, but per-key ordering across a retry is **not** promised: a requeued message rejoins the stream from the back. If a key's messages must stay ordered even through failures, the handler has to absorb the failure instead of nacking. |
| `.transactional()` × `workers(n)` | One transaction per batch, exactly as in the sequential loop. Concurrent batches run concurrent, independent transactions; each stays atomic (commit-then-ack per batch). |
| `Buffered` × `workers(n)` | Batches still close by `max_size` / `max_wait` only; the pool bounds how many closed batches are processed at once and never affects batch boundaries. |
| `publish(..)` × `workers(n)` | Replies are produced concurrently, so reply order across deliveries is not promised. A failed reply publish retries only its own delivery. |
| middleware × a batch handler | App-global and router layers wrap per-message handlers and do not apply to batch registrations (a per-message layer cannot wrap a whole-batch handler). |

## Macro or manual

`#[subscriber]` is sugar over a generic API. The macro generates a typed handler and its metadata;
you can write the same registration by hand as a named type whose `impl Handle` carries the body,
bound to its source by `subscriber(source, body)` and sealed with `.build()`. Both forms below
register the same handler.

=== "Macro"

    ```rust
    use ruststream::subscriber;

    --8<-- "examples/subscribers.rs:contract"

    // inside with_broker(...):
    b.include(handle);
    ```

=== "Manual"

    ```rust
    use ruststream::prelude::*;

    // inside with_broker(...):
    --8<-- "examples/subscribers.rs:manual"
    ```

The manual body returns a `Result`: the `Ok` side carries what the handler produces (the reply, or
nothing) and the `Err` side carries the settlement, so `Ok(())` acks and
`Err(HandlerOutcome::retry())` requeues; a page body settles element-wise with
`Err(Vec<HandlerOutcome>)`. Between `subscriber(..)` and `.build()`, the chain takes the same
settings the attribute's clauses would (`.name`, `.workers`, `.on_failure`, `.buffered`) plus the
documentation controls: a registration is documented by default under the `asyncapi` feature,
`.describe(..)` sets its description, and `.undocumented()` opts it out (see
[AsyncAPI](asyncapi.md#payload-schemas)).

Reach for the manual form when a handler needs state the macro cannot express (a struct handler
with fields), or when the `macros` feature is off. Otherwise the attribute is less to maintain.

## Publishers

A handler that produces a reply is a publisher. See [Publishing and replies](publishing.md).
