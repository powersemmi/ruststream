# Subscribers

A subscriber binds a handler to one subscription. Declare one with the `#[subscriber]` macro.
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

The macro turns the function into a subscriber definition of the same name (here `handle`) that
implements the mounting contract. You pass that definition to `include`.

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

The macro resolves the context type itself, so the `Context` name needs no import while it appears
only in `#[subscriber]` signatures. The rest - the headers working copy, state access, broker
per-delivery fields - is covered in [Context and state](context.md).

### Extractor parameters

Any further parameter, after the message and the optional `&mut Context`, is an **extractor**. The
runtime resolves it from the delivery before the body runs. If it cannot be resolved, the delivery
settles and the body does not run. Four kinds can appear:

- `State<T>` - a field of the application state (derive `FromRef` on the state type).
- `Ctx<K>` - a broker per-delivery field, read by its key.
- `Headers<T>` - the delivery headers parsed into a typed contract; a violation settles the
  delivery by the `on_failure(decode = ..)` policy (see [typed headers](headers.md)).
- any type implementing `FromContext` - a custom extractor (an auth guard, a request-scoped
  resolver).

The mechanics are described in
[Injecting dependencies](context.md#injecting-dependencies-extractor-parameters) and
[Context fields as parameters](context.md#context-fields-as-parameters).

One more parameter shape is not an extractor but an **injection**. `Out(out): Out<impl
Publisher>` receives a live publisher constructed by the policy you name at the `include` site
(`b.include(handler).out(marker, policy).build()`). The concrete publisher type never appears in
the signature.

An optional third position, `Out<impl Publisher, Marker, (A, B)>`, declares the message set the
handler publishes and enables the dictionary-driven typed publish path
([typed headers](headers.md)). See
[Publishing from inside a handler](publishing.md#publishing-from-inside-a-handler).

### Acking

You can return anything that converts into a
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

On the message itself, `ack` consumes `self`, so the type system prevents acking twice.

### Post-settle continuations

`HandlerOutcome::ack().and_after(fut)` attaches a continuation to the returned outcome - a
non-critical notification, slow follow-up work, a cache warm-up. Any outcome works,
`drop().and_after(..)` included:

=== "Macros"

    ```rust
    --8<-- "examples/post_settle.rs:single"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/post_settle.rs:single"
    ```

The continuation follows the shared post-settle rules: at-most-once, it runs only after the ack or
nack settles, and a graceful shutdown drains it. See
[Post-settle hooks](context.md#post-settle-hooks).

In a batch each element settles individually, so you attach the continuation per element:

=== "Macros"

    ```rust
    --8<-- "examples/post_settle.rs:batch"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/post_settle.rs:batch"
    ```

Batch *publishing* (a batch handler with `publish(..)`) settles the whole batch at once, under one
transaction, so per-element `and_after` does not compose with it.

### Delayed redelivery

`retry_after` covers the not-ready-yet case: a dependency has not arrived, an external service
limits the request rate. An immediate redelivery would make no progress:

=== "Macros"

    ```rust
    --8<-- "examples/retry.rs:retry_after"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/retry.rs:retry_after"
    ```

The runtime honours the delay as follows:

- A broker with native delayed redelivery receives the delay directly. The in-memory broker
  redelivers on a timer; a NATS JetStream broker could send a `NAK` with a delay.
- A broker without native support gets a **deferred re-publish**: after `delay` the runtime
  publishes the message back to where it came from, and drops the original. In the new copy the
  framework retry-count header
  ([`RETRY_COUNT_HEADER`](https://docs.rs/ruststream/latest/ruststream/runtime/constant.RETRY_COUNT_HEADER.html))
  is incremented by one, and a handler can read it to cap redeliveries.

  You can turn it on per scope with
  [`BrokerScope::retry_via(publisher)`](https://docs.rs/ruststream/latest/ruststream/runtime/struct.BrokerScope.html#method.retry_via);
  the publisher must target the same broker. Without a publisher the delay is dropped and the
  message is requeued immediately. The deferred re-publish is **at-most-once** over the delay
  window: if the process exits before the timer fires, the copy is lost.

  A transport that cannot settle at all (MQTT at QoS 0, ZeroMQ, Redis pub/sub) takes the same
  path: there is nothing to drop, so the deferred copy is the whole retry. The other case is a
  settlement the broker rejects: the message stays with the broker and it redelivers on its own, so
  the runtime returns the error and adds no copy on top.

The `batch_retry_after` form composes with
[selective batch outcomes](#selective-acknowledgement): a `Vec<HandlerOutcome>` sets the delays
per element, so entries that are not ready wait without holding up the rest of the batch:

=== "Macros"

    ```rust
    --8<-- "examples/retry.rs:batch_retry_after"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/retry.rs:batch_retry_after"
    ```

## Choosing the subscription source

The attribute always fixes the *kind* of subscription: a subject, a JetStream consumer, a Redis
stream, a pub/sub channel and a list are different types. What you can leave out is the *value*:
the mount site then supplies it. There are four forms, shortest first:

| Form | The kind | The value |
|---|---|---|
| `#[subscriber]` | the by-name source | from the mount site |
| `#[subscriber(RedisStream)]` | named here | from the mount site |
| `#[subscriber("orders")]` | the by-name source | fixed here |
| `#[subscriber(RedisStream::new("orders").group("w"))]` | named here | fixed here |

### By name

`#[subscriber("orders")]` subscribes by name. The form works with any broker that implements the
`Subscribe` capability, and every broker crate in the family does. The broker maps the name to the
subscription kind it treats as its default. Whatever else that kind needs, you set once on the
broker.

`#[subscriber]` is the same source without the value: a name the service learns only at startup, a
subject built from a shard number, a topic read from configuration:

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

Some kinds need more than a name: a Pulsar source takes a topic *and* a subscription name. Such a
kind does not implement `FromName`, so the forms that take their name from the mount site do not
compile for it. Write those kinds out in full.

### Broker-specific descriptors

When a subscription needs broker-specific options (a consumer group, a durable name, a delivery
policy), the broker crate exposes a descriptor type. Call its constructor directly in the attribute:

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

The source can also be a chain of settings on that constructor:

<!-- inline-rust: illustrative builder-chain source; the concrete options type lives in a broker crate, so there is no in-repo compiled home -->
```rust
#[subscriber(StreamOptions::new("orders").durable("audit"))]
async fn handle(order: &Order) -> HandlerOutcome {
    HandlerOutcome::ack()
}
```

To name the source type, the macro follows the chain down to the base `Type::new(..)`, so every
method in the chain must return `Self`. Free functions are rejected: their type is not visible to
the macro.

A source built this way is rebuilt for each mount, so a broker's descriptor type is `Clone`. One
definition can mount on two brokers.

## Settings at the mount site

Name, worker policy, failure policies and the start position are values. You can give each in the
attribute, at the mount site, or partly in each. The attribute expands into exactly the calls you
would write yourself:

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

The methods are declared on the `SubscriberSettings` trait, which every generated definition
implements. Import it, or the
[prelude](https://docs.rs/ruststream/latest/ruststream/prelude/index.html), to reach them.

You set broker-specific settings the same way, in the broker's own vocabulary: a JetStream stream,
a durable consumer name. Core does not know that vocabulary, so it offers one extension point - a
transform over the source it is building. A broker crate adds its own trait on top, bound to its
own source type; see [Broker authors](../broker-authors/index.md#subscription-sources).

The order in a chain follows from what each step does: the name comes first because it constructs
the source, the broker settings transform it, and the buffer described below wraps the result last.

## Mounting handlers

Inside `with_broker`, mount a definition with `include`:

<!-- inline-rust: minimal include mount fragment with placeholder info/broker; the full compiled program is examples/subscribers.rs (its app is pulled in via other anchors on this page) -->
```rust
RustStream::new(info).with_broker(broker, |b| {
    b.include(handle);
});
```

`include` decodes the payload with the codec from the most specific level you set: per handler or
per scope. If you set none, `include` uses the feature-selected default codec. See
[where the codec comes from](codecs.md#where-the-decode-codec-comes-from).

To group handlers by module and mount them all at once, collect them into a `Router`; see
[Routing](routing.md).

## Batch subscribers

A handler that takes a slice receives a whole batch: it runs once per batch the broker delivers.
One database round-trip, one bulk API call. The macro reads the batch shape off the signature, so
the attribute does not declare it.

=== "Macros"

    ```rust
    --8<-- "examples/subscribers.rs:batch"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/subscribers.rs:batch"
    ```

Mount it with `include`, like any other form. The batch shape is already declared in the
definition, and the mount site adds one number, the batch size:

=== "Macros"

    ```rust
    --8<-- "examples/subscribers.rs:batch_mount"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/subscribers.rs:batch_mount"
    ```

`batch(n)` is the framework's only batch parameter, and it is mandatory: a batch handler mounted
without it does not compile. The runtime passes the size to the broker, which builds its batches to
that size: `XREADGROUP COUNT`, a JetStream pull batch, a Kafka poll limit. The body sees exactly
the batch the broker delivered, never a slice of it. A batch is shorter than `n` when that is all
the broker had.

Everything else that shapes a batch (a block timeout, a consumer group, a prefetch window) is the
broker's own vocabulary. Those settings chain after the size, on the broker's subscription source:

<!-- inline-rust: the extra step belongs to a broker crate, which this repository cannot depend on -->
```rust
// on a Redis broker: the size is the core's word, `.block(..)` is the broker's
b.include(reconcile.name("orders").batch(nonzero!(6)).block(Duration::from_secs(5)));
```

You name the size the same way on every batch shape, including a batch that replies and one that
publishes through an `Out` slot. A single-message handler has no batch, so `batch(n)` on it does
not compile. How many deliveries such a handler takes at once is `workers(n)`.

Every broker offers batches. A broker whose client fetches batches natively implements the
`BatchSubscriber` capability directly: Kafka poll, JetStream pull consumers, Redis `XREADGROUP`,
the in-memory broker. A broker whose transport delivers one message at a time assembles the
batches on the client, through the core's `Buffered` adapter, inside its own crate. The mount site
does not show which of the two paths was taken. The broker-authors guide describes how a broker
does it under [Batches](../broker-authors/index.md#batches-batchsubscriber).

The semantics differ from those of a single-message handler in a few places:

- An element that fails to decode is nacked on its own, by the decode-failure policy, and never
  reaches the handler. The rest arrive as one slice.
- The returned value settles the whole batch. A single `HandlerOutcome` (or `()` / `Result<_, E>`)
  settles **every** message the same way: `ack()` acks them all, `retry()` requeues them all.
- Per-message headers are not available in the `&[T]` form, and the headers on the context are
  empty.
- The context is one per batch, and its broker fields are the ones scoped to the *whole
  subscription*. A batch body names the broker's batch context type
  (`ctx: &mut Context<'_, MemoryBatchContext>` for the in-memory broker) and reads its keys with
  `ctx.context(..)`. A broker with no subscription-scoped fields leaves batches on the `()`
  default.
- Per-delivery data is not on that context: a batch spans many deliveries, so a position or a
  header is stored in the elements themselves and read from a `&[Message<H, T>]` batch element by
  element. The delivery context and the batch context are separate types, so a batch body that
  asks for the per-delivery one does not compile.
- App-global and router middleware wrap per-message handlers and do not apply to batch
  registrations.

### Selective acknowledgement

Partial readiness is a common case: some messages of the batch are processed, others are not ready
yet. Only the ones that are not ready need redelivery. Return `Vec<HandlerOutcome>` to settle
element `i` of the slice with outcome `i`:

=== "Macros"

    ```rust
    --8<-- "examples/subscribers.rs:batch_selective"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/subscribers.rs:batch_selective"
    ```

On the broker side the semantics are those of a per-message `nack(requeue = true)`. A broker with
per-message redelivery supports selective retry natively; a positional broker degrades the same way
it does on a single-message nack, and its crate documents that. A vector whose length does not
match the batch is a bug in the handler: the unmatched remainder is retried and the mismatch is
logged.

## Seeking

Replaying a stream after fixing a handler bug, reprocessing from a known point, skipping forward
past a poison region: each of these names a position in the stream. The position is where a
subscription opens, or where a running subscription moves to without being interrupted.

A broker over a replayable log (Kafka, Redis streams, the in-memory broker's publish log)
implements the `Seekable` capability, owns the position type, and offers seek keys on its
per-delivery context. On a broker without a replayable log, the mounts below are rejected at
compile time rather than at run time.

The in-memory broker keeps a log only when you ask for one, so a service that seeks on it names how
much history to keep:

```rust
--8<-- "examples/seek.rs:retaining"
```

### Opening at a chosen position

A new subscription opens where the broker decides: at the tail for a plain consumer, at a stored
cursor for a durable one. The service never sees what was published before that point. An audit
trail needs the whole history, while a monitor must not work through a backlog at all.

Where a subscription opens is a property of the mount, not of the handler, so you name it there:
the `start_at(<position>)` clause on the attribute, or the `.start_at(..)` settings step:

=== "Macros"

    ```rust
    --8<-- "examples/seek.rs:start_at"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/seek.rs:start_at"
    ```

    ```rust
    --8<-- "examples/manual/seek.rs:start_at_mount"
    ```

The position is a value of the broker's own position type, so you can name exactly what that broker
is able to express. The in-memory log offers `MemoryPosition::start()` for the oldest message it
still keeps, `MemoryPosition::end()` for everything from the next publish onwards, and
`MemoryPosition::sequence(n)` for one log entry.

The clause sets the position on every startup. Without it the subscription opens at the broker's
default. A conditional default, applied only when the broker holds no stored cursor for the group
(Kafka's offset reset, a JetStream deliver policy), is set on the broker's own subscription
descriptor, which expresses it natively.

### Repositioning from a handler

A handler repositions its own subscription through the broker's context keys. The delivery context
holds the position and a live seek handle, which the broker creates once, when the subscription
opens. The handler reads them by key: the `Ctx` extractor on the attribute path, a
`ctx.context(..)` call against the broker's context type on the manual one. Nothing is attached at
the `include` site:

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

A batch body repositions its subscription the same way, one level up. The seek handle belongs to
the whole subscription, so it sits on the broker's batch context. The target position, the one the
publisher asks the consumer to resume from, is stored in the batch's own elements.

The scope of one seek differs per broker: repositioning a consumer instance (Kafka) moves that
instance only, repositioning a shared group cursor (Redis streams) moves the whole group. A
reposition also invalidates the ack bookkeeping the broker kept for that subscription. The broker
crate documents both. Broker authors prove the contract with the
[`capabilities::seeking` conformance suite](../broker-authors/conformance.md#capability-suites).

## Raw subscribers

Sometimes the payload is not a serialized value but a binary frame, or a foreign wire format you
parse yourself. The payload type takes the codec out of the path:

```text
decoded:  broker -> bytes -> codec -> &Order     -> handler
raw:      broker -> bytes ->          &Frame<'_> -> handler
```

The trait names are the mnemonic for the path: `Deserialize`/`Serialize` means the framework's
codec does the work, `Deserialized`/`Serialized` means the type did it itself.

A `Deserialized` type is a named `&[u8]`: one field, nothing copied. The whole declaration is
`#[derive(Deserialized)]` on a newtype over `&'a [u8]`, and a `&Frame<'_>` parameter puts the
handler on the raw path. The bytes arrive exactly as the broker handed them over, borrowed from
its buffer.

=== "Macros"

    ```rust
    --8<-- "tests/raw_subscriber.rs:raw"
    ```

=== "Manual"

    ```rust
    --8<-- "tests/manual_raw_subscriber.rs:raw"
    ```

A bare `&[u8]` parameter does not compile: a payload always arrives in a named type of the
service's own, and the compile error names the derive to add. The Manual tab shows the pair of
impls the derive writes: the construction from bytes, and the declaration that puts the type on
the raw path.

The form rule does not change with the path: `&T` is one message, `&[T]` is a batch. A batch of
frames is `&[Frame<'_>]`, and the same derive writes the batch declaration, so a batch body needs
no second impl. Its elements borrow the batch's own messages for the duration of the call, so
nothing is copied here either. Settlement follows the batch rules.

=== "Macros"

    ```rust
    --8<-- "examples/subscribers.rs:raw_batch"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/subscribers.rs:raw_batch"
    ```

A construction that validates the payload (a flatbuffers root, a capnp reader, a length check)
returns `Err` from `from_payload`. The `on_failure(decode = ..)` policy then settles that delivery,
the same policy that settles a codec decode failure and a typed `Headers` violation.

Everything else composes as usual. Extractors, `&mut Context`, `workers(..)`,
`on_failure(panic = ..)` and the injected `Out` parameters work unchanged on the single-delivery
shape; a batch of frames takes no `Out` parameter. Such a subscriber mounts with the same
`include` as every other definition.

A scope codec does not apply here, because this path never calls a codec. The raw form is
therefore the one subscriber form that works with no codec feature enabled at all. For a
serialization format of your own that you want *typed* handlers for, implement
[`Codec`](codecs.md) and stay on the typed path.

A handler on this path replies through the same `publish` clause every reply form uses.
The reply *type* picks the wire form, by the same mnemonic: a `serde::Serialize` reply is encoded
by the reply codec, and a `#[derive(Serialized)]` reply produces its own bytes, published exactly
as the handler returned them. You can return the reply directly, or as
`Result<Export, HandlerOutcome>` for the same explicit ack control the encoded form has.

The policy you name at the `include` site constructs the publisher, and both wire forms name that
policy the same way: `b.include(relay).out(Reply, Publish)`. With no `.out(..)` call, the broker's
default publish policy constructs it.

The chains diverge after that: an encoded reply takes `.codec(..)`, `.transform(..)` and
`.transactional()`, while a `Serialized` reply's bytes are published untouched, so those steps do
not exist on that path. A failed reply publish nacks the delivery with requeue, exactly as on the
encoded path:

=== "Macros"

    ```rust
    --8<-- "tests/raw_subscriber.rs:raw_reply"
    ```

=== "Manual"

    ```rust
    --8<-- "tests/manual_raw_subscriber.rs:raw_reply"
    ```

Neither side constrains the other: the input type picks the decoding, the reply type picks the
encoding, and they combine freely. A decoded input with a `Serialized` reply is the gateway shape:
the service takes structured messages in and returns a wire format the handler assembled itself.
The input still decodes with the scope codec and keeps its own decode failure policy:

=== "Macros"

    ```rust
    --8<-- "tests/raw_subscriber.rs:raw_reply_typed"
    ```

=== "Manual"

    ```rust
    --8<-- "tests/manual_raw_subscriber.rs:raw_reply_typed"
    ```

The other combination reads the same way: a `Serialize` reply is encoded by the reply codec, while
a `Frame<'_>` input never touches one.

Two cases fall outside this rule. A `Vec<u8>` reply does not count as raw: it is an ordinary
`Serialize` value and is published encoded, so a payload that must stay untouched needs the
newtype. A batch reply is always published through the reply codec: the `Serialized` wire form
works for single replies.

## Worker pools

The dispatch loop is sequential per subscriber: a delivery is handled and settled before the
subscriber pulls the next one. One slow handler therefore holds up the whole subscription. With a
`workers(n)` clause the subscriber handles up to `n` deliveries at once, each in its own task on
the multi-thread runtime:

=== "Macros"

    ```rust
    --8<-- "examples/subscribers.rs:workers"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/subscribers.rs:workers"
    ```

Back-pressure holds: the stream is not polled while `n` deliveries are in flight. That matches
broker-side limits like JetStream `max_ack_pending`. **Global processing order is lost, by
design.** If delivery order matters, stay sequential or split the work into lanes by key:

=== "Macros"

    ```rust
    --8<-- "examples/subscribers.rs:workers_by_key"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/subscribers.rs:workers_by_key"
    ```

`workers(n, by_key)` splits the work into `n` sequential lanes. A delivery goes to the lane its
partition key hashes to, so messages that share a key are never handled at the same time and never
reorder. This is the in-process analogue of Kafka partition semantics.

The key comes from `partition_key()` on the broker message: brokers whose messages implement the
`Partitioned` capability offer it, and the in-memory broker reads the `partition-key` header.
Messages without a key are spread over the lanes in turn. `by_key` applies to single-message
subscribers, while batch forms take a plain `workers(n)` pool of batches.

On shutdown the subscriber stops pulling new deliveries, and the workers in flight finish within
the app's `shutdown_timeout`.

## Composition rules

The subscriber features combine with each other. Below is the rule at each intersection; every one
of them is pinned by an integration test.

| Combination | Rule |
|---|---|
| `workers(n)` × a batch handler | The pool holds up to `n` **batches** in flight. `by_key` does not apply to batch forms: lanes order single messages per key, and the macro rejects the combination at compile time. |
| `retry()` / `retry_after` × `workers(n)` | Retried deliveries re-enter the pool and complete like any other delivery. |
| `retry()` / `retry_after` × `workers(n, by_key)` | Retries complete, but per-key ordering across a retry is **not** promised: a requeued message rejoins the stream from the back. If a key's messages must stay ordered even through failures, the handler has to absorb the failure instead of nacking. |
| `.transactional()` × `workers(n)` | One transaction per batch, exactly as in the sequential loop. Concurrent batches run concurrent, independent transactions; each stays atomic (commit-then-ack per batch). |
| a batch size × `workers(n)` | Batches still close at `batch(n)` or, where the broker batches on the client, at its deadline; the pool bounds how many closed batches are processed at once and never affects batch boundaries. |
| `publish(..)` × `workers(n)` | Replies are produced concurrently, so reply order across deliveries is not promised. A failed reply publish retries only its own delivery. |
| middleware × a batch handler | App-global and router layers wrap per-message handlers and do not apply to batch registrations (a per-message layer cannot wrap a whole-batch handler). |

## Macro or manual

`#[subscriber]` is sugar over a generic API. The macro generates a typed handler and its metadata.
You can write the same registration by hand: the handler body goes into the `impl Handle` of a
named type, `subscriber(source, body)` binds that type to its source, and `.build()` closes the
chain. Both forms below register the same handler.

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

The manual body returns a `Result`. `Ok` holds what the handler produced: the reply, or nothing.
`Err` holds the settlement, so `Ok(())` acks and `Err(HandlerOutcome::retry())` requeues the
message. A batch body settles element by element with `Err(Vec<HandlerOutcome>)`.

Between `subscriber(..)` and `.build()` the chain takes the same settings as the attribute's
clauses: `.name`, `.workers`, `.on_failure`, `.batch`. You control the documentation there too:
with the `asyncapi` feature a registration is documented by default, `.describe(..)` sets its
description, and `.undocumented()` leaves it out of the document (see
[AsyncAPI](asyncapi.md#payload-schemas)).

The manual form is what you need when a handler requires state the macro cannot express (a struct
handler with fields), or when the `macros` feature is off. Otherwise the attribute costs less to
maintain.

## Publishers

A handler that produces a reply is a publisher. See [Publishing and replies](publishing.md).
