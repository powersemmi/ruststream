# Publishing and replies

There are two ways to publish: return a reply from a handler, or publish explicitly through a
publisher injected into the handler with the `Out` parameter. Either way the handler never
sees an unconnected publisher: registrations carry publish *policies* (pure declarations), and
the runtime pairs them with the connected broker at startup.

## Replying from a handler

Name a reply destination with `publish(..)` and return the reply value. The runtime encodes it and
sends it:

```rust
use ruststream::subscriber;

--8<-- "examples/publishing.rs:reply"
```

Mount it with plain `include`. With nothing else said, the reply goes out through the broker's
default publish policy under the default codec; to name the reply codec or add transforms, chain
`.publisher(..)` with a [`TypedPublisher`] stack over the broker's publish policy
(`TypedPublisher::new` uses the default codec; name one with `TypedPublisher::with_codec`). The
stack is a declaration: the runtime pairs it with the connected broker at startup.

```rust
--8<-- "examples/publishing.rs:reply_mount"
```

Decoding of the incoming request follows the scope (the scope codec set with
`with_broker_codec`, else the default codec); the reply codec travels on the attached stack. See
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

To publish to a destination other than a single reply (a computed destination, fan-out, side
effects), take the publisher as a handler parameter with `Out`: the pattern
`Out(out): Out<impl Publisher>` binds `out` to a live publisher inside the body. The signature
names only the capability the handler needs, never a broker publisher type: the concrete type
is inferred from the policy attached where the handler is included, and the runtime pairs it
after the broker connects. The handler cannot observe a "not connected" publisher, the state
stays free of connection-bound values, and the same handler mounts unchanged on a production
broker and on its in-process test transport - no `#[cfg(test)]` type aliases.

```rust
use ruststream::runtime::Out;

--8<-- "examples/publishing.rs:forward"
```

The include site names the source; for the scope's own broker it is the publish policy:

```rust
--8<-- "examples/publishing.rs:forward_mount"
```

An `Out` slot left unbound is a compile error, not a runtime one: an injected publisher has no
broker-side default, so the registration does not build until every slot has a policy.

### Named slots

A handler that takes several publishers names a **slot marker** per parameter: a unit struct
deriving `OutSlot`, written as the second type argument (`Out<impl Publisher, Primary>`). The
include site binds each marker with `.out(marker, policy)` and commits the registration with a
terminal `.mount()`. The calls bind by marker, so their order does not matter; binding the same
slot twice (or a marker the handler does not declare) fails to compile, and `.mount()` exists
only once every slot is bound - a forgotten binding is a compile error whose attachment type
names the slot (`MissingSlot<Audit>`). A single unnamed `Out<impl Publisher>` parameter binds
the implicit `DefaultSlot` through the plain `.publisher(policy)` call, which binds and commits
in one step, so the common case stays noise-free.

```rust
use ruststream::OutSlot;

--8<-- "examples/publishing.rs:slots"
```

```rust
--8<-- "examples/publishing.rs:slots_mount"
```

The capability in the bound can be refined: `Out<impl OwnedTransactions, Ledger>` compiles only
against a policy whose live publisher supports owned transactions, checked at the include site
with a diagnostic naming the missing capability. The slot marker is also the identity the
[test harness](testing.md#asserting-on-out-slots) records publishes against.

A named marker can go further and declare a **publish dictionary** - which message types it
publishes and to which channels. The `Out` parameter's optional third position then declares
what this handler sends (`Out<impl Publisher, Marker, (A, B)>`, a single type, or a
`#[derive(OutMessages)]` set enum), and the handler publishes by value with compile-checked
destinations and header contracts, feeding the `send` operations of the generated AsyncAPI
document. See [typed headers](headers.md).

The parameter composes with every subscriber form: next to a `Seek` parameter, on a byte-input
handler, and on batch handlers (`b.include(f).publisher(..)` - the whole page in,
per-element destinations out). On the reply forms - `publish(..)` / `publish_raw(..)` and
their batch counterpart - `.publisher(..)` stays the reply's own attachment and the injected
publisher attaches with `.out(marker, ..)` plus the terminal `.mount()` (`DefaultSlot` for a
single unnamed slot), so a gateway can answer on a fixed destination while fanning side copies
out through the injection:

```rust
--8<-- "examples/publishing.rs:publish_out"
```

```rust
--8<-- "examples/publishing.rs:publish_out_mount"
```

### Publishing to a different broker

When the handler consumes one broker and publishes to another (consume Kafka, forward to Redis),
wrap the target broker with `.bindable()` and mint a **bound token** before registration: the
token carries the instance identity a foreign scope cannot provide, and because tokens exist
before any `with_broker` runs, registration order does not matter - a bidirectional bridge binds
both directions up front. The token is then the source at the include site (shown here with two
in-memory brokers, the shape is the same for any pair):

```rust
--8<-- "tests/out_injection.rs:cross_broker"
```

A token shares a slot with the `Bindable` wrapper it was minted from, and registering that same
wrapper (`with_broker(bindable, ..)`) is what lets startup fill the slot with the connected
broker - so pairing needs no lookup and cannot pick a wrong instance, and a token whose broker
never registers fails fast at pairing with a clear error. The same shape works for reply
publishing (`.publisher(token)` on a `publish("dest")` handler) and for the batch forms. Outside
a registration, a token pairs itself once startup
connected its broker: `running.publisher(token)` hands a sibling task its live publisher - see
[Running beside another server](http.md). For the first publish at startup, no token is needed
at all: the scope-level `b.after_startup(policy, hook)` runs the hook with an already-paired
publisher once subscriptions are open (see [Lifespan](lifespan.md#lifecycle-hooks)); the
publishing example's seeding rides it.

## The publish pipeline

Two kinds of transform run before a message leaves the process, and they compose:

- **Static `PublishTransform`** on a `TypedPublisher`, added with `.transform(..)`. Zero-cost,
  per-destination transforms (an envelope, a fixed content type, or stamping the delivery's trace /
  correlation id onto the reply). They run first, closest to the value.
- **Static `PublishLayer`** on the application, added with `.publish_layer(..)`. Cross-cutting
  concerns (publish metrics, a dead-letter wrapper) applied to every published message, around the
  send so they can observe its result. The chain composes into a concrete type (no `dyn` dispatch at
  all), so it becomes part of the app's type. A builder usually returns `impl App` and never spells
  it; name the concrete `RustStream<L, St, PublishStack<MyMiddleware, PublishIdentity>>` and the
  pipeline shows up there, while an app with no `publish_layer` keeps the default `PublishIdentity`.
  Each middleware must be `Clone` (the pipeline is cloned into each publishing handler), and the last
  one added runs outermost. The default (no middleware) is a direct send. For a middleware set decided
  at runtime, wrap it in a `PublishDynStack` (the publish counterpart of `DynStack`) and add that.

A static `PublishTransform` implements `apply(&mut Outgoing<'_>, &PublishContext<'_, C>)`; the
`PublishContext` is a read-only view of the delivery that produced the reply (its channel, the
incoming headers, and the broker's typed per-delivery context by `Field` key), so a transform can
carry a value from the incoming message onto the reply:

```rust
--8<-- "examples/publishing.rs:static_transform"
```

A batch handler's replies skip the per-message `.transform(..)` stack; add a transform there with
`.batch_transform(..)`, reusing a per-message `PublishTransform` via `for_batch(transform)`.

A `PublishLayer` implements an around/next signature, so it can short-circuit, retry, or
observe (reserve "dynamic" for `PublishDynLayer` inside a `PublishDynStack`):

```rust
--8<-- "examples/publishing.rs:app_layer"
```

Both levels compose on the application:

```rust
--8<-- "examples/publishing.rs:pipeline"
```

The pipeline runs on the reply path (the `publish(..)` form). An injected `Out` publisher is the
attached policy's live form, used directly, so compose any per-publisher transforms into the
policy at the include site with `TypedPublisher::transform`. The full program is
[`examples/publishing.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/publishing.rs).

## Batch replies and transactions

A `#[subscriber("in", publish("out"))]` handler taking `&[T]` consumes a whole decoded batch and
returns the replies for it - the consume-transform-produce pattern. `Ok(replies)` publishes every reply to
the reply name and acks the batch; `Err(result)` publishes nothing and settles the whole batch
with `result` (all-or-nothing: selective per-element outcomes do not compose with a
transaction):

```rust
--8<-- "examples/publishing.rs:batch_publishing"
```

Mount it with `include`, chaining the reply wiring with `.publisher(..)`:

```rust
--8<-- "examples/publishing.rs:batch_publishing_mount"
```

With a plain `TypedPublisher`, each reply publishes independently; a mid-batch failure retries
the whole batch, so the earlier replies may be published again on redelivery (at-least-once).
Calling `.transactional()` on the `TypedPublisher` switches the wiring to one broker transaction
per batch: the runtime begins a transaction, publishes every reply, commits, and only then acks
the incoming batch; any failure aborts, so replies are never half-visible. The transactional
requirement is enforced where the wiring is consumed: mounting it needs the policy's live
publisher to implement the `TransactionalPublisher` capability, so a broker without transactions
still fails to compile. The single-message reply forms keep taking a plain `TypedPublisher`
stack: a one-message transaction adds broker round-trips for no atomicity gain.

## Manual transactions

Outside the batch-reply path, drive a transaction by hand: `begin()` on the transactional wiring
opens a `TransactionScope` that owns the transaction. Publishes go through the scope, and
`commit()` / `abort()` consume it - so a commit without a begin, a second commit, or a publish
after settling are compile errors, not runtime surprises:

```rust
--8<-- "examples/publishing.rs:manual_transaction"
```

The scope encodes values with the publisher's codec and sends them directly: per-publisher
transforms and the app-wide `publish_layer` middleware belong to the dispatch path (they read the
originating delivery) and do not run here. Dropping an unsettled scope logs a warning and leaves
the broker transaction open on that handle - always settle explicitly.

The scope is the borrowed transaction kind: it borrows the handle's single broker-side
transaction, so one scope per handle is open at a time. Brokers whose transactions are client
buffers rather than producer state also implement the owned kind, `OwnedTransactions`: every
`transaction()` call opens an independent transaction whose buffer lives in the returned
`Transaction` value, so any number can be open concurrently on one handle and settling one never
touches another. `publish` buffers into the value and `commit()` / `abort()` consume it - the
same settle-by-consuming discipline as the scope - while dropping one merely discards its buffer
(with a warning) instead of leaving a broker transaction open. Kafka-like brokers, whose client
holds exactly one transaction per producer, implement only the borrowed kind.

The owned kind has typed sugar too: on a `TypedPublisher` whose publisher implements
`OwnedTransactions`, `transaction()` opens a `TypedTransaction` that owns the broker transaction
and encodes with the publisher's codec - `let mut txn = typed.transaction().await?;`, then
`txn.publish("orders", &value).await?;` and `txn.commit().await?;`. Where `.transactional()` +
`begin()` gives the borrowed scope (one per handle), any number of `TypedTransaction`s can be
open on one `TypedPublisher` at a time.

## Batch publishing

There is no direct batch-publish API on `Publisher`. For most brokers (NATS, Kafka) the client
already coalesces writes, so a per-message `publish` loop achieves the same throughput. Where a
broker has a genuine pipeline primitive (Redis), the broker crate exposes it as a broker-specific
capability.
