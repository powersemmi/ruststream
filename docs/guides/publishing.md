# Publishing and replies

A handler publishes in one of two ways. Returning a reply is the shorter one, and the right default
when the handler answers on a single destination. Take a publisher as an `Out` parameter instead
when it sends somewhere else, or to more than one place. Either way the handler never sees an
unconnected publisher: registrations carry publish *policies* (pure declarations), and the runtime
pairs them with the connected broker at startup.

An explicit publish is always the same builder: it starts with `message(..)` and ends with
`publish()`. What travels is a value of a declared type, and the wire follows that type:

```text
message(&order)   -> codec -> bytes -> broker    (a Serialize value encodes)
message(&export)  ->          bytes -> broker    (a Serialized value produces its own)
```

Bytes a service already holds encoded travel as a `Serialized` newtype - naming them puts them
in the generated document instead of leaving an anonymous payload on the channel. The positions
the call site has to fill - the destination, the typed headers, the codec - follow from what the
message type declares, so an under-specified publish is a compile error rather than a run-time
surprise.

## Replying from a handler

Name a reply destination with `publish(..)` and return the reply value. The runtime encodes it and
sends it:

=== "Macros"

    ```rust
    use ruststream::subscriber;

    --8<-- "examples/publishing.rs:reply"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/publishing.rs:reply"
    ```

Mount it with plain `include`. With nothing else said, the reply goes out through the broker's
default publish policy under the default codec; `.out(Reply, Publish)` names the policy the broker
prelude exports, and the steps after it fill the rest of the reply wiring - `.codec(..)` for the
reply codec, `.transform(..)` for a static publish transform, `.transactional()` for a batch's
replies in one broker transaction. Each step fills its own slot once, so naming a codec twice is a
compile error rather than a silent overwrite. The wiring is a declaration: the runtime pairs it
with the connected broker at startup.

A definition never names a publisher. It declares what the handler replies with and where it
goes; a publish policy is a broker's, so it is named where a broker is named - the mount site -
with the same `.out(marker, policy)` call that binds an `Out` slot. `Reply` is the marker of the
position a handler's returned value leaves through.

=== "Macros"

    ```rust
    --8<-- "examples/publishing.rs:reply_mount"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/publishing.rs:reply_mount"
    ```

Decoding of the incoming request follows the scope (the scope codec set with
`with_broker_codec`, else the default codec); the reply codec travels on the wiring the chain
built. See [Codecs](codecs.md#the-publish-side).

One clause serves both wires, because the choice belongs to the reply type rather than to the
clause. A `serde::Serialize` reply encodes, as above. A `#[derive(Serialized)]` reply produces
its own bytes and leaves byte-for-byte, so its `.out(Reply, ..)` takes the policy and nothing
else: there is no codec to name on that wire, and `.codec(..)` after it does not compile. See
[raw subscribers](subscribers.md#raw-subscribers).

## Controlling the acknowledgement

A plain reply form always publishes and acks. Return `Result<Reply, HandlerOutcome>` instead to
take control: `Ok(reply)` publishes and acks, `Err(outcome)` publishes nothing and the dispatcher
acts on the returned `HandlerOutcome` (`HandlerOutcome::drop()` to dead-letter,
`HandlerOutcome::retry()` to ask for redelivery):

=== "Macros"

    ```rust
    --8<-- "examples/publishing.rs:reply_result"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/publishing.rs:reply_result"
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
after the broker connects. The same handler mounts unchanged on a production broker and on its
in-process test transport.

=== "Macros"

    ```rust
    use ruststream::runtime::Out;

    --8<-- "examples/publishing.rs:forward"
    ```

=== "Manual"

    ```rust
    use ruststream::runtime::Out;

    --8<-- "examples/manual/publishing.rs:forward"
    ```

`message(&value)` publishes on the value's own wire: a `Serialize` value encodes with the
scope's codec (name another one for a single call with `.with_codec(..)`), a `Serialized` one
leaves byte-for-byte and has no codec position at all. Either fills the headers position with
`.with_headers(..)` - the message's declared contract by reference (`&meta`), or an already-built
`HeaderMap` by value - and either ends in `publish()`.

The include site names the source; for the scope's own broker it is the publish policy:

=== "Macros"

    ```rust
    --8<-- "examples/publishing.rs:forward_mount"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/publishing.rs:forward_mount"
    ```

An `Out` slot left unbound is a compile error, not a runtime one: the registration does not build
until every slot has a policy.

### Named slots

A handler that takes several publishers names a **slot marker** per parameter: a unit struct
deriving `OutSlot`, written as the second type argument (`Out<impl Publisher, Primary>`). The
include site binds each marker with `.out(marker, policy)` and commits the registration with a
terminal `.build()`. The calls bind by marker, so their order does not matter; binding the same
slot twice (or a marker the handler does not declare) fails to compile, and `.build()` exists
only once every slot is bound - a forgotten binding is a compile error whose attachment type
names the slot (`MissingSlot<Audit>`). A single unnamed `Out<impl Publisher>` parameter binds
the implicit `DefaultSlot` (`.out(DefaultSlot, Publish).build()`).

A `.transform(..)` after an `.out(..)` rides the position that call named (see
[the publish pipeline](#the-publish-pipeline)), so a registration that both replies and fans out
names one on each: `.out(Reply, Publish).transform(StampSource).out(Audit, Publish)
.transform(Envelope)`.

=== "Macros"

    ```rust
    use ruststream::OutSlot;

    --8<-- "examples/publishing.rs:slots"
    ```

    ```rust
    --8<-- "examples/publishing.rs:slots_mount"
    ```

=== "Manual"

    ```rust
    use ruststream::OutSlot;

    --8<-- "examples/manual/publishing.rs:slots"
    ```

    ```rust
    --8<-- "examples/manual/publishing.rs:slots_mount"
    ```

The capability in the bound can be refined: `Out<impl OwnedTransactions, Ledger>` compiles only
against a policy whose live publisher supports owned transactions, checked at the include site
with a diagnostic naming the missing capability. The bound names a broker capability trait -
`Publisher`, `TransactionalPublisher`, `OwnedTransactions`, `RequestReply`, or one your broker
crate defines - and never a broker type, so the body stays broker-agnostic; on the manual path
the same bound sits on the entry's wired value, `where L: OutEntry<Ledger, Wire:
OwnedTransactions>`. Under each bound the entry offers that capability's typed form (the publish
builder, a transaction scope, an owned transaction) over the include site's codec and the
marker's list.
The slot marker is also the identity the
[test harness](testing.md#asserting-on-out-slots) records publishes against.

The `Out` parameter's optional third position declares what this handler sends
(`Out<impl Publisher, Marker, (A, B)>`, a single type, or a `#[derive(OutMessages)]` set enum);
a marker's own `#[publishes(A, B)]` list says what the slot may publish, which is what the
generated document reports for a handler that leaves the position unrestricted. A typed publish
of a type the marker does not name is a compile error naming the missing membership. A marker
listing nothing publishes nothing at all: every publish carries a message type, so every publish
is subject to the list. The implicit `DefaultSlot` of a
single unnamed `Out<impl Publisher>` has no declaration site to list types on, so it admits
every declared message. See [typed headers](headers.md).

The wire of a typed publish is selected by the type. `message(&value)` encodes a
`serde::Serialize` value with the resolved codec, as above; a `#[derive(Serialized)]` type
produces its own bytes - holding them, or writing them on the spot - and the same call publishes
them exactly as they are, with no codec anywhere on the path. Everything else follows the
ordinary rules: give the type `#[derive(Outgoing)]` and
list it in `#[publishes(..)]` like any model. It is documented under its own name (with no
payload schema - the bytes are the format), its declared destination resolves the publish, and
the dictionary, a declared message set and the headers positions gate it exactly as they gate an
encoded model.

=== "Macros"

    ```rust
    --8<-- "tests/lanes.rs:serialized_out"
    ```

=== "Manual"

    ```rust
    --8<-- "tests/manual_out_slots.rs:serialized_out"
    ```

### Declaring where a message goes

A message type declares everything about being sent through one derive, with every parameter in
the same `key = value` form. `name` is the destination and `headers` names the contract type
(which stays an ordinary serde struct the derive does not touch):

=== "Macros"

    ```rust
    use ruststream::Outgoing;

    --8<-- "examples/publishing.rs:declared"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/publishing.rs:declared"
    ```

The declaration decides which destination position the call site has:

- **A fixed name** resolves the destination, so there is no `to(..)` to write - and no way to
  send that type somewhere the document does not mention.
- **A name template** (`"orders.{tenant}.placed"`) opens `to()`, which returns a builder with one
  setter per placeholder. `publish()` compiles only once every placeholder is bound, and an
  unbound one rides in the builder's type, so the compile error states that the address is
  unfinished and names the segment that was forgotten. The address is rendered per publish; a
  fixed name publishes from a `&'static str`.
- **No `name` at all** means the call site names it: `.to("orders.archived")`, taking a `&str` or
  a computed `String`.

A message declaring `headers = Meta` publishes only with `.with_headers(&meta)` - forgetting it,
or passing another type, does not compile. In the generated document a fixed name becomes its
channel, a template becomes a templated address whose parameters block is filled from its
placeholders, and a type declaring no destination contributes nothing.

The derive is what makes a value publishable this way, the third case included. A `Serialize`
type owned by another crate cannot derive `Outgoing`, so it stays outside the builder: wrap it in
a newtype that derives `Outgoing`, or, inside a transaction, keep the scope's
`publish(name, &value)`.

=== "Macros"

    ```rust
    --8<-- "examples/publishing.rs:declared_mount"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/publishing.rs:declared_mount"
    ```

The parameter composes with every subscriber form: next to a `Ctx` extractor, on a
self-deserializing input, and on batch handlers (`b.include(f).out(marker, policy).build()` - the
whole batch in, per-element destinations out). On the reply forms - `publish(..)` and its batch
counterpart - the reply is one more position on the same chain, so a gateway names both and
answers on a fixed destination while fanning side copies out through the injection:

=== "Macros"

    ```rust
    --8<-- "examples/publishing.rs:publish_out"
    ```

    ```rust
    --8<-- "examples/publishing.rs:publish_out_mount"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/publishing.rs:publish_out"
    ```

    ```rust
    --8<-- "examples/manual/publishing.rs:publish_out_mount"
    ```

### Publishing to a different broker

When the handler consumes one broker and publishes to another (consume Kafka, forward to Redis),
wrap the target broker with `.bindable()` and mint a **bound token** before registration. The token
is then the source at the include site, shown here with two in-memory brokers; the shape is the
same for any pair:

=== "Macros"

    ```rust
    --8<-- "tests/out_injection.rs:cross_broker"
    ```

=== "Manual"

    ```rust
    --8<-- "tests/manual_out_injection.rs:cross_broker"
    ```

Tokens exist before any `with_broker` runs, so registration order does not matter: a
bidirectional bridge binds both directions up front.

A token shares a slot with the `Bindable` wrapper it was minted from, so register that same
wrapper (`with_broker(bindable, ..)`) for startup to fill the slot with the connected broker; a
token whose broker never registers fails fast at pairing with a clear error. The same shape
works for reply publishing (`.out(Reply, token)` on a `publish("dest")` handler) and for the
batch forms. Outside a registration, a token pairs itself once startup
connected its broker: `running.publisher(token)` hands a sibling task its live publisher - see
[Running beside another server](http.md). For the first publish at startup, no token is needed
at all: the scope-level `b.after_startup(policy, hook)` runs the hook with an already-paired
publisher once subscriptions are open (see [Lifespan](lifespan.md#lifecycle-hooks)); the
publishing example's seeding rides it.

## Where the headers come from

A publish takes its headers from two places. The call site names them with `.with_headers(..)` -
the message's declared contract by reference, or an already-built `HeaderMap` by value - and
the handle sending them may contribute a base of its own. A publisher that carries an argument
for a run of messages (a tenant, a partition hint, a delivery option the broker expresses as a
header) exposes it through `base_headers`, and so does a transaction opened from it.

The builder assembles the outgoing map once - the base first, the call site's headers written
over it key by key, the most specific level winning:

- the **call site** wins over the handle, on every key it names;
- the **handle** wins over nothing, on every key the call leaves alone;
- a handle with no base of its own leaves the call site's headers exactly as written.

Both forms merge the same way: a map upserts entry by entry, and a declared
`headers = Meta` contract serializes its fields over the base, so a message with a contract
still carries the handle's argument.

`.with_headers(..)` is still filled once: a second call is a compile error.

A reply is assembled the same way, though nothing writes `.with_headers(..)` on it: the publisher
the mount site named for the `Reply` position lays its base down first, and the chain's own
`.transform(..)` steps write over it. So a broker option that rides the headers is on what a
`publish("dest")` handler answers with, on every reply of a batch, and on what a body sends
through an `Out` slot, without the handler knowing about it.

## The publish pipeline

Three kinds of transform run before a message leaves the process, and they compose:

- **Static `PublishTransform`** on the reply wiring, chained with `.transform(..)` after
  `.out(Reply, ..)`. Zero-cost, per-destination transforms (an envelope, a fixed content type, or
  stamping the delivery's trace / correlation id onto the reply). They run first, closest to the
  value.
- **Static `OutTransform`** on one `Out` slot, chained with `.transform(..)` after
  `.out(marker, policy)`. The same place in the order, for what leaves through that slot: an
  outbox envelope, a fixed content type, a tenant tag. It takes no `PublishContext` - a slot
  publish is issued by the body itself, so the delivery is the body's own to read and put on the
  message.
- **Static `PublishLayer`** on the application, added with `.publish_layer(..)`. Cross-cutting
  concerns (publish metrics, a dead-letter wrapper) applied to every published message, around the
  send so they can observe its result. The chain composes into a concrete type, so it becomes part
  of the app's type. A builder usually returns `impl App` and never spells
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
`.batch_transform(..)`, reusing a per-message `PublishTransform` via `for_batch(transform)`. Each
reply runs through it one at a time, but they share one `PublishContext`, and it is the batch's,
not a delivery's: a batch spans many deliveries, so `name()` is the subscription, `headers()` is
empty, and `context(..)` reads the broker's batch context. A transform that has to read the
message it is answering for belongs on the per-message path, where a reply and its delivery are
one.

An `OutTransform` implements `apply(&mut Outgoing<'_>)` and rides one slot:

```rust
--8<-- "examples/publishing.rs:slot_transform"
```

A `PublishLayer` implements an around/next signature, so it can short-circuit, retry, or
observe:

```rust
--8<-- "examples/publishing.rs:app_layer"
```

Both levels compose on the application:

=== "Macros"

    ```rust
    --8<-- "examples/publishing.rs:pipeline"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/publishing.rs:pipeline"
    ```

The app-wide layer wraps every publish a handler makes: the reply of a `publish(..)` form and
every message that leaves through an injected `Out` slot. The per-mount transforms stay with what
they were named on - `.out(Reply, Publish).transform(StampSource)` grows the reply's stack,
`.out(Audit, Publish).transform(OutboxEnvelope)` that slot's - so a registration that carries both
writes both, and `.transform(..)` reads as "on the position before it". The order on the wire is the
same on either side: the mount site's transforms first (closest to the encoded value), then the
app-wide middleware, then the send.

Two publishes stay outside it, and both are the ones a body drives itself: a transaction opened on
a slot (`begin()`, `transaction()`) sends into the broker's transaction, and a request / reply
round trip (`request(..)`) waits for an answer instead of ending in a send. A router's slots take
their own transforms and not the app-wide chain: `include_router` mounts routes that were typed
before the app existed, so a handler whose slot publishes have to travel that chain mounts on the
broker scope with `b.include(..)`. The full program is
[`examples/publishing.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/publishing.rs).

## Batch replies and transactions

A `#[subscriber("in", publish("out"))]` handler taking `&[T]` consumes a whole decoded batch and
returns the replies for it - the consume-transform-produce pattern. `Ok(replies)` publishes every reply to
the reply name and acks the batch; `Err(outcome)` publishes nothing and settles the whole batch
with `outcome` (all-or-nothing: selective per-element outcomes do not compose with a
transaction):

=== "Macros"

    ```rust
    --8<-- "examples/publishing.rs:batch_publishing"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/publishing.rs:batch_publishing"
    ```

Mount it with `include`, chaining the reply wiring with `.out(Reply, ..)`:

=== "Macros"

    ```rust
    --8<-- "examples/publishing.rs:batch_publishing_mount"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/publishing.rs:batch_publishing_mount"
    ```

Without `.transactional()`, each reply publishes independently; a mid-batch failure retries
the whole batch, so the earlier replies may be published again on redelivery (at-least-once).
Chaining `.transactional()` after `.out(Reply, ..)` switches the wiring to one broker transaction
per batch: the runtime begins a transaction, publishes every reply, commits, and only then acks
the incoming batch; any failure aborts, so replies are never half-visible. The transactional
requirement is enforced where the wiring is consumed: mounting it needs a policy whose live
publisher is transactional, so a broker without transactions still fails to compile. A
single-message reply has no batch to make atomic, so `.transactional()` mounts on the batch forms
only.

## Manual transactions

Outside the batch-reply path, drive a transaction by hand: `begin()` on any transactional
publisher opens a `TransactionScope` that owns the transaction. Publishes go through the scope,
and `commit()` / `abort()` consume it - so a commit without a begin, a second commit, or a publish
after settling are compile errors, not runtime surprises:

```rust
--8<-- "examples/publishing.rs:manual_transaction"
```

The scope carries the same builder as every other surface (`scope.message(&value).publish()`),
sending into the open transaction instead of straight to the broker. It encodes values with the
publisher's codec and sends them directly: per-publisher transforms and the app-wide
`publish_layer` middleware belong to the dispatch path (they read the
originating delivery) and do not run here. Dropping an unsettled scope logs a warning and leaves
the broker transaction open on that handle - always settle explicitly.

An `Out` slot opens the same scope: bind the slot with `Out<impl TransactionalPublisher, Journal>`
(`where W: TransactionalPublisher` on the manual path), and `begin()` on the entry returns the
scope, driven exactly as above. A scope opened on a slot admits what the slot's own `message`
admits - the marker's list, narrowed by the parameter's declared set - so a transaction cannot
publish what the generated document never declared, and its publishes keep the slot's capture in
the test harness.

The scope is the borrowed transaction kind: it borrows the handle's single broker-side
transaction, so one scope per handle is open at a time. Brokers whose transactions are client
buffers rather than producer state also implement the owned kind, `OwnedTransactions`: every call
opens an independent transaction whose buffer lives in the returned `TypedTransaction`, so any
number can be open concurrently on one handle and settling one never touches another.
`message(..).publish()` buffers into the value and `commit()` / `abort()` consume it - the same
settle-by-consuming discipline as the scope - while dropping one merely discards its buffer (with
a warning) instead of leaving a broker transaction open. Kafka-like brokers, whose client holds
exactly one transaction per producer, implement only the borrowed kind.

The owned kind reads the same on both surfaces: a publisher that buffers client-side transactions
offers `owned_transaction()`, and a slot bound with `Out<impl OwnedTransactions, Ledger>` offers
`transaction()`. Both open a `TypedTransaction` that owns the broker transaction and encodes with
the surface's codec - `let mut txn = publisher.owned_transaction().await?;`, then
`txn.message(&value).publish().await?;` and `txn.commit().await?;`. Where `begin()` gives the
borrowed scope (one per handle), any number of `TypedTransaction`s can be open on one publisher at
a time. (The name spells out *owned* because a broker's own `OwnedTransactions::transaction` is in
scope wherever this is.) An owned transaction's buffer settles outside the slot, so its publishes
land in the broker's publish log rather than the slot's own capture.

## Batch publishing

To publish many messages, publish them in a loop: for most brokers (NATS, Kafka) the client
already coalesces writes, so the loop reaches the same throughput a dedicated batch call would.
Where a broker has a genuine pipeline primitive (Redis), its crate exposes it as a broker-specific
capability.
