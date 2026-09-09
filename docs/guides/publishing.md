# Publishing and replies

A handler publishes in one of two ways. Returning a reply is the shorter one, and the right default
when the handler answers on one declared destination. You take a publisher as an `Out` parameter
when the destination is decided at run time, or when the message goes to several destinations at
once, including destinations in different brokers. Either way the handler never sees an unconnected
publisher: you specify a publish *policy* when you register the handler, and at startup it
instantiates the publisher on the connected broker.

An explicit publish is always the same builder: it starts with `message(..)` and ends with
`publish()`. What you publish is a value of a declared type, and the type itself selects how it is
sent:

```text
message(&order)   -> codec -> bytes -> broker    (a Serialize value encodes)
message(&export)  ->          bytes -> broker    (a Serialized value produces its own)
```

Bytes a service already holds encoded are published through a newtype deriving `Serialized`, and
the name puts them in the generated document. The message type declares which positions are left
to the call site: the destination, the typed headers, the codec. An under-specified publish does
not compile.

## Replying from a handler

A reply is a message this service sends, so its type derives `Outgoing`. Return the reply value
and write `publish` on the subscriber; `#[outgoing(name = "..")]` on the type is the destination:

=== "Macros"

    ```rust
    use ruststream::Outgoing;

    --8<-- "examples/publishing.rs:reply_declared"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/publishing.rs:reply_declared"
    ```

A reply type that declares no name takes one from the mount site: `publish("responses")` on the
attribute, `.to("responses")` on the chain. On a type that fixes its own name that mount-site name
is a default and does not apply.

=== "Macros"

    ```rust
    use ruststream::subscriber;

    --8<-- "examples/publishing.rs:reply"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/publishing.rs:reply"
    ```

Mount it with plain `include`. With nothing else said, the reply is published through the broker's
default publish policy under the default codec. `.out(Reply, Publish)` names the policy the broker
prelude exports, and the steps after it fill the rest of the reply wiring: `.codec(..)` sets the
reply codec, `.transform(..)` a static publish transform, `.transactional()` one broker transaction
per batch of replies. The codec is named once, so a second `.codec(..)` is a compile error.

A handler definition never names a publisher: it declares what the handler replies with and where
it goes. A publish policy belongs to a broker, so you specify it where the broker is named, at the
mount site, with the same `.out(marker, policy)` call that binds an `Out` slot. `Reply` is the
marker of the position a handler's returned value is published through.

=== "Macros"

    ```rust
    --8<-- "examples/publishing.rs:reply_mount"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/publishing.rs:reply_mount"
    ```

The incoming request decodes with the scope codec set by `with_broker_codec`, or with the default
codec. The reply codec comes from the wiring the chain built. See
[Codecs](codecs.md#the-publish-side).

One `publish(..)` serves both ways of sending: the choice belongs to the reply type. A
`serde::Serialize` reply encodes, as above. A `#[derive(Serialized)]` reply produces its own bytes
and is published byte for byte, so its `.out(Reply, ..)` takes the policy and nothing else: there
is no codec to name on that path, and `.codec(..)` after it does not compile. See
[raw subscribers](subscribers.md#raw-subscribers).

## Controlling the acknowledgement

A plain reply form always publishes and acks. To take control, return
`Result<Reply, HandlerOutcome>` instead. `Ok(reply)` publishes and acks; `Err(outcome)` publishes
nothing, and the dispatcher acts on the returned `HandlerOutcome` (`HandlerOutcome::drop()` to
dead-letter, `HandlerOutcome::retry()` to ask for redelivery):

=== "Macros"

    ```rust
    --8<-- "examples/publishing.rs:reply_result"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/publishing.rs:reply_result"
    ```

The `Result` form is detected from the written signature, so spell it out: a type alias hiding the
`Result` counts as a plain reply type. A publishing handler, like any other, can declare an
optional second `&mut Context` parameter and read application state or publish by hand.

If the reply publish itself returns an error (the broker rejected it, the connection was lost), the
incoming message is nacked with `requeue = true` and the broker delivers it again. Make publishing
handlers idempotent under redelivery.

## Publishing from inside a handler

When the destination is decided at run time, or the message goes to several destinations at once,
including destinations in different brokers, take the publisher as a handler parameter with `Out`.
The pattern `Out(out): Out<impl Publisher>` binds `out` to a live publisher inside the body. The
signature names only the capability the handler needs, never a broker publisher type: the concrete
type is inferred from the policy specified where the handler is included. The same handler mounts
unchanged on a production broker and on its in-process test transport.

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

`message(&value)` publishes the way the value's type selects. A `serde::Serialize` value encodes
with the scope's codec; for a single call you can name another codec with `.with_codec(..)`. A
`Serialized` value is published byte for byte and has no codec position. Both kinds have a headers
position: `.with_headers(..)` takes the message's declared contract by reference (`&meta`), or an
already-built `HeaderMap` by value. A publish ends with `publish()`.

The include site names the source; for the scope's own broker it is the publish policy:

=== "Macros"

    ```rust
    --8<-- "examples/publishing.rs:forward_mount"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/publishing.rs:forward_mount"
    ```

An unbound `Out` slot is a compile error: the registration does not build until every slot has a
policy.

### Named slots

A handler that takes several publishers names a **slot marker** per parameter: a unit struct
deriving `OutSlot`, written as the second type argument (`Out<impl Publisher, Primary>`). The
include site binds each marker with `.out(marker, policy)` and commits the registration with a
terminal `.build()`. The calls bind by marker, so their order does not matter. Binding the same
slot twice, or naming a marker the handler does not declare, is a compile error. `.build()` exists
only once every slot is bound, so a forgotten binding does not compile either, and the slot is
named in the type (`MissingSlot<Audit>`). A single unnamed `Out<impl Publisher>` parameter binds
the implicit `DefaultSlot` (`.out(DefaultSlot, Publish).build()`).

A `.transform(..)` after an `.out(..)` applies to the position that call named (see
[the publish pipeline](#the-publish-pipeline)), so a registration that both replies and sends
copies names one on each:
`.out(Reply, Publish).transform(StampSource).out(Audit, Publish).transform(Envelope)`.

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
against a policy whose live publisher supports owned transactions. The bound is checked at the
include site, and the compile error names the missing capability. The bound names a broker
capability trait (`Publisher`, `TransactionalPublisher`, `OwnedTransactions`, `RequestReply`, or
one your broker crate defines) and never a broker type, so the body stays broker-agnostic. On the
manual path the same bound sits on the entry's wired value,
`where L: OutEntry<Ledger, Wire: OwnedTransactions>`. Under each bound the entry offers that
capability's typed form (the publish builder, a transaction scope, an owned transaction) over the
include site's codec and the marker's list. The slot marker is also the name the
[test harness](testing.md#asserting-on-out-slots) records publishes against.

The `Out` parameter's optional third position declares what this handler publishes:
`Out<impl Publisher, Marker, (A, B)>`, a single type, or a set enum deriving `OutMessages`. A
marker's own `#[publishes(A, B)]` list says what the slot may publish, and that is what the
generated document reports for a handler that leaves the third position unrestricted. A typed
publish of a type the marker does not name is a compile error. A marker listing nothing publishes
nothing at all: every publish carries a message type, so the list covers every publish. The
implicit `DefaultSlot` of a single unnamed `Out<impl Publisher>` has no declaration site to list
types on, so it admits every declared message. See [typed headers](headers.md).

A type deriving `Serialized` is published into a slot byte for byte, with no codec on the path.
Everything else follows the ordinary rules: give the type `#[derive(Outgoing)]` and list it in
`#[publishes(..)]` like any model. The document shows it under its own name and with no payload
schema, because here the bytes are the format. The destination comes from the declaration, and the
dictionary, a declared message set and the headers positions gate it exactly as they gate an
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

A message type declares everything about being sent through one derive, and every parameter is
written in the same `key = value` form. `name` is the destination, and `headers` names the headers
contract type; the contract itself stays an ordinary serde struct that the derive does not touch:

=== "Macros"

    ```rust
    use ruststream::Outgoing;

    --8<-- "examples/publishing.rs:declared"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/publishing.rs:declared"
    ```

The declaration decides which destination position is left to the call site:

- **A fixed name** sets the destination, so there is no `to(..)` to write: such a type publishes
  only to the address in its declaration.
- **A name template** (`"orders.{tenant}.placed"`) opens `to()`, which returns a builder with one
  setter per placeholder. `publish()` compiles only once every placeholder is bound, and an
  unbound one stays in the builder's type, so the compile error says that the address is
  unfinished and names the forgotten segment. The address is built again on every publish, while a
  fixed name is published from a `&'static str`.
- **No `name` at all** means the call site names the destination: `.to("orders.archived")` takes a
  `&str` or a computed `String`.

A message declaring `headers = Meta` publishes only with `.with_headers(&meta)`: forgetting the
call, or passing another type, does not compile. In the generated document a fixed name becomes
the message's channel, and a template becomes an address with parameters, whose block is filled
from the placeholders. A type that declares no destination does not appear in the document.

The derive is what makes a value publishable through the builder, the third case included. A
`Serialize` type from another crate cannot derive `Outgoing`: you can wrap such a value in a
newtype that derives `Outgoing`, or, inside a transaction, publish it through the scope's
`publish(name, &value)`.

=== "Macros"

    ```rust
    --8<-- "examples/publishing.rs:declared_mount"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/publishing.rs:declared_mount"
    ```

An `Out` parameter composes with every subscriber form: next to a `Ctx` extractor, on a handler that
deserializes its input itself, and on batch handlers (`b.include(f).out(marker, policy).build()`,
the whole batch in, per-element destinations out). On the reply forms, `publish(..)` and its batch
counterpart, the reply is one more position on the same chain. So a gateway names both positions,
answers on a fixed destination and at the same time sends side copies through the injected
publisher:

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

When a handler consumes from one broker and publishes to another (consume Kafka, forward to Redis),
wrap the target broker with `.bindable()` and mint a **bound token** before registration. The token
is then the source at the include site. The form is the same for any pair of brokers; it is shown
here on two in-memory brokers:

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
wrapper (`with_broker(bindable, ..)`) for startup to fill the slot with the connected broker. A
token whose broker never registers returns a clear error right at pairing. The same form works for
reply publishing (`.out(Reply, token)` on a `publish("dest")` handler) and for the batch forms.

Outside a registration a token pairs itself once startup has connected its broker:
`running.publisher(token)` hands a sibling task its live publisher, see
[Running beside another server](http.md). For the very first publish at startup no token is needed
at all: the scope-level `b.after_startup(policy, hook)` runs the hook with an already-paired
publisher once subscriptions are open (see [Lifespan](lifespan.md#lifecycle-hooks)). The publishing
example's seeding runs on it.

## Where the headers come from

A publish takes its headers from two places. The call site names them with `.with_headers(..)`:
the message's declared contract by reference, or an already-built `HeaderMap` by value. The
publisher can add a base of its own: it exposes through `base_headers` one argument for a whole run
of messages (a tenant, a partition hint, a delivery option the broker expresses as a header). A
transaction opened from that publisher does the same.

The builder assembles the outgoing headers once: the base first, then the call site's headers over
it, key by key. A key takes its value in this order:

- a key the **call site** names takes its value from there;
- a key the call site leaves alone takes its value from the base of the **publisher**;
- a publisher with no base of its own leaves the call site's headers exactly as written.

Both forms merge the same way: an already-built `HeaderMap` is added entry by entry, and a declared
`headers = Meta` contract serializes its fields over the base, so a message with a contract also
gets the publisher's argument.

`.with_headers(..)` is filled once: a second call is a compile error.

A reply is assembled the same way, though nothing writes `.with_headers(..)` on it. Its base comes
from the publisher the policy constructed for the `Reply` position named at the mount site, and the
chain's own `.transform(..)` steps write over it. So a broker option set through a header is on the
reply of a `publish("dest")` handler, on every reply of a batch, and on what the body sends through
an `Out` slot, without the handler knowing about it.

## The publish pipeline

Three kinds of transform run before a message leaves the process, and they compose:

- **Static `PublishTransform`** on the reply wiring, chained with `.transform(..)` after
  `.out(Reply, ..)`. Zero-cost transforms for one destination: an envelope, a fixed content type,
  the delivery's trace / correlation id on the reply. They run closest to the value, before the
  app-wide pipeline.
- **Static `OutTransform`** on one `Out` slot, chained with `.transform(..)` after
  `.out(marker, policy)`. It takes the same place in the order, and works on what leaves through
  that slot: an outbox envelope, a fixed content type, a tenant tag. It takes no `PublishContext`:
  the body itself issues a slot publish, so the body reads the delivery and puts it on the message.
- **Static `PublishLayer`** on the application, added with `.publish_layer(..)`. Cross-cutting
  concerns (publish metrics, a dead-letter wrapper) applied to every published message, around the
  send so they can read its result. The chain composes into a concrete type and becomes part of the
  app's type: a builder usually returns `impl App` and never spells it out, while the concrete
  `RustStream<L, St, PublishStack<MyMiddleware, PublishIdentity>>` shows the pipeline in the type
  itself, and an app with no `publish_layer` keeps the default `PublishIdentity`. Each middleware
  must be `Clone` (the pipeline is cloned into each publishing handler), and the last one added runs
  outermost. The default (no middleware) is a direct send. You can wrap a middleware set decided at
  run time in a `PublishDynStack` (the publish counterpart of `DynStack`) and add that instead.

A static `PublishTransform` implements `apply(&mut Outgoing<'_>, &PublishContext<'_, C>)`. The
`PublishContext` gives read-only access to the delivery that produced the reply: its channel, the
incoming headers, and the broker's typed per-delivery context, read by `Field` key. So a transform
can copy a value from the incoming message to the reply:

```rust
--8<-- "examples/publishing.rs:static_transform"
```

A batch handler's replies go past the per-message `.transform(..)` stack. You can add a transform
for them with `.batch_transform(..)`, reusing a per-message `PublishTransform` through
`for_batch(transform)`.

The replies run through it one at a time, and they share one `PublishContext`. That context is the
batch's, not a delivery's: a batch spans many deliveries, so `name()` is the subscription,
`headers()` is empty, and `context(..)` reads the broker's batch context. A transform that reads
the incoming message belongs on the per-message path, where a reply and its delivery are one.

An `OutTransform` implements `apply(&mut Outgoing<'_>)` and works on one slot:

```rust
--8<-- "examples/publishing.rs:slot_transform"
```

### Naming a destination per message

Where a message goes is declared: on the message type with `#[outgoing(name = "..")]`, at the mount
site with `publish("dest")`, or at a slot's call site with `.to(..)`. Some answers have no
destination to declare. An AMQP request carries the queue to answer on in its `reply-to` header,
and a ZeroMQ `ROUTER` addresses each answer to the peer that asked.

`.redirect(..)` is the step that gives a transform the destination. It takes the same
`PublishTransform` as `.transform(..)`, and that transform reads the delivery and sets the name:

```rust
--8<-- "examples/publishing.rs:redirect"
```

Name it on the chain with `.redirect(..)`:

```rust
--8<-- "examples/publishing.rs:redirect_mount"
```

The step is what the compiler holds you to, not the transform. `.redirect(..)` applies to a reply
type that leaves its destination open; on a type carrying `#[outgoing(name = "receipts")]` it is a
compile error naming the reply type. A reply whose channel the document reports therefore has no
way to be redirected. The mount site's `publish("answers")` stays the reply's declared destination.
It is the name the generated document reports, and where a reply goes when the redirect leaves the
name alone.

A batch's replies cannot be redirected: they are published against the batch, which answers many
deliveries and carries none of their headers. A position takes one redirect, so a second
`.redirect(..)` on it does not compile.

An `Out` slot takes the same step, with the `OutTransform` that `.transform(..)` takes there.

Every type in a redirected slot's `#[publishes(..)]` list has to leave its destination open. The
body still writes `.to(..)` on every publish, and the redirect writes over that name. A marker with
no list admits every declared message and cannot be redirected at all, the implicit `DefaultSlot`
of a single unnamed `Out<impl Publisher>` included.

Such a slot offers plain sending only: a transaction or a request / reply round trip reaches the
broker without the slot's publish path, so a handler that asks for either does not compile.

A `PublishLayer` implements an around/next signature, so it can stop the chain, retry the send, or
just observe:

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

The app-wide layer wraps every publish a handler makes: the reply of a `publish(..)` form and every
message that leaves through an injected `Out` slot.

The mount site's transforms act on the position they were named on:
`.out(Reply, Publish).transform(StampSource)` grows the reply's stack,
`.out(Audit, Publish).transform(OutboxEnvelope)` grows that slot's. A registration with both sides
writes both calls, and `.transform(..)` applies to the position named before it. A position runs
its steps in a fixed order, whatever order the chain names them in: the redirect first, then the
position's transforms, then the app-wide middleware, then the send.

Two publishes stay outside the pipeline, and the body drives both itself: a transaction opened on a
slot (`begin()`, `transaction()`) sends into the broker's transaction, and a request / reply round
trip (`request(..)`) waits for an answer instead of ending in a send.

A router's slots run their own transforms and not the app-wide chain: `include_router` mounts routes
whose types were fixed before the app existed. To send a slot's publishes through that chain, mount
the handler on the broker scope with `b.include(..)`. The full program is
[`examples/publishing.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/publishing.rs).

## Batch replies and transactions

A `#[subscriber("in", publish("out"))]` handler taking `&[T]` consumes a whole decoded batch and
returns the replies for it - the consume-transform-produce pattern. `Ok(replies)` publishes every
reply to the declared destination and acks the batch; `Err(outcome)` publishes nothing and settles
the whole batch with `outcome` (all-or-nothing: selective per-element outcomes do not compose with
a transaction):

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

Without `.transactional()`, each reply publishes independently. A mid-batch failure retries the
whole batch, so on redelivery the earlier replies may be published again (at-least-once).

Chaining `.transactional()` after `.out(Reply, ..)` switches the wiring to one broker transaction
per batch: the runtime begins a transaction, publishes every reply, commits it, and only then acks
the incoming batch. Any failure aborts the transaction, so replies are never half-visible.

Mounting such wiring needs a policy whose live publisher is transactional, so a program on a broker
without transactions does not compile. A single-message reply has no batch to make atomic, so
`.transactional()` is only on the batch forms.

## Manual transactions

Outside the batch-reply path, drive a transaction by hand: `begin()` on any transactional publisher
opens a `TransactionScope` that owns the transaction. Publishes go through the scope, and
`commit()` / `abort()` consume it, so a commit without a begin, a second commit, or a publish after
settling are compile errors, not surprises at run time:

```rust
--8<-- "examples/publishing.rs:manual_transaction"
```

The scope has the same builder as every other surface (`scope.message(&value).publish()`), but it
sends into the open transaction instead of straight to the broker. It encodes values with the
publisher's codec and sends them directly: per-publisher transforms and the app-wide
`publish_layer` middleware belong to the dispatch path (they read the originating delivery) and do
not run here. A scope dropped unsettled logs a warning and leaves the broker transaction open on
that handle, so settle it explicitly.

An `Out` slot opens the same scope: constrain the slot with
`Out<impl TransactionalPublisher, Journal>` (`where W: TransactionalPublisher` on the manual path),
and `begin()` on the entry returns the scope, driven exactly as above.

A scope opened on a slot admits what the slot's own `message` admits: the marker's list, narrowed
by the parameter's declared set. So a transaction cannot publish what the generated document never
declared, and the test harness records its publishes against the slot.

The scope is the borrowed kind of transaction: it takes the handle's single broker-side
transaction, so one scope per handle is open at a time.

Brokers whose transactions are client buffers rather than producer state also implement the owned
kind, `OwnedTransactions`: every call opens an independent transaction whose buffer lives in the
returned `TypedTransaction`, so one handle holds any number of them at once, and settling one never
touches another. `message(..).publish()` buffers into the value and `commit()` / `abort()` consume
it, the same settle-by-consuming discipline as the scope, while a dropped one merely discards its
buffer (with a warning) and leaves no broker transaction open. Kafka-like brokers, whose client
holds exactly one transaction per producer, implement only the borrowed kind.

The owned kind reads the same on both surfaces: a publisher that buffers transactions client-side
offers `owned_transaction()`, and a slot bound with `Out<impl OwnedTransactions, Ledger>` offers
`transaction()`. Both open a `TypedTransaction` that owns the broker transaction and encodes with
the surface's codec: `let mut txn = publisher.owned_transaction().await?;`, then
`txn.message(&value).publish().await?;` and `txn.commit().await?;`. Where `begin()` gives the
borrowed scope (one per handle), one publisher holds any number of `TypedTransaction`s at a time.
An owned transaction's buffer settles outside the slot, so its publishes go into the broker's
publish log and are not recorded against the slot.

## Batch publishing

Publish many messages in a loop: for most brokers (NATS, Kafka) the client already coalesces
writes, so the loop reaches the same throughput a dedicated batch call would. Where a broker has a
genuine pipeline primitive (Redis), its crate exposes it as a broker-specific capability.
