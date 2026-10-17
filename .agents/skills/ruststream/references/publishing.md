# Publishing: replies, slots, settings, transforms

How a handler answers and publishes. Component pages: [Publishing](https://powersemmi.github.io/ruststream/latest/components/subscribers/publishing/), [Publishers](https://powersemmi.github.io/ruststream/latest/components/subscribers/publishing/publishers/), [Transforms](https://powersemmi.github.io/ruststream/latest/components/subscribers/publishing/transforms/).

## Contents

- Replying (request/response)
- Publishing from a handler (`Out`)
- Broker settings per message
- Publish transforms
- First publish at startup (`after_startup`)

## Replying (request/response)

Return the reply value and write `reply` on the subscriber. The reply type derives `Outgoing`,
and its declaration decides where the answer goes: a type fixing its own name is published there
and the clause names nothing, a type naming nothing takes the clause's name. The reply publisher is
the broker's default policy unless the mount names one with `.out_reply(..)`.

```rust
use ruststream::memory::prelude::*;
use serde::Serialize;

// the destination on the type: the clause stays bare
#[derive(Debug, Serialize, Outgoing)]
#[outgoing(name = "receipts")]
struct Receipt {
    id: u64,
}

#[subscriber("receipt-requests", reply)]
async fn issue_receipt(order: &Order) -> Receipt {
    Receipt { id: order.id }
}

// the destination left open: the mount site says
#[derive(Debug, Serialize, Outgoing)]
struct Confirmation {
    id: u64,
    accepted: bool,
}

#[subscriber("orders", reply("confirmations"))]
async fn confirm(order: &Order) -> Confirmation {
    Confirmation { id: order.id, accepted: true }
}

// in with_broker(broker, |b| { ... }):
b.include(issue_receipt);                              // the type carries the destination
b.include(confirm);                                    // broker's default publish policy
b.include(confirm).out_reply(Publish);                 // an explicit one
b.include(confirm).out_reply(Publish).codec(CborCodec).transform(Envelope);  // the whole wiring
```

The steps after `.out_reply(..)` fill the reply wiring, each once: `.codec(..)` names the reply
codec (the default codec applies without it), `.transform(..)` composes a static
`PublishTransform`, `.batch_transform(..)` does the same on a batch's replies, and
`.transactional()` publishes a batch's replies inside one broker transaction. A second `.codec(..)`,
or any of these on a `Serialized` (byte-for-byte) reply, is a compile error.

Return `Result<Reply, HandlerOutcome>` to control acknowledgement: `Ok(reply)` publishes and acks,
`Err(outcome)` publishes nothing and the dispatcher acts on the returned `HandlerOutcome`. Spell the
`Result` out in the signature (a type alias is treated as a plain reply type). A failed reply
publish nacks the incoming message with `requeue = true`.

The `reply(..)` clause takes a string literal, and a name template does not resolve here.
Answering somewhere else per delivery (a direct `reply-to`, a `ROUTER` peer identity) is a
transform; see [naming a destination from a transform](#naming-a-destination-from-a-transform).

## Publishing from a handler (`Out`)

The publisher arrives as a parameter. The mount binds each slot marker to a publish policy; the
runtime pairs it with the connected broker at startup, so the handler always holds a live
publisher.

```rust
use ruststream::memory::prelude::*;
use serde::Serialize;

#[derive(Debug, Serialize, Outgoing)]
struct Event {
    id: u64,
}

#[subscriber("ingress")]
async fn forward(event: &Event, Out(out): Out<impl Publisher>) -> HandlerOutcome {
    if out.message(event).to("egress").publish().await.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

// in with_broker(broker, |b| { ... }): the single unnamed slot is DefaultSlot
b.include(forward).out(DefaultSlot, Publish).build();
```

### Several publishers: one marker per path

A handler that publishes down more than one path names a **slot marker** per parameter - a unit
struct deriving `OutSlot`, whose `#[publishes(..)]` list is what may leave through that path. The
include site binds each marker with `.out(marker, policy)` and commits with a terminal `.build()`;
the calls bind by marker, so their order does not matter. A forgotten binding is a compile error
naming the slot (`MissingSlot<Audit>`), never a runtime surprise. `DefaultSlot` (in the
prelude) names the implicit single slot when a handler also has a reply side.

```rust
// different paths, different message types
#[derive(OutSlot)]
#[publishes(OrderConfirmed, OrderPlaced)]
struct Orders;

#[derive(OutSlot)]
#[publishes(AuditEntry)]
struct Audit;

#[subscriber("orders.incoming")]
async fn route(
    event: &Event,
    Out(orders): Out<impl Publisher, Orders>,
    Out(audit): Out<impl Publisher, Audit>,
) -> HandlerOutcome {
    // publishing an AuditEntry through `orders` is a compile error: not in its list
    HandlerOutcome::ack()
}

b.include(route)
    .out(Audit, Publish)
    .transform(OutboxEnvelope)  // rides the slot the .out(..) before it bound
    .out(Orders, Publish)
    .build();
```

`.transform(..)` composes a `PublishTransform` onto the position named right before it, and onto
that one only; before any `.out(..)` the step has no position to ride. The step repeats, and the
transforms run in the order the chain writes them. On a registration that also replies,
`.transform(..)` after `.out_reply(..)` grows the reply's stack and after `.out(marker, ..)` the
slot's.

The capability in the bound can be refined - `Out<impl OwnedTransactions, Ledger>` compiles only
against a policy whose live publisher supports owned transactions, checked at the include site.
The bound names a broker capability trait (`Publisher`, `TransactionalPublisher`,
`OwnedTransactions`, `RequestReply`, or one a broker crate defines), never a broker type; on the
manual path the same bound sits on the entry's wired value,
`where L: OutEntry<Ledger, Wire: OwnedTransactions>`. Under it the entry offers that capability's
typed form - `message(..)`, `begin()`, `transaction()` - over the include site's codec and the
marker's list.

### Narrowing one path to a set of types

The optional **third** position declares what this handler sends through that slot. Omitted (or
`()`) it is unrestricted: the whole marker list, which is also what the generated document reports.

```rust
Out<impl Publisher, Orders>                            // whole #[publishes(..)] list
Out<impl Publisher, Orders, ()>                        // the same, spelled out
Out<impl Publisher, Orders, OrderConfirmed>            // one type (it declares itself)
Out<impl Publisher, Orders, (OrderConfirmed, OrderPlaced)>  // a tuple set
Out<impl Publisher, Orders, ConvertSends>              // a reusable named set
```

A named set is a `#[derive(OutMessages)]` enum whose variants wrap the models. It is a type-level
declaration and is never constructed, so it is the way to share one set across handlers:

```rust
#[derive(OutMessages)]
enum ConvertSends {
    Confirmed(OrderConfirmed),
    Placed(OrderPlaced),
}
```

Publishing a type the set does not name is a compile error naming the missing membership. A type
that carries its own wire format belongs in the dictionary too, and the wire follows the type:
derive `Serialized` next to `#[derive(Outgoing)]`, list it in `#[publishes(..)]` like any model -
the document reports it under its own name with no payload schema - and publish it through the
same typed entry, `out.message(&export).publish()`: its bytes leave as they are, no codec runs,
and the destination comes off the declaration. Bytes with no declared type have no publish entry;
wrap them in a `Serialized` newtype.

### Where a message goes

The declaration on the message type decides which destination position the call site even has:

```rust
#[derive(Debug, Serialize, Outgoing)]
#[outgoing(name = "orders.confirmed")]           // fixed
struct OrderConfirmed { id: u64 }

#[derive(Debug, Serialize, Outgoing)]
#[outgoing(name = "orders.{tenant}.placed")]     // template
struct OrderPlaced { id: u64 }

#[derive(Debug, Serialize, Outgoing)]            // no name: the call site says
struct Event { id: u64 }

#[derive(Debug, Outgoing, Serialized)]           // bytes that already are the payload
struct Export(Vec<u8>);
```

```rust
// fixed name: resolved, so there is no `to(..)` to write
orders.message(&OrderConfirmed { id }).publish().await?;

// template: `to()` opens a builder with one setter per placeholder, and `publish()`
// compiles only once every one is bound - an unbound segment rides in the type, so the
// error names the segment that was forgotten
orders.message(&OrderPlaced { id }).to().tenant("acme").publish().await?;

// no declared name: `to(..)` takes a &str or a computed String
out.message(&Event { id }).to(format!("events.{shard}")).publish().await?;

// bytes as delivered: a Serialized newtype rides the same entry, and no codec runs
out.message(&Export(bytes)).to("egress").publish().await?;
```

A message declaring `headers = Meta` publishes only with `.with_headers(&meta)` - forgetting it,
or passing another type, does not compile. `with_headers` also takes an already-built `HeaderMap`
by value. A `Serialize` type owned by another crate cannot derive `Outgoing`: wrap it in a newtype
that does. The publisher may add a base of its own (`base_headers`, a tenant or a producer name);
the builder lays that base down first and writes the call site's headers over it, key by key.

## Broker settings per message

A QoS, a priority, an ordering key, an expiration are the broker's own per-message settings, not
headers. They reach a publish from two places. The publish policy carries the defaults, named at
the mount site, and every message through that position is sent with them:

```rust
b.include(record).out(Ledger, Publish::default().qos(1)).build();
b.include(acknowledge).out_reply(Publish::default().qos(1));  // a reply has no call site
```

The call adjusts one setting for one message, through a step the broker adds to the publish
builder, and a transform on the position adjusts one where there is no call site to (a reply, the
deferred retry copy). A setting neither names keeps the policy's default, and the rest of the
publish is still the mount site's: the entry's codec, its transforms, the slot the test harness
records it against.

```rust
// the body imports the broker's prelude for the step, and names its options type in the bound -
// the one place a handler body imports more than the framework prelude
use ruststream_rumqttc::prelude::*;

#[subscriber("telemetry.in")]
async fn record(
    reading: &Reading,
    Out(audit): Out<impl Publisher<Options = MqttOptions>, Telemetry>,
) -> HandlerOutcome {
    if audit.message(reading).to("telemetry.out").retain(true).publish().await.is_err() {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}
```

The steps are on the builder, so they work on every publish surface: an `Out` slot, a transaction
opened on one, a bare publisher from the state or a startup hook. Which steps exist is the broker's
documentation to answer. A setting never rides a header and never wraps the publisher (an adapter
would lose the mount site's codec).

## Publish transforms

A published message passes two levels. A `PublishTransform` sits on one position, chained with
`.transform(..)` after the `.out(..)` that named it; a `PublishLayer` sits on the application,
added with `.publish_layer(..)` before `with_broker`, and wraps every publish a handler makes,
replies and slots alike. A position's transforms run first, then the app-wide layers, then the
send. A router's slots run their own transforms and not the app-wide chain, because its routes are
typed before the app that mounts them.

Every impl of `PublishTransform<K, Options>` declares three things. What it **reads** is the
position's kind: `ForReply<C>` hands it `PublishContext<'_, C>` (the delivery being answered - its
channel, the incoming headers, the broker's typed per-delivery fields), `ForSlot` hands it
`SlotContext<'_>` (the slot's own name and nothing else, because the body issued that publish and
has already read its own `Context`). A transform reading nothing writes one impl over every kind
and mounts anywhere. What it **writes** is `Options`, the broker's per-message settings: a
transform generic over them touches none and mounts over any broker's publisher, one naming a
broker's type mounts over that broker's publisher and nowhere else. What it **may do** is
`type Destination`: `Reads` leaves the destination alone, `Names` sets it. Stable Rust has no
associated-type default, so every impl writes the line; almost every one writes `Reads`.

```rust
use ruststream::runtime::{
    ContextKind, ForReply, ForSlot, Names, Outgoing, PublishContext, PublishTransform, Reads,
    SlotContext,
};

/// Reads and writes nothing, so one impl serves a reply and a slot on any broker alike.
struct EnvelopeTransform;

impl<K: ContextKind, Options> PublishTransform<K, Options> for EnvelopeTransform {
    type Destination = Reads;

    fn apply(
        &self,
        out: &mut Outgoing<'_>,
        _options: &mut Option<Options>,
        _cx: &K::View<'_>,
    ) {
        out.headers_mut().insert("x-envelope", b"1".to_vec());
    }
}

/// Writes one of the broker's per-message settings, so it mounts over that broker's publisher
/// only. The position starts at `None` - the policy's defaults apply - and on a slot it starts
/// as a copy of what the call site's steps set, so this completes the call rather than
/// replacing it.
struct Expedite;

impl PublishTransform<ForSlot, MqttOptions> for Expedite {
    type Destination = Reads;

    fn apply(
        &self,
        _out: &mut Outgoing<'_>,
        options: &mut Option<MqttOptions>,
        _cx: &SlotContext<'_>,
    ) {
        options.get_or_insert_with(MqttOptions::default).retain = Some(true);
    }
}
```

### Naming a destination from a transform

Some answers have no destination to declare: an AMQP request carries the queue to answer on in its
`reply-to` header, a ZeroMQ `ROUTER` addresses each answer to the peer that asked. A transform
declaring `Destination = Names` sets the name per delivery, and it mounts with the ordinary
`.transform(..)` step.

```rust
struct ReplyTo;

impl<C, Options> PublishTransform<ForReply<C>, Options> for ReplyTo {
    type Destination = Names;

    fn apply(
        &self,
        out: &mut Outgoing<'_>,
        _options: &mut Option<Options>,
        cx: &PublishContext<'_, C>,
    ) {
        if let Some(to) = cx.headers().get_shared("reply-to")
            && let Ok(to) = Str::try_from(to)
        {
            out.set_name(to);
        }
    }
}

// `Response` leaves its destination open, so this position offers the naming right;
// "answers" is the declared fallback and what the document reports
b.include(answer).out_reply(Publish).transform(ReplyTo);
```

`get_shared` hands the header value over as the buffer the delivery holds, and `Str::try_from`
checks it is UTF-8, so the address reaches `set_name` without being copied.

A position offers that right only where nothing declared the destination, and elsewhere the
`.transform(..)` call does not compile, naming the type that already declared one. A reply type
carrying `#[outgoing(name = ..)]` offers nothing, a batch's replies never do (they answer many
deliveries at once), and a position offers it once. A slot offers it only when every type in its
marker's `#[publishes(..)]` dictionary leaves its destination open, which means a marker with no
dictionary - `DefaultSlot` included - never does. A slot that gave the right away offers plain
sending only: a transaction and a request / reply round trip do not compile on it.

## First publish at startup (`after_startup`)

A publish that has to happen once, at startup (seeding reference data, announcing the service),
goes through the scope's `after_startup`. It takes two arguments: a **publish policy** and a hook.
The runtime pairs the policy with the connected broker and hands the hook the **live** publisher,
so the seed cannot race the connect. The hook
returns `Result<(), E>` for any `E: std::error::Error + Send + Sync`.

The policy is the broker's own - usually there is nothing to wrap:

```rust
use ruststream::memory::prelude::*;

// in with_broker(broker, |b| { ... }):
b.after_startup(Publish, async move |publisher| {
    // `publisher` is the live MemoryPublisher, already connected
    publisher
        .message(&Region { code: "eu-west" })
        .to("reference.regions")
        .publish()
        .await
});
```

Every `Publisher` gets `.message(&value)` from `PublishExt`, which the prelude carries - a
`Serialize` value encodes with `DefaultCodec`, a `Serialized` value's bytes leave as they are -
so a bare policy is all a hook ever takes. Name a codec at the call, and open a transaction
straight on the publisher:

```rust
use ruststream::codec::CborCodec;

// a codec other than the crate default, named at the publish
b.after_startup(Publish, async move |p| {
    p.message(&Seed { id: 1 }).with_codec(CborCodec).publish().await
});

// an atomic seed: `begin()` opens the scope a transactional publisher owns
b.after_startup(
    TransactionalPublish,
    async move |seeder| seed_events(seeder).await.map_err(std::io::Error::other),
);
```

The hook's error type is a single concrete error, so a body that mixes error types collects them
in one function and the hook maps once, as above. The transaction scope owns the buffer, and both
records become visible together on commit or not at all - a commit without a begin, a second
commit, or a publish after settling do not compile:

```rust
use std::error::Error;

use ruststream::prelude::*;

// The bound names the capability the seeding needs, not the broker's publisher; the same
// capability unlocks `begin()` on an `Out` slot (`Out<impl TransactionalPublisher, Journal>`).
async fn seed_events<P>(seeder: P) -> Result<(), Box<dyn Error + Send + Sync>>
where
    P: TransactionalPublisher,
{
    let scope = seeder.begin().await?;
    scope.message(&Event { id: 1 }).to("events").publish().await?;
    scope.message(&Event { id: 2 }).to("events").publish().await?;
    scope.commit().await?;
    Ok(())
}
```

A policy is named the same way wherever a publisher is attached, and the verb is one:
`.out_reply(..)` for a reply handler, `.out(marker, ..)` for a slot, `.out_retry(..)` for the
deferred `retry_after` copy, `after_startup(policy, hook)` for the first publish.
