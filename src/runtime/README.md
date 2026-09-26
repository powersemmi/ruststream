The application object, the handler surface, and dispatch.

A service is one [`RustStream`] value. Hooks and middleware go on it first, then each broker
is registered with [`with_broker`](RustStream::with_broker), and handlers are mounted inside
that scope with `include`. [`run`](RustStream::run) owns the process, and the
[`#[ruststream::app]`](macro@crate::app) attribute writes the `main` that calls it (see
[`cli`]). The sections below show the attribute form of each feature; the manual form is
under [Macro or manual](#macro-or-manual).

# Subscribers

A handler is an `async fn` whose first parameter is a reference to the decoded payload: `&T`
handles one message, `&[T]` a whole batch. An optional `&mut Context` comes second. Any
further parameter is an extractor the runtime resolves before the body runs: [`State<T>`]
for a field of the shared state, [`Ctx<K>`] for a broker per-delivery field, [`Headers<T>`]
for the typed header contract, or a type of your own implementing [`FromContext`]. A
parameter written `Out(out): Out<impl Publisher>` is not an extractor but an injected
publisher, described under [Publishing](#publishing).

```
# #[cfg(all(feature = "macros", feature = "memory", feature = "json"))]
# mod demo {
use ruststream::memory::prelude::*;
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u64,
}

#[derive(Clone)]
struct Region(String);

#[derive(Clone, FromRef)]
struct AppState {
    region: Region,
}

#[subscriber("orders", workers(4))]
async fn handle(
    order: &Order,
    ctx: &mut Context<'_, (), AppState>,
    State(region): State<Region>,
) -> HandlerOutcome {
    if ctx.headers().get("x-dry-run").is_some() {
        return HandlerOutcome::ack();
    }
    println!("order {} for {} on {}", order.id, region.0, ctx.name());
    HandlerOutcome::ack()
}
# }
# fn main() {}
```

What the body returns settles the delivery. [`HandlerOutcome::ack`],
[`retry`](HandlerOutcome::retry), [`retry_after`](HandlerOutcome::retry_after) and
[`drop`](HandlerOutcome::drop) are the four settlements. `()` acks; `Result<(), E>` acks on
`Ok` and drops on `Err`; `Result<HandlerOutcome, E>` returns the inner outcome on `Ok`.
[`and_after`](HandlerOutcome::and_after) attaches a continuation that runs once the
settlement is through, at most once and off the delivery path. On the message itself `ack`
consumes `self`, so acking twice does not compile.

## The subscription source

The attribute fixes the *kind* of subscription and may leave its *value* to the mount site:

| Form | Kind | Value |
|---|---|---|
| `#[subscriber]` | by name | named at the mount site |
| `#[subscriber(RedisStream)]` | the broker's descriptor | named at the mount site |
| `#[subscriber("orders")]` | by name | fixed here |
| `#[subscriber(RedisStream::new("orders").group("w"))]` | the broker's descriptor | fixed here |

The by-name form works on any broker implementing [`Subscribe`](crate::Subscribe), which maps
the name to its default subscription kind. A descriptor is a type from the broker crate
implementing [`SubscriptionSource`](crate::SubscriptionSource). The macro reads its type off
the constructor call, so the chain ends in `Type::new(..)` and every step returns `Self`. A
kind that needs more than a name (a Pulsar topic and subscription) implements no
[`FromName`](crate::FromName) and is written out in full.

## Settings at the mount site

Name, workers, failure policies, batch size and start position are values, given in the
attribute or at the mount site through [`SubscriberSettings`]. A setting the attribute named
is fixed in the definition's type, so naming it again does not compile. Broker-specific
settings (a consumer group, a durable name) chain in the broker's own vocabulary on the same
builder, and the broker crate documents them.

```
# #[cfg(all(feature = "macros", feature = "memory", feature = "json"))]
# mod demo {
use ruststream::memory::prelude::*;
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u64,
}

/// Everything is left to the mount site: the name, the pool, the failure policy.
#[subscriber]
async fn bill(order: &Order) -> HandlerOutcome {
    HandlerOutcome::ack()
}

fn app(shard: u32) -> RustStream {
    RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(
            bill.name(format!("orders-{shard}"))
                .workers(nonzero!(4))
                .on_failure(FailurePolicies::default().with_decode(FailurePolicy::Skip)),
        );
    })
}
# }
# fn main() {}
```

## Batches

A `&[T]` parameter makes the handler a batch handler: it runs once per batch the broker
delivers, and the mount site names the one batch parameter the framework carries down,
`batch(n)`. Mounting a batch handler without it does not compile. The returned value settles
the whole batch; return `Vec<HandlerOutcome>` to settle element `i` with outcome `i`. An
element that fails to decode is settled by the decode policy on its own and never reaches
the body. A batch body gets one context per batch, holding the broker's subscription-scoped
fields only; per-delivery data such as headers rides the elements as `&[Message<H, T>]`.
App-wide and router middleware wrap per-message handlers and do not apply to batch
registrations.

```
# #[cfg(all(feature = "macros", feature = "memory", feature = "json"))]
# mod demo {
use ruststream::memory::prelude::*;
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u64,
}

/// One database round-trip per batch; entries that are not ready come back alone.
#[subscriber("orders")]
async fn reconcile(orders: &[Order]) -> Vec<HandlerOutcome> {
    orders
        .iter()
        .map(|order| {
            if order.id == 0 {
                HandlerOutcome::retry()
            } else {
                HandlerOutcome::ack()
            }
        })
        .collect()
}

fn app() -> RustStream {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(reconcile.batch(nonzero!(64)));
    })
}
# }
# fn main() {}
```

## Payloads that decode themselves

A newtype over `&'a [u8]` deriving [`Deserialized`](macro@crate::Deserialized) takes the
codec out of the path: a `&Frame<'_>` parameter receives the delivery's bytes as the broker
handed them over, and `&[Frame<'_>]` is the batch form. A bare `&[u8]` parameter does not
compile. A constructor that rejects the bytes settles the delivery by the decode policy. The
same mnemonic holds on the way out: a reply or a published value deriving
[`Serialized`](macro@crate::Serialized) produces its own bytes and meets no codec. The
[`codec`](crate::codec) module has the lanes in full, `#[wire(prost)]` included.

## Workers

Dispatch is sequential per subscriber: a delivery is settled before the next one is pulled.
`workers(n)` handles up to `n` deliveries at once on `n` long-lived workers, tasks of the
runtime the app runs on, and the stream is not polled while `n` are in flight. Global order is
lost by design. `workers(n, by_key)` makes the same workers `n` sequential lanes by the
message's partition key ([`Partitioned`](crate::Partitioned)), so messages sharing a key never
reorder. The workers start with the subscription, so a delivery costs its handoff and no
allocation. Batch forms take a plain pool of batches, each batch in a task of its own. On shutdown the workers in flight finish within the
[`shutdown_timeout`](RustStream::shutdown_timeout). A panic outside the handler's future (in a
manual handler's `handle` call, say) ends its worker, and the subscription stops dispatching,
as the sequential loop does.

## Delayed redelivery and its cap

[`HandlerOutcome::retry_after`] asks for the delivery back no sooner than a delay. A broker
with native delayed redelivery gets the delay. On any other the runtime publishes a copy back
to the subscription after the delay and drops the original, with the framework's
[`RETRY_COUNT_HEADER`] incremented; that copy is at most once over the delay window. A graceful
shutdown waits for a pending copy within the [`shutdown_timeout`](RustStream::shutdown_timeout),
so it is published before the broker closes. The timeout is unset by default, so a shutdown
waits out the longest pending delay; set one when delays run long. A zero delay publishes the
copy at once, on the dispatch path, as `retry()` does. The
publisher it leaves through is on every registration already, from the broker's default
policy. `.out_retry(policy)` replaces it, and the steps after it are the slot steps,
`.codec(..)` and `.transform(..)`. The copy lends its bytes rather than handing them over, so
a broker that keeps owned bytes copies once here: this path runs only after a delivery has
failed, and lending costs the runtime nothing on the path that has not. The copy goes where the subscription says it is reached
again ([`RedeliveryAddressed`](crate::RedeliveryAddressed)). A subscription reading many
destinations ([`NamedCopies`](crate::NamedCopies)) makes the mount site name one with
`.to(name)`, or a transform names it per delivery. A name given with `.to(name)` is a channel
the registration sends to, and the generated document reports it; the address a subscription
takes its own copies back at is not reported, being the subscription's own channel.

Two steps right after `include` end a message that keeps coming back. `max_attempts(n)`
counts the deliveries one message gets, and `dead_letter(name)` is where a spent delivery is
republished as it arrived. A cap without a destination rejects the delivery instead. The
count is the broker's own where the transport keeps one and the framework's header where it
keeps none, never both, and an immediate `retry()` obeys the same cap. Where the broker moves
the delivery itself (a queue with a delivery limit and a dead-letter exchange), the
declaration reaches the descriptor, `.out_retry(..)` does not compile, and a `retry()` stays
the broker's own requeue, cap or no cap.

```
# #[cfg(all(feature = "macros", feature = "memory", feature = "json"))]
# mod demo {
use std::time::Duration;

use ruststream::memory::prelude::*;
use serde::Deserialize;

#[derive(Deserialize)]
struct Payment {
    settled: bool,
}

#[subscriber("payments")]
async fn reconcile(payment: &Payment) -> HandlerOutcome {
    if !payment.settled {
        return HandlerOutcome::retry_after(Duration::from_secs(5));
    }
    HandlerOutcome::ack()
}

fn app() -> RustStream {
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(reconcile)
            .max_attempts(nonzero!(5u32))
            .dead_letter("payments.dead");
    })
}
# }
# fn main() {}
```

## Seeking

A broker over a replayable log implements [`Seekable`](crate::Seekable) and owns the
position type. Where a subscription opens is a property of the mount: the
`start_at(<position>)` clause, or [`start_at`](SubscriberSettings::start_at) at the mount
site. A handler repositions its own subscription through the broker's context keys, read
with `Ctx<K>` or `ctx.context(KEY)`. The in-memory broker keeps a log only when built with
[`MemoryBroker::retaining`](crate::memory::MemoryBroker::retaining); on a broker without a
log these mounts do not compile.

## Macro or manual

The attribute is sugar over a generic API. The body goes into an `impl Handle<Order>` of a
named type, [`subscriber(source, body)`](subscriber) binds it to a source, and `.build()`
closes the chain. The chain takes the same settings as the attribute's clauses, and with the
`asyncapi` feature `.describe(..)` and `.undocumented()` control the document. The manual
form is for a handler that needs state the attribute cannot express, and for a build without
the `macros` feature.

```
# #[cfg(all(feature = "memory", feature = "json", feature = "asyncapi"))]
# mod demo {
use std::future::{Future, ready};

use ruststream::memory::prelude::*;
use serde::Deserialize;

// A manual registration is documented by default, which is where the schema derive is owed.
#[derive(Deserialize, ruststream::schemars::JsonSchema)]
struct Order {
    id: u64,
}

struct Inline;

impl Handle<Order> for Inline {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        println!("got order {}", order.id);
        ready(Ok(()))
    }
}

fn app() -> RustStream {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(subscriber("orders", Inline).build());
    })
}
# }
# fn main() {}
```

# Publishing

A handler publishes in two ways. Returning a value is the reply form, the right default when
the handler answers on one destination. Taking a publisher as an `Out` parameter is for a
destination decided at run time, or several destinations at once, in other brokers included.
Either way the handler never sees an unconnected publisher: the mount site names a
[`PublishPolicy`](crate::PublishPolicy), and startup pairs it with the connected broker.

## Replies

Every reply type derives [`Outgoing`](macro@crate::Outgoing). `#[outgoing(name = "..")]` on
the type is the destination, and the clause is then the bare `publish`; a type declaring no
name takes one from the mount site, `publish("dest")`. `Result<Reply, HandlerOutcome>` keeps
the acknowledgement in hand: `Ok` publishes and acks, `Err` publishes nothing and settles by
the outcome. A reply publish that fails nacks the delivery with requeue, so a replying
handler is idempotent under redelivery.

With nothing else said the reply leaves through the broker's default policy under the
default codec. `.out_reply(policy)` names the policy, and the steps after it fill the wiring:
`.codec(..)` for the reply codec (the request still decodes with the scope's),
`.transform(..)` for a publish transform, `.transactional()` for one broker transaction per
batch of replies.

What the encoded reply costs is the broker's own declaration ([`Publisher::Payload`](crate::Publisher::Payload)): a
broker that reads the payload is lent the dispatch loop's own buffer, so replying allocates
nothing per delivery, and one whose client keeps the payload is handed the buffer the codec
wrote.

```
# #[cfg(all(feature = "macros", feature = "memory", feature = "json"))]
# mod demo {
use ruststream::memory::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct Request {
    id: u64,
}

/// The reply type fixes its destination, so the clause names none.
#[derive(Serialize, Outgoing)]
#[outgoing(name = "receipts")]
struct Receipt {
    id: u64,
}

#[subscriber("requests", publish)]
async fn issue(req: &Request) -> Result<Receipt, HandlerOutcome> {
    if req.id == 0 {
        return Err(HandlerOutcome::drop());
    }
    Ok(Receipt { id: req.id })
}

fn app() -> RustStream {
    RustStream::new(AppInfo::new("receipts", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(issue).out_reply(Publish);
    })
}
# }
# fn main() {}
```

## `Out` slots

`Out(out): Out<impl Publisher>` binds a live publisher inside the body. The bound names a
capability ([`Publisher`](crate::Publisher),
[`TransactionalPublisher`](crate::TransactionalPublisher),
[`OwnedTransactions`](crate::OwnedTransactions), [`RequestReply`](crate::RequestReply)) and
never a broker type, so the same handler mounts on a production broker and on its in-process
test transport. A handler with several publishers names a slot marker per parameter: a unit
struct deriving [`OutSlot`](crate::OutSlot), whose `#[publishes(..)]` list is what the slot
may send. The mount site binds each marker with `.out(marker, policy)` and commits with
`.build()`; a single unnamed slot is [`DefaultSlot`]. A forgotten binding, a marker the
handler does not declare, and a publish of a type outside the list do not compile. The
optional third position, `Out<impl Publisher, Marker, (A, B)>`, narrows what this handler
publishes.

A publish is one builder: `message(&value)`, then the positions the type leaves open, then
`publish()`. A fixed `#[outgoing(name = "..")]` needs no `.to(..)`; a template such as
`"orders.{tenant}.placed"` opens `.to()` with one setter per placeholder; a type without a
name takes `.to("dest")`. A type declaring `headers = Meta` publishes only with
`.with_headers(&meta)`. `.with_codec(..)` names a codec for one call, and a
[`Serialized`](macro@crate::Serialized) value is sent byte for byte with no codec position.

```
# #[cfg(all(feature = "macros", feature = "memory", feature = "json"))]
# mod demo {
use ruststream::memory::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders.confirmed")]
struct Confirmed {
    id: u64,
}

#[derive(Serialize, Outgoing)]
struct Audit {
    id: u64,
}

#[derive(OutSlot)]
#[publishes(Confirmed)]
struct Orders;

#[derive(OutSlot)]
#[publishes(Audit)]
struct Trail;

#[subscriber("orders.incoming")]
async fn route(
    event: &Confirmed,
    Out(orders): Out<impl Publisher, Orders>,
    Out(trail): Out<impl Publisher, Trail>,
) -> HandlerOutcome {
    // `Confirmed` fixes its destination; `Audit` leaves it to the call.
    if orders.message(event).publish().await.is_err()
        || trail
            .message(&Audit { id: event.id })
            .to("audit")
            .publish()
            .await
            .is_err()
    {
        return HandlerOutcome::retry();
    }
    HandlerOutcome::ack()
}

fn app() -> RustStream {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(route)
            .out(Orders, Publish)
            .out(Trail, Publish)
            .build();
    })
}
# }
# fn main() {}
```

## Headers and per-message settings

Outgoing headers are assembled once: the publisher's
[`base_headers`](crate::Publisher::base_headers) first, then the call site's over them, key
by key. A reply gets the same base without writing anything. The headers working copy on the
[`Context`] never reaches an outgoing message. A broker's per-message settings (a `QoS`, a
priority, an ordering key) are [`Publisher::Options`](crate::Publisher::Options): the policy
holds the defaults, a step the broker adds to the builder changes one for a call, and a
transform adjusts them where there is no call site. A body that adjusts one bounds
`Out<impl Publisher<Options = ..>, _>` and imports that broker's prelude.

## The publish pipeline

A [`PublishTransform`] is static and belongs to one position, chained with `.transform(..)`
after the `.out(..)` that named it. An impl declares three things: what it reads (the kind:
[`ForReply<C>`](ForReply) sees the delivery being answered through [`PublishContext`],
[`ForSlot`] sees the slot's name through [`SlotContext`]), which settings it writes (the
`Options` parameter, `()` or the broker's type), and `type Destination`: [`Reads`] leaves the
destination alone, [`Names`] sets it. A position offers `Names` only where nothing declared
the destination, and once; elsewhere the `.transform(..)` call does not compile. A name a
transform reads off the delivery - the queue a request asked to be answered on - reaches
[`Outgoing::set_name`] as the delivery's own buffer, through
[`HeaderMap::get_shared`](crate::HeaderMap::get_shared) and [`Str`](crate::Str), so naming a
destination per message allocates nothing. A
[`PublishLayer`] is app-wide, added with [`publish_layer`](RustStream::publish_layer), and
wraps every publish a handler makes around the send. The order is a position's transforms as
written, then the app-wide layers, then the send. Batch replies take `.batch_transform(..)`,
and [`for_batch`] lifts a per-message transform into it.

```
# #[cfg(all(feature = "macros", feature = "memory", feature = "json"))]
# mod demo {
use ruststream::memory::prelude::*;
use ruststream::runtime::{Outgoing, PublishContext};
use serde::{Deserialize, Serialize};

/// Carries the request's correlation id onto its reply. `ForReply` reads the delivery being
/// answered, so this transform mounts on a reply position and nowhere else.
struct Correlate;

impl<C, Options> PublishTransform<ForReply<C>, Options> for Correlate {
    type Destination = Reads;

    fn apply(
        &self,
        out: &mut Outgoing<'_>,
        _options: &mut Option<Options>,
        cx: &PublishContext<'_, C>,
    ) {
        if let Some(id) = cx.headers().correlation_id() {
            out.headers_mut().insert("correlation-id", id.to_owned());
        }
    }
}

# #[derive(Deserialize)]
# struct Request {
#     id: u64,
# }
# #[derive(Serialize, Outgoing)]
# struct Response {
#     ok: bool,
# }
#[subscriber("requests", publish("responses"))]
async fn respond(req: &Request) -> Response {
    Response { ok: req.id != 0 }
}

fn app() -> RustStream {
    RustStream::new(AppInfo::new("responder", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(respond).out_reply(Publish).transform(Correlate);
    })
}
# }
# fn main() {}
```

## Transactions

A batch reply mounted with `.transactional()` publishes every reply inside one broker
transaction and acks the batch after the commit; a policy whose publisher has no
transactions does not compile there. By hand, `begin()` on a
[`TransactionalPublisher`](crate::TransactionalPublisher) opens a [`TransactionScope`] that
owns the transaction: publishes go through the scope, and `commit()` or `abort()` consumes
it, so a publish after settling does not compile. Brokers whose transactions are client
buffers also implement [`OwnedTransactions`](crate::OwnedTransactions): `owned_transaction()`
returns a [`TypedTransaction`] of its own, any number at a time. A slot bound with
`Out<impl TransactionalPublisher, Ledger>` opens the same scope from the body, admitting what
the slot admits. Publishing many messages in a loop is the batch publish: the clients
coalesce writes, and a broker with a pipeline primitive exposes it in its own crate.

## Another broker's publisher

To consume from one broker and publish to another, wrap the target with
[`bindable`](crate::Broker::bindable) and mint a [`Bound`] token with [`bind`](Bindable::bind)
before registration. The token is then the policy at the mount site (`.out(marker, token)`,
`.out_reply(token)`), and the same wrapper is what `with_broker` registers. Outside a
registration, [`RunningApp::publisher`] pairs a token once startup has connected its broker.

# Routing

A [`Router`] collects one module's handlers, and [`include_router`](BrokerScope::include_router)
mounts the group on a broker scope. `include` is the one entry point, and
[`with_codec`](Router::with_codec) switches the decode codec for the registrations that
follow. A registration with a publish position commits with `.build()`, and its policies stay
pure declaration, so a router needs no broker. [`Router::layer`] gives the router its own
middleware, and [`merge`](Router::merge) appends another router's registrations.

```
# #[cfg(all(feature = "macros", feature = "memory", feature = "json"))]
# mod demo {
use ruststream::memory::prelude::*;
use serde::Deserialize;

# #[derive(Deserialize)]
# struct Order {
#     id: u64,
# }
# #[derive(Deserialize)]
# struct Shipment {
#     order_id: u64,
# }
#[subscriber("orders")]
async fn accept(order: &Order) -> HandlerOutcome {
    HandlerOutcome::ack()
}

#[subscriber("shipments")]
async fn dispatch(shipment: &Shipment) -> HandlerOutcome {
    HandlerOutcome::ack()
}

fn orders() -> Router<MemoryBroker, impl RouterDef<MemoryBroker>> {
    Router::new().include(accept)
}

fn shipping() -> Router<MemoryBroker, impl RouterDef<MemoryBroker>> {
    Router::new().include(dispatch)
}

fn app() -> RustStream {
    RustStream::new(AppInfo::new("routing", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include_router(orders().merge(shipping()));
    })
}
# }
# fn main() {}
```

# Context and state

The shared state is one typed value produced by [`on_startup`](RustStream::on_startup), or
`()` when the service needs none. A handler reading it names the state type as the last
parameter of [`Context`] (`Context<'_, C, S>`) and mounts only on an app with that state; one
naming no state type mounts anywhere. Deriving [`FromRef`](macro@crate::FromRef) on the state
lets a handler take any field as [`State<T>`]. The [`Context`] is built per delivery and
carries the channel name, a working copy of the headers that middleware may enrich, the
broker's typed per-delivery fields read by key with [`context`](Context::context), a scratch
slot middleware writes with [`set`](Context::set), and the post-settle hooks:
[`after_ack`](Context::after_ack), [`after_settle`](Context::after_settle) and
[`after(outcome).then(..)`](Context::after) run off the delivery path once the message has
settled, at most once. The headers cost what is read: the delivery is asked for its map by the
first [`headers`](Context::headers) call and by nothing else, so a handler over a decoded payload
never reaches the broker's accessor, and the map is copied by the first
[`headers_mut`](Context::headers_mut). A [`Ctx<K>`] parameter binds one broker field without the context
parameter, and a key the broker does not have is a compile error.

```
# #[cfg(all(feature = "macros", feature = "memory", feature = "json"))]
# mod demo {
use std::convert::Infallible;

use ruststream::memory::prelude::*;
use serde::Deserialize;

# #[derive(Deserialize)]
# struct Order {
#     id: u64,
# }
struct Config {
    reject_zero_ids: bool,
}

#[subscriber("orders")]
async fn handle(order: &Order, ctx: &mut Context<'_, (), Config>) -> HandlerOutcome {
    if ctx.state().reject_zero_ids && order.id == 0 {
        return HandlerOutcome::drop();
    }
    let id = order.id;
    ctx.after_ack(async move { println!("order {id} acked") });
    HandlerOutcome::ack()
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(Config { reject_zero_ids: true }))
        .with_broker(MemoryBroker::new(), |b| {
            b.include(handle);
        })
}
# }
# fn main() {}
```

# Typed headers

A header contract is a flat `serde` struct with scalar fields; every value travels as a
string, parsed on the way in and written back on the way out. [`Headers<T>`] parses the
delivery's headers before the body runs, and a missing or unparsable header settles the
delivery by the decode policy. In a batch the headers stay per delivery, so the input is
`&[Message<H, T>]`. On the way out `#[outgoing(headers = Meta)]` makes the contract part of
the type: the builder compiles only with `.with_headers(&meta)`, and a transform writes a
contract into a map with [`HeaderMap::insert_typed`](crate::HeaderMap::insert_typed). The
generated document carries the contract as a headers schema.

```
# #[cfg(all(feature = "macros", feature = "memory", feature = "json"))]
# mod demo {
use ruststream::memory::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct ChunkMeta {
    task_id: u64,
    chunk_no: u32,
}

#[derive(Serialize, Outgoing)]
#[outgoing(name = "chunks.done", headers = ChunkMeta)]
struct ChunkDone {
    output_key: String,
}

#[derive(OutSlot)]
#[publishes(ChunkDone)]
struct Events;

#[derive(Deserialized)]
struct Chunk<'a>(&'a [u8]);

#[subscriber("chunks.raw")]
async fn convert(
    chunk: &Chunk<'_>,
    Headers(meta): Headers<ChunkMeta>,
    Out(events): Out<impl Publisher, Events>,
) -> HandlerOutcome {
    let done = ChunkDone {
        output_key: format!("chunks/{}/{}.part", meta.task_id, meta.chunk_no),
    };
    if events.message(&done).with_headers(&meta).publish().await.is_err() {
        return HandlerOutcome::retry();
    }
    let _ = chunk.0.len();
    HandlerOutcome::ack()
}
# }
# fn main() {}
```

# Lifecycle

Four hooks bracket the run loop:

```text
on_startup(prev) -> S      before brokers connect: build async resources, produce the state
  -> brokers connect, subscriptions open
after_startup(Arc<S>)      handlers are live: signal readiness, publish a first message
  ... running ...
  -> shutdown triggered (a signal, or the run_until future)
on_shutdown(Arc<S>)        brokers still connected
  -> brokers shut down, in-flight handlers drained
after_shutdown(Arc<S>)     final teardown
```

[`on_startup`](RustStream::on_startup) takes the previous state by value (`()` first) and
returns the new one, so its future owns a pool across awaits. It comes before the first
`with_broker`, because handlers are registered against the state type it produces. The later
hooks receive the state as `Arc<S>`. A first publish belongs in the scope-level
[`after_startup`](BrokerScope::after_startup): it runs once subscriptions are open, and the
hook receives a live publisher paired from the policy it names. A startup hook's error aborts
the service; a shutdown hook's error is logged, so shutdown runs to completion.
[`shutdown_timeout`](RustStream::shutdown_timeout) bounds the drain of in-flight handlers and
post-settle continuations. [`run`](RustStream::run) installs the signal handlers and owns the
process; [`run_until`](RustStream::run_until) stops on a future of yours;
[`start`](RustStream::start) resolves once subscriptions are open and hands the rest of the
lifecycle to a [`RunningApp`].

```
# #[cfg(all(feature = "macros", feature = "memory", feature = "json"))]
# mod demo {
use std::sync::Arc;
use std::time::Duration;

use ruststream::memory::prelude::*;
use serde::Deserialize;

# #[derive(Deserialize)]
# struct Order {
#     id: u64,
# }
/// A stand-in for a connection pool.
struct Database;

impl Database {
    async fn connect(url: &str) -> Result<Self, std::io::Error> {
        let _ = url;
        Ok(Self)
    }

    async fn close(&self) {}
}

#[subscriber("orders")]
async fn handle(order: &Order, ctx: &mut Context<'_, (), Database>) -> HandlerOutcome {
    let _db: &Database = ctx.state();
    HandlerOutcome::ack()
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .on_startup(async move |()| Database::connect("postgres://localhost/orders").await)
        .after_shutdown(async move |db: Arc<Database>| {
            db.close().await;
            Ok::<_, std::io::Error>(())
        })
        .shutdown_timeout(Duration::from_secs(10))
        .with_broker(MemoryBroker::new(), |b| {
            b.include(handle);
        })
}
# }
# fn main() {}
```

# Middleware

A layer transforms one handler into another: implement [`Layer<H>`](Layer) and a [`Handler`]
for the wrapper, which receives the same `&mut Context` the handler gets. Three scopes take a
layer. [`RustStream::layer`], before `with_broker`, wraps every handler of the application;
[`Router::layer`] wraps every handler of that router; `.layer(..)` after an `include` on a
router wraps one registration, outside its decode step. The first two hide the handlers'
concrete types, so their layers implement [`BlanketLayer`] as well; a registration keeps a
concrete type, so a plain `Layer<H>` is enough there. Static layers cost nothing on the hot
path. A chain assembled at run time goes into a [`DynStack`] of [`DynMiddleware`] and rides
one registration. [`layers::TracingLayer`] is the bundled one. Batch registrations are not
wrapped by the application or router stacks. The publish side has its own [`PublishLayer`].

```
# #[cfg(all(feature = "macros", feature = "memory", feature = "json"))]
# mod demo {
use ruststream::memory::prelude::*;
use ruststream::runtime::{BlanketLayer, Handler, Identity, Layer, Stack};
use serde::Deserialize;

#[derive(Clone)]
struct LogLayer;

struct Logged<H>(H);

impl<H> Layer<H> for LogLayer {
    type Handler = Logged<H>;

    fn layer(&self, inner: H) -> Logged<H> {
        Logged(inner)
    }
}

impl<M, C, S, H> Handler<M, C, S> for Logged<H>
where
    M: Send + Sync,
    C: Send,
    S: Send + Sync,
    H: Handler<M, C, S>,
{
    async fn handle(&self, msg: &M, ctx: &mut Context<'_, C, S>) -> HandlerOutcome {
        println!("-> {}", ctx.name());
        let outcome = self.0.handle(msg, ctx).await;
        println!("<- {}", ctx.name());
        outcome
    }
}

// The application stack wraps handlers whose concrete type the mount site hides.
impl BlanketLayer for LogLayer {
    fn apply<M, C, S, H>(&self, handler: H) -> impl Handler<M, C, S> + 'static
    where
        M: Send + Sync + 'static,
        C: Send + 'static,
        S: Send + Sync + 'static,
        H: Handler<M, C, S> + 'static,
    {
        Logged(handler)
    }
}

# #[derive(Deserialize)]
# struct Order {
#     id: u64,
# }
# #[subscriber("orders")]
# async fn handle(order: &Order) -> HandlerOutcome {
#     HandlerOutcome::ack()
# }
fn app() -> RustStream<Stack<LogLayer, Identity>> {
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .layer(LogLayer)
        .with_broker(MemoryBroker::new(), |b| {
            b.include(handle);
        })
}
# }
# fn main() {}
```

# Failure policy

A delivery goes unhandled when the body panics or the payload does not decode, and
`on_failure(panic = .., decode = ..)` sets what happens then, per subscriber
([`FailurePolicies`] at the mount site). The defaults differ. A panic is a bug, so
`panic = fail_fast` logs, starts a graceful shutdown and makes `run` return an error for the
orchestrator to restart on. A decode failure is bad input, so `decode = drop` nacks the one
message without requeue and keeps consuming. The values are [`FailurePolicy::FailFast`],
[`Drop`](FailurePolicy::Drop), [`Retry`](FailurePolicy::Retry),
[`RetryAfter`](FailurePolicy::RetryAfter) and [`Skip`](FailurePolicy::Skip), which acks the
failed message to move past it. The decode key also settles a violated header contract and a
self-decoding payload that rejects its bytes. A panic is caught with `catch_unwind`, so a
build with `panic = "abort"` applies no panic policy at all.

```
# #[cfg(all(feature = "macros", feature = "memory", feature = "json"))]
# mod demo {
use ruststream::memory::prelude::*;
use serde::Deserialize;

# #[derive(Deserialize)]
# struct Order {
#     id: u64,
# }
/// An untrusted topic: a malformed message is requeued, a handler bug still stops the service.
#[subscriber("ingest", on_failure(panic = fail_fast, decode = retry))]
async fn ingest(order: &Order) -> HandlerOutcome {
    HandlerOutcome::ack()
}
# }
# fn main() {}
```

# Running beside another server

A service sharing its process with an HTTP framework starts the messaging side with
[`start`](RustStream::start). It performs the same startup and resolves once subscriptions
are open, so a startup error surfaces before the host accepts traffic, and it installs no
signal handlers. The [`RunningApp`] it returns has [`stopping`](RunningApp::stopping), a
future for the host's graceful shutdown that resolves when the messaging side stopped itself
on a fail-fast failure; [`health`](RunningApp::health), a cloneable [`HealthProbe`] for a
liveness route; [`publisher`](RunningApp::publisher), which pairs a token minted before
registration so a request handler or a relay task holds a live publisher; and
[`shutdown`](RunningApp::shutdown), the explicit graceful teardown. Publishing on the request
path couples the response to the broker, and a database write beside it opens a gap a crash
falls into. The transactional outbox closes it: the endpoint records the event next to the
write, and a relay publishes it afterwards. `examples/http_outbox.rs` in the repository is
that pattern on axum.

```no_run
# #[cfg(all(feature = "memory", feature = "json"))]
# async fn run() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
use ruststream::memory::prelude::*;

let broker = MemoryBroker::new().bindable();
let egress = broker.bind(Publish);
let app = RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |_b| {});

let running = app.start().await?;
let publisher = running.publisher(egress).await?;
// hand `publisher` to the HTTP state or a relay task, serve until `running.stopping()`
// resolves or the host stops ...
# let _ = publisher;
running.shutdown().await?;
# Ok(())
# }
```
