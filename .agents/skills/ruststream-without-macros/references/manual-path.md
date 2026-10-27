# The manual path in depth

Each section is the hand-written form of what an attribute or a derive declares. The component pages on <https://powersemmi.github.io/ruststream/latest/components/> show every topic in a Manual tab.

## Contents

- Decoding
- Raw bytes
- Dependencies and state
- Headers
- Publishing
- Middleware
- What the chain reaches, and what it does not

## Decoding

The constructor resolves the decode codec off the mount surface, the same ladder the attribute path
uses: the scope's (`with_broker_codec`), the router chain's (`Router::with_codec`), else
`DefaultCodec`. `.codec(..)` overrides it for one registration, and `.on_failure(..)` carries the
decode-failure policy.

```rust
use ruststream::codec::CborCodec;

// the decode-failure policy, the manual counterpart of `on_failure(decode = ..)`
b.include(
    subscriber("orders", Receive)
        .on_failure(FailurePolicies::default().with_decode(FailurePolicy::Retry))
        .build(),
);

// this registration's own decode codec, overriding the scope's
b.include(subscriber("audit", Receive).codec(CborCodec).build());
```

## Raw bytes

The lane is the payload type's own business, and the trait names are the mnemonic: `Deserialize`
means the framework's codec builds the value, `Deserialized` means the type builds itself from the
delivery's bytes. So the raw input spelling is a named type of the service's own - `Frame<'_>`
below - and the pair of impls `#[derive(Deserialized)]` writes by hand is the construction plus
the `Input` spelling that routes it onto the lane. No codec is involved, and this is the only
subscriber form available with no codec feature at all. The batch spelling (`[Frame<'_>]`) comes
with those two impls; a bare `[u8]` is not an input, because it is also a batch of decoded `u8`
elements and coherence cannot split the two.

```rust
use std::convert::Infallible;

struct Frame<'a>(&'a [u8]);

impl Deserialized for Frame<'_> {
    type Output<'a> = Frame<'a>;
    type Error = Infallible;

    fn from_payload(payload: &[u8]) -> Result<Frame<'_>, Self::Error> {
        Ok(Frame(payload))
    }
}

impl Input for Frame<'_> {
    type Axis = SoloDeserialized<Frame<'static>>;
}

struct Ingest;

impl<'p> Handle<Frame<'p>> for Ingest {
    fn handle(
        &self,
        frame: &Frame<'p>,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        parse(frame.0);
        ready(Ok(()))
    }
}

b.include(subscriber("frames", Ingest).build());
```

A constructor that validates (a flatbuffers root, a length check) returns `Err` and the delivery
settles by the subscriber's `on_failure(decode = ..)` policy - the same rung a codec decode
failure lands on, so that policy is accepted on this lane.

A generated message (Protobuf, Cap'n Proto, FlatBuffers) rides the same lanes with the format's own
functions in the impls, which is all `#[wire(prost)]` and `#[wire(encode = .., decode = ..)]` fill
in. The outgoing half is three impls: `Serialized::wire_bytes` takes the publish path's own
`BytesMut` and answers `WireBytes::InBuffer` where it wrote into it, or `WireBytes::Own(..)` where
the bytes are the value's already, so the message is encoded once and nothing intermediate is
allocated; the two wire spellings say where the value may be used.

```rust
impl Serialized for Order {
    type Error = prost::EncodeError;

    fn wire_bytes(&self, buf: &mut BytesMut) -> Result<WireBytes<'_>, Self::Error> {
        prost::Message::encode(self, &mut *buf)?;
        Ok(WireBytes::InBuffer)
    }
}

impl MessageWire for Order {
    type Wire = SerializedWire;
}

impl ReplyShape for Order {
    type Body = Self;
    type Headers = ();
    type Wire = SerializedReply;
}
```

## Dependencies and state

A closure captures; a named handler holds. Anything the body needs that is fixed at registration
becomes a field, and everything the runtime owns per delivery comes off the context: the state is
the body's last axis, read with `ctx.state()`.

```rust
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Held: fixed at registration. Read off the context: owned by the runtime.
struct Confirm {
    seen: Arc<AtomicU64>,
}

struct AppState {
    receipts: MemoryPublisher,
}

// The state is the body's last axis, so the impl names it and the mount checks it against the
// app's; the reply and injection axes stay at `()` in between.
impl Handle<Order, (), (), (), AppState> for Confirm {
    async fn handle(
        &self,
        order: &Order,
        _outs: &(),
        ctx: &mut Context<'_, (), AppState>,
    ) -> Result<(), HandlerOutcome> {
        self.seen.fetch_add(order.id, Ordering::SeqCst);

        // `Receipt` implements the outgoing traits by hand (see Publishing below), so the
        // state-held publisher uses the same one `message(..)` entry as everything else.
        if ctx
            .state()
            .receipts
            .message(&Receipt { order_id: order.id })
            .to("receipts")
            .publish()
            .await
            .is_err()
        {
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
}

let app = RustStream::new(AppInfo::new("orders", "0.1.0"))
    .on_startup(async move |()| Ok::<_, std::convert::Infallible>(AppState { receipts }))
    .with_broker(MemoryBroker::new(), |b| {
        b.include(subscriber("orders", Confirm { seen }).build());
    });
```

`Context<'_, C, S>` names what the attribute would have inferred: `C` the broker's per-delivery
context, `S` the application state. Both default to `()`, so a handler needing neither writes
`Context<'_>`.

## Headers

`Headers<T>` is an extractor, so the manual path declares the contract on the input instead:
`Message<H, P>` is one delivery's decoded payload next to its decoded header contract, with both
public fields ready when the body runs. The pairing is structural, so two subscribers of one
subject may read different header sets, and a violation settles by the same
`on_failure(decode = ..)` policy that covers the payload.

```rust
impl Handle<Message<ChunkMeta, Chunk>> for Convert {
    async fn handle(
        &self,
        msg: &Message<ChunkMeta, Chunk>,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        tracing::info!(msg.headers.task_id, "converting {}", msg.body.id);
        Ok(())
    }
}

b.include(subscriber("chunks", Convert).build());
```

A batch reads `[Message<H, P>]`, one contract per element. `ctx.headers()` is still the untyped
`HeaderMap` for a discriminator read (`get_str`), `ctx.headers_mut()` is the working copy
middleware enriches, and `HeaderMap::insert_typed(&value)` writes a contract back into it.

## Publishing

`message(&value)` needs `T: OutgoingDestination`, which is what `#[derive(Outgoing)]` implements.
Without the `macros` feature the same impls are written by hand; the caller-named form is three
short ones, and then the value publishes like any declared message:

```rust
impl OutgoingDestination for Receipt {
    // FixedName or NameTemplate for a declared destination, and then the name itself:
    //   type Form = FixedName;
    //   const DESTINATION: &'static str = "receipts";
    // NameTemplate adds PARAMETERS and a TemplateAddress builder, one setter per placeholder.
    type Form = CallerName;
}

impl MessageHeaders for Receipt {
    type Contract = NoHeaders; // WithHeaders<Meta> for a typed header contract
}

impl<M: OutSlot> OutMessages<M> for Receipt {
    fn outgoing() -> Vec<OutgoingMessageMetadata> {
        Vec::new() // a declared type reports one entry here; the document reads it
    }
}

publisher.message(&Receipt { order_id: 7 }).to("receipts").publish().await?;
```

A set of message models a handler receives on one subscription (what `#[derive(InMessages)]`
declares) is two impls on the enum: `InMessages`, with the rule written out in `decode` (each
model through `<Model as InVariant<Set>>::decode_variant`, so a `serde` model takes the codec and
a `Deserialized` one its own reader) and one `IncomingMessageMetadata` per model in `incoming()`
(`<Model as InVariant<Set>>::describe()`, plus `with_discriminator` under a header rule), and
`impl Input for Set { type Axis = SoloSet<Set>; }`. The handler is an `impl Handle<Set>` that
matches on the enum.

A slot marker is the same kind of declaration: `impl OutSlot` names the marker's `NAME`, its
`Destination` offer (`Reads` unless the slot may name a destination) and the dictionary its
`outgoing()` reports, and one `impl PublishedThrough<Marker> for Msg` per listed type is the
membership the typed publish checks. A payload that already is bytes adds the `Serialized` and
wire impls from [Raw bytes](#raw-bytes) and rides the same `message(..)` entry with no codec on
the path.

The mount chain, `after_startup`, publish transforms and `publish_layer` are all runtime API
and work unchanged. The reply clause becomes the body's `R` axis plus the one `.reply()` on the
chain - `.to(dest)` names the destination the reply type does not declare itself - and the wire
follows the reply *type*, not the chain: both wires name the policy at the mount site with
`.out_reply(policy)` (or nothing, for the broker's default), and what differs is what chains
after it. A `serde::Serialize`
`R` encodes through the codec, so it takes `.codec(..)`, `.transform(..)` and `.transactional()`;
an `R` implementing `Serialized` (the impls above; `ReplyShape` with `type Wire = SerializedReply`
covers the reply position) leaves byte-for-byte, so those steps do not compile on it. There is one
`.reply()` and one `.out_reply(..)` for both wires. A batch reply always publishes through the
reply codec; the `Serialized` wire covers single replies. A reply type that declares its own
destination takes the bare `.reply()`, exactly as the attribute's bare `reply` clause does.

The `Out` slot parameter becomes the `O` axis: `Outs<(E,)>` with one type parameter per marker,
each bounded `E: OutEntry<Marker, Wire: Capability>`. The body states the marker and the broker
capability it needs of the wired value, and nothing else: the codec the include site gave the slot
and the publish path it composed (its `.transform(..)` steps over the app's own `publish_layer`)
stay behind the bound, which is what lets one body mount under any of them.

```rust
impl<M> Handle<Order, (), Outs<(M,)>> for Mirror
where
    M: OutEntry<Primary, Wire: Publisher>,
{
    async fn handle(
        &self,
        order: &Order,
        outs: &Outs<(M,)>,
        _ctx: &mut Context<'_>,
    ) -> Result<(), HandlerOutcome> {
        outs.get(Primary)
            .message(&Event { id: order.id })
            .to("mirror")
            .publish()
            .await
            .map_err(|_| HandlerOutcome::retry())?;
        Ok(())
    }
}

// the reply chain seals with `.build()`, and the include site binds the arena
b.include(subscriber("orders", Confirm).reply().to("confirmations").build());
b.include(subscriber("mirror", Mirror).build())
    .out(Primary, Publish)
    .build();
```

The markers come from the impl, so a missing, duplicate or extra `.out(..)` is a compile error
naming the slot, and a single unnamed slot binds through the implicit `DefaultSlot`
(`.out(DefaultSlot, policy).build()`). `outs.get(Marker)` hands back the entry itself: it keeps the
`.message(..)` builder and is a transparent `Deref` onto the wired live value, so a broker-defined
capability is called straight on it. A trait bound does not travel through that `Deref`, so a
helper generic over such a capability works only where the broker crate grafted a blanket impl onto
`Slot<M, W, E, Pipe, Body>`.

## Middleware

A layer is the same named-type shape one level up: `Layer<H>` wraps an inner handler, and the
wrapper implements `Handler` by delegating. This is the form the bundled layers already use, so
nothing here is macro-dependent.

```rust
struct Counted<H> {
    inner: H,
    count: Arc<AtomicU64>,
}

impl<M, C, S, H> Handler<M, C, S> for Counted<H>
where
    M: Sync,
    C: Send,
    S: Send + Sync,
    H: Handler<M, C, S>,
{
    async fn handle(&self, msg: &M, ctx: &mut Context<'_, C, S>) -> HandlerOutcome {
        self.count.fetch_add(1, Ordering::SeqCst);
        self.inner.handle(msg, ctx).await
    }
}
```

## What the chain reaches, and what it does not

Every form is reachable through the one constructor. The application state, the typed broker
context and the injections are axes of the body's own `impl Handle`; the reply and document steps
ride the chain (`.reply()` / `.to(..)`, `.describe(..)`, `.undocumented()`) next to the declarative
settings the attribute path shares, the mandatory batch size `.batch(n)` among them. Registrations
are documented by default under `asyncapi`, which is why message types need `schemars::JsonSchema`
and `.undocumented()` is the per-registration exit.

The definition traits behind `subscriber(..)` are crate-internal, so a service writes no definition
type of its own: a computed
source is a value handed to `subscriber(..)`, computed settings are calls on the chain, and a
description is `.describe(..)`. The settings state rides in the builder's type (`Open` per setting
still free, `Fixed` per setting already named), which is what makes naming one twice a compile
error rather than a silent override.

A `Router` adds two steps of its own, each applying to the registration just included:
`.workers(Workers::pool(n))` / `Workers::keyed(n)` / `Workers::threads(n)` /
`Workers::threads_keyed(n)` is the functional counterpart of the `workers(..)` and `threads(..)`
clauses, and `.layer(..)` wraps that one registration outside its decode step.
`Router::with_codec` sets the decode codec of everything included after it.

Read the expansion in `crates/ruststream-macros/src/expand/unified.rs` when you need the exact
mapping from a handler shape to the axes and the form marker: it is the only place the whole table
is written down.

Enabling `macros` remains the shorter road, and the two styles compose in one application: a
macro-free registration and an `include(def)` sit in the same `with_broker` scope.
