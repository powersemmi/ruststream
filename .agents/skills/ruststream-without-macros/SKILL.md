---
name: ruststream-without-macros
description: "Use when a RustStream service is built without the `macros` feature, or when code must spell out what `#[subscriber]`, `#[ruststream::app]` and the derives generate: `Handle` impls, `subscriber(..).build()`, a hand-written `main`, manual `Outgoing`, `OutSlot`, `Deserialized` and `Serialized` impls."
---

# RustStream without the macros

The attribute macros are sugar over public runtime API: a service without the `macros` feature
loses no capability, only the attribute surface. The handler body is a named type implementing
`Handle`, a registration is `subscriber(source, body)` plus settings and `.build()`, and `main`
is yours. This guidance targets ruststream 0.7. For everything the two styles share (outcomes,
the mount chain, publish policies, codecs, state) use the `ruststream` skill; for tests the
`ruststream-testing` skill.

Depth: [references/manual-path.md](references/manual-path.md) covers decoding, raw bytes,
state, headers, publishing, `Out` slots, middleware and what the chain reaches.

## A whole service

```rust
use std::convert::Infallible;
use std::error::Error;
use std::future::{Future, ready};

// Without `macros` the glob still brings the builder, `Handle`, `subscriber` and the traits the
// hand-written impls name.
use ruststream::memory::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

#[derive(Debug, Serialize)]
struct Confirmation {
    id: u64,
}

// What `#[derive(Outgoing)]` would write: no declared name, so `.to(..)` on the chain names it.
impl OutgoingDestination for Confirmation {
    type Form = CallerName;
}

impl MessageHeaders for Confirmation {
    type Contract = NoHeaders;
}

/// The handler body: the axes are the input, the reply, the injections, the context and the state.
struct Confirm;

impl Handle<Order, Confirmation> for Confirm {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<Confirmation, HandlerOutcome>> {
        tracing::info!(order.id, "confirming");
        ready(Ok(Confirmation { id: order.id }))
    }
}

/// Raw bytes are a named type that builds itself from the payload; `[u8]` is not an input.
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

struct Audit;

impl<'p> Handle<Frame<'p>> for Audit {
    fn handle(
        &self,
        frame: &Frame<'p>,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(len = frame.0.len(), "audit frame");
        ready(Ok(()))
    }
}

fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(subscriber("orders", Confirm).reply().to("confirmations").build())
            .include(subscriber("audit", Audit).build())
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    app().run().await?;
    Ok(())
}
```

## Rules

### The body is a named type; the signature decides the form

`Handle<In, R = (), O = (), C = (), S = ()>`: `In` is the input (`T` decoded, a `Deserialized`
type, `Message<H, T>` with typed headers, `[T]` for a batch), `R` the reply, `O` the `Out` slots
(`Outs<(E,)>`), `C` the broker context, `S` the application state read with `ctx.state()`. A
closure is not a body: what it would capture becomes a field of the type.

### The verdict is a `Result`, one spelling per shape

| Shape | Return |
|---|---|
| one message, no reply | `Result<(), HandlerOutcome>` (`Ok(())` acks) |
| one message, reply | `Result<Reply, HandlerOutcome>` |
| batch | `Result<(), Vec<HandlerOutcome>>` |
| batch with replies | `Result<Vec<Reply>, Vec<HandlerOutcome>>` |

Post-settle work rides the error side: `Err(HandlerOutcome::ack().and_after(fut))`.

### Register with `subscriber(..)` and seal with `.build()`

`b.include(subscriber("orders", Confirm).workers(nonzero!(4)).build())`. Settings, `.reply()`,
`.to(dest)`, `.describe(..)` and `.undocumented()` ride the chain before `.build()`; publish
policies (`.out_reply(..)`, `.out(Marker, ..)`) follow `include` exactly as on the macro path.
`Router::handle` and `BrokerScope::handle` are runtime machinery that skips the scope codec and the
settings; a service mounts through `include`.

### Keep anything that can panic inside the future

The dispatcher calls `handle(..)` and only then guards the returned future. A panic before the
future exists escapes the `on_failure(panic = ..)` policy and kills the task. A body that awaits
is written `async fn handle`; one with nothing to await returns `ready(..)`.

### `main` is yours

`#[tokio::main]` plus `app().run().await`. `run_until(future)` stops on a future you own;
`start()` returns a running handle for hosting beside another server. The attribute's command
line (`run`, `asyncapi gen`) is what the macro path adds.

## Common mistakes

| Symptom | Fix |
|---|---|
| `async fn on_order(order: Order) -> Confirmation` passed to a builder | a type implementing `Handle<Order, Confirmation>` |
| `app.subscriber("orders").handle(f)` | `b.include(subscriber("orders", Body).build())` inside `with_broker` |
| `RustStream::new(broker)` | `RustStream::new(AppInfo::new(name, version)).with_broker(broker, \|b\| { .. })` |
| raw bytes as `Bytes` or `&[u8]` | a `Deserialized` newtype plus its `Input` impl |
| `fn app() -> RustStream` with state or layers | `fn app() -> impl App`: state and layers change the type |
| a reply type with only `Serialize` | add `OutgoingDestination` and `MessageHeaders` impls |

The exact mapping from a handler shape to its axes is the macro expansion in
`crates/ruststream-macros/src/expand/unified.rs` of the core repository. Component pages show
every topic with a Manual tab: <https://powersemmi.github.io/ruststream/latest/components/>.
