---
name: ruststream
description: "Use when writing, changing or debugging a service built on the RustStream messaging framework (crates `ruststream` and `ruststream-*` broker crates such as nats, rdkafka, lapin, fred): subscriber handlers, the app, replies and publishing, acks, retries and dead letters, state, codecs, batches, workers and threads, routers, middleware, running beside an HTTP server, and the framework's compile errors. Use it whenever the project depends on `ruststream`."
---

# Writing a RustStream service

RustStream is an async messaging framework for Rust. A handler is an `async fn` under
`#[subscriber(..)]`; the app is a synchronous builder under `#[ruststream::app]`; the broker
crate decides the transport. Misuse is a compile error that names the fix, so read the compiler
message before changing the design.

This guidance targets ruststream 0.7. Check the version in `Cargo.toml` before applying it to
another line. Deeper material:

- [references/handlers.md](references/handlers.md): outcomes, retries and dead letters, `and_after`, mount-site settings (`workers`, `threads`, `batch`, `start_at`, `on_failure`), extractors, raw payloads
- [references/publishing.md](references/publishing.md): replies, `Out` slots, destinations, per-message broker settings, publish transforms, the first publish at startup, transactions
- [references/app.md](references/app.md): the app builder, state, codecs, routers, middleware, broker descriptors, AsyncAPI

For tests, use the `ruststream-testing` skill.

## A whole service

```rust
use std::convert::Infallible;
use std::io;
use std::time::Duration;

// The broker's prelude: the broker, its descriptors, its `Publish` policy and the core prelude.
use ruststream::memory::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
    amount_cents: u64,
}

// A reply type derives `Outgoing`; `#[outgoing(name = ..)]` fixes where it goes.
#[derive(Debug, Serialize, Outgoing)]
#[outgoing(name = "receipts")]
struct Receipt {
    order_id: u64,
}

#[derive(Debug, Serialize, Outgoing)]
#[outgoing(name = "audit")]
struct AuditEntry {
    order_id: u64,
    action: String,
}

// One marker per publish path; the list is what may leave through it.
#[derive(OutSlot)]
#[publishes(AuditEntry)]
struct Audit;

#[derive(Clone)]
struct Db;

impl Db {
    async fn store(&self, _order: &Order) -> io::Result<()> {
        Ok(())
    }
}

// Every field becomes injectable as `State<FieldType>`.
#[derive(FromRef)]
struct AppState {
    db: Db,
}

#[subscriber("orders.created", reply)]
async fn place(
    order: &Order,
    State(db): State<Db>,
    Out(audit): Out<impl Publisher, Audit>,
) -> Result<Receipt, HandlerOutcome> {
    if db.store(order).await.is_err() {
        // Not ready yet: come back in 30 seconds instead of spinning.
        return Err(HandlerOutcome::retry_after(Duration::from_secs(30)));
    }
    let entry = AuditEntry { order_id: order.id, action: "stored".to_owned() };
    if audit.message(&entry).publish().await.is_err() {
        return Err(HandlerOutcome::retry());
    }
    tracing::info!(order.id, order.amount_cents, "order stored");
    Ok(Receipt { order_id: order.id })
}

#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .on_startup(async move |()| Ok::<_, Infallible>(AppState { db: Db }))
        .with_broker(MemoryBroker::new(), |b| {
            b.include(place)
                .max_attempts(nonzero!(5u32))
                .dead_letter("orders.dead")
                .out(Audit, Publish)
        })
}
```

`#[ruststream::app]` writes `main`: `cargo run -- run` starts the service and
`cargo run -- asyncapi gen` prints its AsyncAPI document. On NATS the only change is the glob and
the broker value: `use ruststream_nats::prelude::*;` and `NatsBroker::new("nats://localhost:4222")`.
The handlers stay as they are.

## Rules that the compiler does not explain

### Construct brokers synchronously and let the runtime connect

The builder is synchronous. A broker's `new(..)` records configuration only; the runtime connects
after the builder ran. Async setup (a database pool) goes into `on_startup`.

Wrong:

```rust
#[ruststream::app]
async fn app() -> RustStream {
    let db = Db::connect(url).await.expect("db"); // the builder is not async
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(NatsBroker::new(url), |b| b.include(..))
}
```

Correct: `fn app()`, and `.on_startup(async move |()| Ok::<_, DbError>(AppState { db: Db::connect(url).await? }))`.
Never write `#[tokio::main]` beside `#[ruststream::app]`.

### Import one glob

A mount site imports the broker's prelude (`use ruststream_nats::prelude::*;`,
`use ruststream::memory::prelude::*;`). A file holding only handler bodies imports
`use ruststream::prelude::*;`. Name codecs (`ruststream::codec`) and the optional feature modules
(`testing`, `asyncapi`, `metrics`, `logging`, `otel`) explicitly. Everything else, `Router`,
`DefaultSlot`, `FailurePolicies`, `nonzero!` and the derives included, rides the glob.

### The first parameter's type picks the form

`&T` is one message, `&[T]` a batch. `T: Deserialize` decodes with the codec. A type with
`#[derive(Deserialized)]` builds itself from the bytes. An enum with `#[derive(InMessages)]`
receives several message types on one subscription, one model per variant, picked by a header
(`#[in_messages(header = "..")]`, `#[in_messages(value = "..")]`). A bare `&[u8]` or an owned
`T` does not compile. Further parameters are extractors (`State<T>`, `Ctx<K>`, `Headers<T>`) or injections
(`Out<impl Publisher, Marker>`). An explicit `ctx: &mut Context` comes right after the payload,
and only when the body uses it.

### The return type settles the delivery

`HandlerOutcome::ack()`, `retry()` (requeue), `drop()` (no requeue) and `retry_after(delay)`.
`()` always acks; `Result<_, E>` drops on `Err`. A reply handler returns the reply, or
`Result<Reply, HandlerOutcome>` to choose the settlement. A batch returns `Vec<HandlerOutcome>`,
one per element. There is no `ack()` call on a message and no `Retry` type.

### Mount with `include`, and name policies only at the mount site

`b.include(handler)` is the one mounting call, and the `with_broker` closure returns the chain it
opens: `|b| b.include(a).include(b)`. The cap and the dead letter follow `include`:
`.max_attempts(nonzero!(5u32)).dead_letter("orders.dead")`. Publish policies are named after it:
`.out_reply(policy)`, `.out(Marker, policy)`, `.out_retry(policy)`, and the next `.include(..)`
follows directly. Chain nothing and the broker's default policy is used. A broker the service only
publishes to returns the scope as it is, `|b| b`. `b.after_startup(..)` and
`b.include_router(r)` are steps of the same chain, written before the `include`s. A handler signature never names a broker type.

### Where a message goes belongs to its type

A reply or a published type derives `Outgoing`. `#[outgoing(name = "receipts")]` fixes the
destination and the call site writes no `.to(..)`; a type naming nothing takes
`reply("dest")` in the attribute or `.to("dest")` at the call. A template
(`"orders.{tenant}.placed"`) gets one setter per placeholder: `.to().tenant("acme")`.

### CPU work goes on `threads(n)`, I/O concurrency on `workers(n)`

`#[subscriber("images.resize", threads(8))]` gives a computing handler eight dedicated threads.
Do not call `spawn_blocking` or `rayon` inside a handler. `workers(16)` is concurrency for handlers
that wait on I/O. A batch size is mount-site only: `b.include(flush.batch(nonzero!(64)))`.

### The codec is a property of the mount site

JSON is the default (`json` > `cbor` > `msgpack` by feature). Change it for a scope with
`with_broker_codec(broker, CborCodec, |b| ..)` or for a group with
`Router::new().with_codec(CborCodec)` (a function returning that router names the codec in its
type: `Router<MemoryBroker, impl RouterDef<MemoryBroker>, CborCodec>`). A message type never
carries a codec, and an attribute has no `codec(..)` clause.

## Common mistakes

| Symptom | Fix |
|---|---|
| `async fn app()` or `#[tokio::main]` | `#[ruststream::app] fn app() -> impl App`; async setup in `on_startup` |
| `RustStream::new(broker)` | `RustStream::new(AppInfo::new(name, version)).with_broker(broker, \|b\| b.include(..))` |
| `msg.ack().await` in a handler | return a `HandlerOutcome` |
| `.max_attempts(5)` | `.max_attempts(nonzero!(5u32))` |
| `.dead_letter(NatsPublish::new(..))` | `.dead_letter("orders.dead")`: a destination name |
| `Inject<Db>`, `.state(db)` | `#[derive(FromRef)]` state from `on_startup`, `State<Db>` |
| `audit: Out<Audit>` | `Out(audit): Out<impl Publisher, Audit>` and `.out(Audit, Publish)` |
| `\|b\| { b.include(h); }` | `\|b\| b.include(h)`: the closure returns the chain |
| `#[subscriber("t", batch(64))]` | `b.include(handler.batch(nonzero!(64)))` |
| `println!` | `tracing` |

## Where the knowledge lives

- Components (one page per topic): <https://powersemmi.github.io/ruststream/latest/components/>
- Build a service, step by step: <https://powersemmi.github.io/ruststream/latest/guides/build-a-service/>
- API reference: <https://docs.rs/ruststream>; a broker's own settings live on its crate's docs.rs page
