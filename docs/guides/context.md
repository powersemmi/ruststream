# Context and state

Everything a handler can reach besides its payload arrives through two objects with different
lifetimes:

| Level | Type | Lives for | Holds |
|---|---|---|---|
| Application | `State` | the whole service | shared resources: pools, clients, configuration |
| Delivery | `Context` | one message | the channel name, a headers working copy, a per-delivery `Extensions` type-map, access to `State` and named publishers |

`State` is filled once, at startup. A `Context` is built fresh for every delivery and threaded as
`&mut` through the middleware chain into the handler, so middleware and the handler observe (and
can enrich) the same per-message view.

## Application level: `State`

`State` is a type-map: one value per type, `Send + Sync`. Two ways to fill it:

- **At build time** with `RustStream::insert_state(value)` - for values that need no async work
  (parsed configuration, a constructed client).
- **In an `on_startup` hook** with `state.insert(value)` - for resources that need `await` to
  create (a database pool). See [Lifespan](lifespan.md) for the hook contract.

Inserting a second value of the same type replaces the first. Handlers read it through the context
with `ctx.state().get::<T>()`, which returns `Option<&T>` - `None` only if nothing of that type was
inserted. The state is shared behind an `Arc` once the service runs, so handlers get cheap shared
references, not copies; interior mutability (an `AtomicU64`, a mutex-guarded map) is the tool when
a shared value must change at runtime.

```rust
--8<-- "examples/context.rs:state"
```

## Delivery level: `Context`

A `#[subscriber]` handler opts in by declaring a second parameter after the payload; omit it when
the handler needs nothing but the message. The macro resolves the type itself, so `Context` needs
no import when it appears only in handler signatures:

```rust
--8<-- "examples/context.rs:handler"
```

What the context exposes:

| Method | Returns | Purpose |
|---|---|---|
| `name()` | `&str` | the channel / subject the message arrived on |
| `headers()` | `&Headers` | the working copy of the message headers |
| `headers_mut()` | `&mut Headers` | the same copy, for middleware to enrich |
| `state()` | `&State` | shared application state (then `.get::<T>()`) |
| `get::<T>()` | `Option<&T>` | a per-delivery [extension](#per-delivery-extensions) |
| `insert::<T>(v)` | `()` | write a per-delivery [extension](#per-delivery-extensions) |
| `publisher(name)` | `Option<ScopedPublisher>` | a [named publisher](publishing.md#publishing-from-inside-a-handler) |
| `after(outcome).then(fut)` | `()` | a [post-settle hook](#post-settle-hooks) gated on the settlement outcome |
| `after_ack(fut)` / `after_settle(fut)` | `()` | post-settle hook sugar (after an ack / after any settlement) |

Closure handlers (the manual `typed(codec, |msg, ctx| ...)` form) always take the context as their
second argument.

## Per-delivery extensions

Beside the shared `State`, the context carries an `Extensions` type-map scoped to one delivery:
`ctx.insert::<T>(value)` writes it, `ctx.get::<T>()` reads it back, and it is dropped when the
handler returns, so one delivery's values never leak into the next. Use it for data that belongs to
a single message rather than the whole service - a correlation id, an authenticated user a layer
resolved, a tracing span.

```rust
--8<-- "examples/context.rs:extensions"
```

A broker can also seed the map: implementing `IncomingMessage::extensions` lets it hand the handler
typed per-delivery values (native delivery metadata, a commit token, a reply-to handle) without
serializing them into the byte-only headers. The runtime moves the broker's map into the context
before the handler runs, and those values stay reachable through the publish pipeline, so a
transactional publisher can read a broker-supplied commit token at commit time.

## The headers working copy

`ctx.headers()` is not the broker message itself: each delivery clones the incoming headers into a
working copy that lives in the context. That makes it a scratchpad for the dispatch chain -
middleware earlier in the chain can stamp values onto it with `headers_mut()`, and the handler
reads the enriched result:

```rust
--8<-- "examples/context.rs:enrich"
```

Mounted globally, the layer runs before every handler, so `handle` above always finds
`x-request-id`:

```rust
--8<-- "examples/context.rs:app"
```

Two boundaries to keep in mind:

- Mutations stay within the delivery: the broker message and other subscribers' deliveries are
  untouched.
- Outgoing messages do not inherit the copy. Replies and manual publishes start from fresh
  headers; attach outgoing metadata in the [publish pipeline](publishing.md#the-publish-pipeline)
  (a `PublishLayer` or `PublishMiddleware`) instead.

## Publishing through the context

`ctx.publisher("name")` resolves a publisher registered with `RustStream::publisher(name, p)` and
returns `None` when nothing is registered under that name. Sends through it run the application's
dynamic publish middleware - the same chain as a macro reply - so a manual publish is not a hole
in the pipeline:

```rust
--8<-- "examples/publishing.rs:forward"
```

A closure handler cannot return a future that borrows the context across `.await`; publish from a
`#[subscriber]` handler or a struct handler with `async fn handle`, both of which own the borrow.
See [Publishing](publishing.md#publishing-from-inside-a-handler).

## Post-settle hooks

Sometimes a handler needs a side effect to fire *after* the message has been settled - a
non-critical notification, slow follow-up work - without it gating the ack decision or affecting
redelivery. Register one on the context:

```rust
--8<-- "examples/context.rs:handler"
```

The handler above ends with `ctx.after_ack(..)`: the continuation runs only once the broker has
acked the message, off the delivery path, so it never delays the ack or the next delivery.

Three forms, all additive:

- `ctx.after(outcome).then(fut)` - runs only if the message settles by `outcome`, matched **by
  kind**. The four kinds are distinct: `Ack`, `drop()` (nack, no requeue), `retry()` (nack,
  requeue), and `retry_after()` (matched regardless of the delay). Drop and retry are separate
  mechanics, so a hook gated on `drop()` does not fire on a `retry()` settlement, and vice versa.
- `ctx.after_ack(fut)` - sugar for `ctx.after(HandlerResult::Ack).then(fut)`.
- `ctx.after_settle(fut)` - runs after the message settles, whatever the outcome.

Multiple registrations accumulate and every matching one runs. The semantics are **at-most-once**:
the message is already settled before any hook runs, so a hook that panics, or that is lost when the
process crashes, never causes a redelivery. A graceful shutdown drains in-flight hooks (bounded by
`shutdown_timeout`); an aborted shutdown may drop them.

On the batch path a `Context` is one per *batch*, so a hook runs after the whole batch has settled.
Because a batch has per-element outcomes, the outcome gate is ill-defined there: only
`after_settle` hooks fire (the gated `after(..)` / `after_ack` forms are ignored on a batch).

## Context in middleware

Every middleware form receives the same `&mut Context` the handler will see, which is what makes
the enrichment pattern work:

- A static layer's `Handler::handle(&self, msg, ctx)` - as in the example above.
- A dynamic `DynMiddleware::handle(&self, input, ctx, next)` - inspect or enrich, then
  `next.run(input, ctx)`.

The middleware forms themselves are covered in [Middleware](middleware.md). The full program for
this page is
[`examples/context.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/context.rs).
