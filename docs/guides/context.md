# Context and state

Everything a handler can reach besides its payload arrives through two objects with different
lifetimes:

| Level | Type | Lives for | Holds |
|---|---|---|---|
| Application | `State` | the whole service | shared resources: pools, clients, configuration |
| Delivery | `Context` | one message | the channel name, a headers working copy, access to `State` and named publishers |

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
with `ctx.get::<T>()`, which returns `Option<&T>` - `None` only if nothing of that type was
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
| `get::<T>()` | `Option<&T>` | shared application state |
| `publisher(name)` | `Option<ScopedPublisher>` | a [named publisher](publishing.md#publishing-from-inside-a-handler) |

Closure handlers (the manual `typed(codec, |msg, ctx| ...)` form) always take the context as their
second argument.

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

## Context in middleware

Every middleware form receives the same `&mut Context` the handler will see, which is what makes
the enrichment pattern work:

- A static layer's `Handler::handle(&self, msg, ctx)` - as in the example above.
- A dynamic `DynMiddleware::handle(&self, input, ctx, next)` - inspect or enrich, then
  `next.run(input, ctx)`.

The middleware forms themselves are covered in [Middleware](middleware.md). The full program for
this page is
[`examples/context.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/context.rs).
