# Context and state

Everything a handler can reach besides its payload arrives through two objects with different
lifetimes:

| Level | Type | Lives for | Holds |
|---|---|---|---|
| Application | the state type `S` | the whole service | shared resources: pools, clients, configuration |
| Delivery | `Context<'_, C, S>` | one message | the channel name, a headers working copy, the broker's typed per-delivery context `C` (read by key), and the typed shared state `S` |

The state is produced once, at startup, and is a single typed value of your own choosing. A
`Context` is built fresh for every delivery and threaded as `&mut` through the middleware chain into
the handler, so middleware and the handler observe (and can enrich) the same per-message view.

## Application level: typed state

The shared application state is one typed value `S` (a struct you define, or `()` when the service
needs none). It is produced by an `on_startup` hook - the value the hook returns becomes the state,
fixing the app's state type:

=== "Macros"

    ```rust
    --8<-- "examples/context.rs:app"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/context.rs:app"
    ```

The state type is checked at compile time: a `#[subscriber]` handler that reads state names it as
the third `Context` generic (`Context<'_, C, S>`), and the runtime only lets that handler mount on
an app whose state type matches. A handler that names no state type is generic over it, so it mounts
on any app. `publish(..)` handlers follow the same rule with one twist: one that ignores the state
omits the `Context` parameter entirely and still mounts on a stateful app, but one that declares a
`Context` without naming a state type pins the state to `()`, so name the app's state type
explicitly to mount such a handler on a stateful app.

Handlers borrow the state with `ctx.state()`, which returns `&S`, the typed state itself - no
lookup and no `Option` to unwrap. The state is shared behind an `Arc` once the service runs, so
handlers get cheap shared references, not copies; interior mutability (an `AtomicU64`, a
mutex-guarded map) is the tool when a shared value must change at runtime. For data scoped to one
message rather than the whole service, use the [per-delivery context](#per-delivery-context)
instead. See [Lifespan](lifespan.md) for the startup-hook contract.

```rust
--8<-- "examples/context.rs:state"
```

## Injecting dependencies: extractor parameters

Reaching for a dependency through `ctx.state().field` always works, but a handler can also take it
as a parameter. Any handler parameter after the message (and the optional `&mut Context`) whose type
implements `FromContext` is an **extractor**: the runtime resolves it from the delivery before the
body runs, and a failed extraction settles the message by the rejection's `HandlerResult` without
running the body.

To inject a piece of the state, derive `FromRef` on the state and take `State<T>` in the handler -
no extractor impl by hand. `State<T>` resolves for any field type (`T: FromRef<S>`), including types
from other crates - a broker publisher, a client pool:

=== "Macros"

    ```rust
    --8<-- "examples/from_context.rs:state"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/from_context.rs:state"
    ```

The handler takes `State<FieldType>`, with no `ctx.state()` reach-through:

=== "Macros"

    ```rust
    --8<-- "examples/from_context.rs:handler"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/from_context.rs:handler"
    ```

A field that should not be injectable, or whose type another field already claims, opts out with
`#[from_ref(skip)]`; two fields may not share a type, since injection by type would be ambiguous. For
a custom extractor that does more than read the state - an auth guard that rejects, a request-scoped
resolver - implement `FromContext` directly: it borrows the `&mut Context`, so it can read headers,
broker fields, or a scratch value a middleware left, and return a `Rejection` to settle the delivery.

## Delivery level: `Context`

A `#[subscriber]` handler opts in by declaring a second parameter after the payload; omit it when
the handler needs nothing but the message. The macro resolves the type itself, so `Context` needs
no import when it appears only in handler signatures:

=== "Macros"

    ```rust
    --8<-- "examples/context.rs:handler"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/context.rs:handler"
    ```

What the context exposes:

| Method | Returns | Purpose |
|---|---|---|
| `name()` | `&str` | the channel / subject the message arrived on |
| `headers()` | `&HeaderMap` | the working copy of the message headers |
| `headers_mut()` | `&mut HeaderMap` | the same copy, for middleware to enrich |
| `state()` | `&S` | the typed shared application state, borrowed directly |
| `context(KEY)` | `KEY::Value` | a [broker field](#per-delivery-context) read by compile-time key |
| `set(KEY, v)` | `()` | write a per-delivery [scratch value](#per-delivery-context) (middleware) |
| `after(outcome).then(fut)` | `()` | a [post-settle hook](#post-settle-hooks) gated on the settlement outcome |
| `after_ack(fut)` / `after_settle(fut)` | `()` | post-settle hook sugar (after an ack / after any settlement) |

Closure handlers (the manual `typed(codec, |msg, ctx| ...)` form) always take the context as their
second argument.

## Per-delivery context

Beside the shared application state, the context carries the broker's typed per-delivery context,
read by **compile-time key** and free on the delivery path. A key is a selector the broker exports;
`ctx.context(KEY)` reads the field straight off the context, so a handler reads native delivery
metadata - a stream id, an offset, a delivery handle - without the broker serializing it into the
byte-only headers. A key the subscription's broker does not carry is a compile error, not a
runtime miss.

```rust
--8<-- "examples/context_field.rs:field"
```

The context type is built from the message by `BuildContext`, which the runtime calls once per
delivery; a broker with no per-delivery fields uses `()`, the default (so a `#[subscriber]` handler
that names no context type - and takes no [`Ctx` extractor](#context-fields-as-parameters) - sees
`Context<'_>`). Middleware can also carry a typed scratch value to a
downstream handler: a writable key (`FieldMut`) lets a layer `ctx.set(KEY, value)` and the handler
`ctx.context(KEY)` it back - a correlation id, an authenticated user a layer resolved - without
serializing it into the headers. The context is built fresh per delivery, so one delivery's values
never leak into the next.

## Context fields as parameters

A field can also arrive as a handler argument, the way `State<T>` injects a state component: the
`Ctx<K>` extractor binds the value the key `K` reads. The handler needs no `&mut Context`
parameter at all: the `#[subscriber]` macro projects the subscription's context type from the
first `Ctx` key in the signature.

```rust
--8<-- "examples/ctx_extractor.rs:key"
```

=== "Macros"

    ```rust
    --8<-- "examples/ctx_extractor.rs:handler"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/ctx_extractor.rs:handler"
    ```

Three things to know:

- Values are owned: an extractor binds before the handler body runs, so it cannot borrow from the
  context. Keys yielding borrowed values (a name as `&str`) stay readable through
  `ctx.context(KEY)` with a declared ctx parameter.
- With a `&mut Context<'_, C>` parameter also present, every `Ctx` key must read that same `C`.
- The projection is syntactic: the macro recognizes the literal `Ctx<K>` shape (any path ending in
  `Ctx` with one type argument). A type alias hides it, and the context type falls back to `()`.

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

=== "Macros"

    ```rust
    --8<-- "examples/context.rs:app"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/context.rs:app"
    ```

Two boundaries to keep in mind:

- Mutations stay within the delivery: the broker message and other subscribers' deliveries are
  untouched.
- Outgoing messages do not inherit the copy. Replies and manual publishes start from fresh
  headers; attach outgoing metadata in the [publish pipeline](publishing.md#the-publish-pipeline)
  (a `PublishTransform` or `PublishLayer`) instead.

## Publishing from a handler

To publish from inside a handler (beyond the `publish(..)` reply form), do not put the publisher
in the state: take it as a handler parameter with `Out` - the pattern
`Out(out): Out<impl Publisher>` binds `out` to a live publisher inside the body. The policy is
attached where the handler is included, the concrete publisher type is inferred from it, and
the runtime pairs it after the broker connects. The full pattern and its snippet live in
[Publishing from inside a handler](publishing.md#publishing-from-inside-a-handler).

## Post-settle hooks

Sometimes a handler needs a side effect to fire *after* the message has been settled - a
non-critical notification, slow follow-up work, a cache warm-up - without it gating the ack
decision or affecting redelivery. Register one on the context:

=== "Macros"

    ```rust
    --8<-- "examples/context.rs:handler"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/context.rs:handler"
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

A handler can also attach a continuation through its return value: any outcome converts into a
`Settle` with `.and_after(fut)`, which is how a batch handler gets per-element continuations. See
[Post-settle continuations](subscribers.md#post-settle-continuations) for that form; the semantics
below apply to both.

Multiple registrations accumulate and every matching one runs, on a tracked task set off the
delivery path. The semantics are **at-most-once**: the message is already settled before any hook
runs, so a hook that panics, or that is lost when the process crashes, never causes a redelivery.
Do not put work whose loss must redeliver the message in a hook; settle by outcome and let the
broker retry instead. A graceful shutdown drains in-flight hooks (bounded by `shutdown_timeout`);
an aborted shutdown may drop them.

On the batch path a `Context` is one per *batch*, so a hook runs after the whole batch has settled.
Because a batch has per-element outcomes, the outcome gate is ill-defined there: only
`after_settle` hooks fire (the gated `after(..)` / `after_ack` forms are ignored on a batch).

## Context in middleware

Every middleware form receives the same `&mut Context` the handler will see:

- A static layer's `Handler::handle(&self, msg, ctx)` - as in the example above.
- A dynamic `DynMiddleware::handle(&self, input, ctx, next)` - inspect or enrich, then
  `next.run(input, ctx)`.

The middleware forms themselves are covered in [Middleware](middleware.md). The full program for
this page is
[`examples/context.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/context.rs).
