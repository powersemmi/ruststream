# Context and state

Everything a handler can reach besides its payload arrives through two objects with different
lifetimes:

| Level | Type | Lives for | Holds |
|---|---|---|---|
| Application | the state type `S` | the whole service | shared resources: pools, clients, configuration |
| Delivery | `Context<'_, C, S>` | one message | the channel name, a headers working copy, the broker's typed per-delivery context `C` (read by key), and the typed shared state `S` |

The state is produced once, at startup. Its type is yours to choose. A `Context` is built fresh
for every delivery and passed as `&mut` through the middleware chain into the handler. Middleware
and the handler work with the same object, so the handler sees everything middleware put into it.

## Application level: typed state

The shared application state is one typed value `S`: a struct you define, or `()` when the service
needs none. An `on_startup` hook produces it, and the type the hook returns becomes the app's state
type.

=== "Macros"

    ```rust
    --8<-- "examples/context.rs:app"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/context.rs:app"
    ```

The state type is checked at compile time. A handler that reads the state names the state type as
the last type parameter of `Context` (`Context<'_, C, S>`) and mounts only on an app with that
same state type. A handler that names no state type is generic over it and mounts on any app.

`publish(..)` handlers follow the same rule, with one special case. One that does not read the
state omits the `Context` parameter entirely and still mounts on a stateful app. One that declares a
`Context` without naming a state type pins the state to `()`. Name the app's state type explicitly
to mount such a handler on a stateful app.

A handler reads the state with `ctx.state()`, which returns `&S`. The state is in shared ownership,
so every handler works with one value. A value that has to change while the service runs is kept
behind interior mutability: an `AtomicU64`, a map under a mutex.

Data scoped to one message rather than the whole service lives in the
[per-delivery context](#per-delivery-context). The startup hook is described in
[Lifespan](lifespan.md).

```rust
--8<-- "examples/context.rs:state"
```

## Injecting dependencies: extractor parameters

A dependency is always reachable through `ctx.state().field`, but a handler can also take it as a
parameter.

An **extractor** is a handler parameter whose type implements `FromContext`. Extractors come after
the message and after the optional `&mut Context` parameter. The runtime resolves an extractor from
the delivery before the handler body runs. An extractor that returns a rejection settles the message
by its outcome, and the body does not run.

To inject a piece of the state, derive `FromRef` on the state and take `State<T>` in the handler.
`State<T>` works with a field of any type (`T: FromRef<S>`), including a type from another crate,
such as a broker publisher or a client pool:

=== "Macros"

    ```rust
    --8<-- "examples/from_context.rs:state"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/from_context.rs:state"
    ```

The handler takes `State<FieldType>` as a parameter:

=== "Macros"

    ```rust
    --8<-- "examples/from_context.rs:handler"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/from_context.rs:handler"
    ```

Two state fields cannot share a type, because injection goes by type. With `#[from_ref(skip)]` you
can exclude a field: one you do not want injected, or one whose type another field already claims.

An extractor that does more than read the state is one you write yourself, through `FromContext`:
an access check that rejects the delivery, a lookup scoped to one request. Such an extractor
receives the `&mut Context` and can read headers, broker fields, or a scratch value middleware left.
It returns a `Rejection` to settle the delivery.

## Delivery level: `Context`

A `#[subscriber]` handler declares the context as a second parameter after the payload; a handler
that needs nothing but the message leaves it out. The macro fills in the type, so `Context` needs no
import while it appears only in handler signatures:

=== "Macros"

    ```rust
    --8<-- "examples/context.rs:handler"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/context.rs:handler"
    ```

What the context gives you:

| Method | Returns | Purpose |
|---|---|---|
| `name()` | `&str` | the channel or subject the message arrived on |
| `headers()` | `&HeaderMap` | the working copy of the message headers |
| `headers_mut()` | `&mut HeaderMap` | the same copy, for middleware to enrich |
| `state()` | `&S` | the typed shared application state |
| `context(KEY)` | `KEY::Value` | a [broker field](#per-delivery-context) read by compile-time key |
| `set(KEY, v)` | `()` | a per-delivery [scratch value](#per-delivery-context), for middleware |
| `after(outcome).then(fut)` | `()` | a [post-settle hook](#post-settle-hooks) selected by the settlement outcome |
| `after_ack(fut)` / `after_settle(fut)` | `()` | sugar: a hook after an ack, a hook after any settlement |

## Per-delivery context

Besides the shared application state, the context holds the broker's typed per-delivery context: the
delivery's own metadata, such as a stream id, an offset, a delivery handle.

A handler reads them by **compile-time key**. A key is a selector the broker exports;
`ctx.context(KEY)` returns the field straight from the context, with no serializing into the
byte-only headers. Such a read costs nothing on the delivery path. A key the subscription's broker
does not have is a compile error.

```rust
--8<-- "examples/context_field.rs:field"
```

A broker with no per-delivery fields has `()` as its context type, the default: a handler that names
no context type and takes no [`Ctx` extractor](#context-fields-as-parameters) sees `Context<'_>`.

Middleware can pass a typed scratch value to a handler further down the chain: a correlation id, a
user a layer authenticated. On a writable key (`FieldMut`) the layer calls `ctx.set(KEY, value)`,
and the handler reads the value back with `ctx.context(KEY)`. The next delivery does not see these
values.

A [batch handler](subscribers.md#batch-subscribers) gets one context per batch, and it holds the
broker's *subscription-scoped* fields only: a seek handle, a stream name. Per-delivery data stays
out: a batch spans many deliveries, so a position or a header is stored on its elements instead.

A batch body names that type as its context type (`ctx: &mut Context<'_, MemoryBatchContext>` on
the in-memory broker) and reads the fields with `ctx.context(..)`. The per-delivery and batch
context types are distinct, so a batch body that asks for the per-delivery one does not compile. A
broker with no subscription-scoped fields has `()` as its batch context type.

## Context fields as parameters

A context field can also arrive as a handler argument, the way `State<T>` injects a piece of the
state: the `Ctx<K>` extractor binds the value the key `K` reads. The `&mut Context` parameter is
then unnecessary: the `#[subscriber]` macro derives the subscription's context type from the first
`Ctx` key in the signature.

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

Three properties of this form:

- Values arrive owned: the extractor binds before the handler body runs and cannot borrow from the
  context. A key that yields a borrowed value (a name as `&str`) is read through
  `ctx.context(KEY)`, with the `ctx` parameter declared.
- If the handler also takes a `&mut Context<'_, C>` parameter, every `Ctx` key must read that same
  `C`.
- The type is derived syntactically: the macro recognizes the written form `Ctx<K>`, any path
  ending in `Ctx` with one type argument. Behind a type alias the macro does not see that form, and
  the context type becomes `()`.

## The headers working copy

Every delivery copies the incoming headers into the context, and `ctx.headers()` returns that copy,
not the headers of the broker message itself. The copy is a scratchpad for the whole dispatch
chain: middleware earlier in the chain can write values into it with `headers_mut()`, and the
handler reads the result:

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

The copy has two boundaries:

- Changes stay inside the delivery: the broker message and other subscribers' deliveries do not see
  them.
- Outgoing messages do not get this copy: a reply and a manual publish start from empty headers.
  Metadata for an outgoing message is set in the
  [publish pipeline](publishing.md#the-publish-pipeline), through a `PublishTransform` or a
  `PublishLayer`.

## Publishing from a handler

Besides the `publish(..)` reply form, a handler can publish through an `Out` slot. The publisher
for that is not kept in the state but taken as a handler parameter: the pattern
`Out(out): Out<impl Publisher>` binds `out` to a live publisher inside the body.

You name the policy where you include the handler in the app. The concrete publisher type is
inferred from it, and the policy instantiates the publisher on the connected broker.

Everything published through the slot goes through the same
[publish pipeline](publishing.md#the-publish-pipeline) as a reply: the app-wide `publish_layer`
chain, under the slot's own `.out(marker, policy).transform(..)` steps. The full pattern with its
code example is in
[Publishing from inside a handler](publishing.md#publishing-from-inside-a-handler).

## Post-settle hooks

On the context you can register a side effect that runs *after* the message has been settled: a
non-critical notification, slow follow-up work, a cache warm-up. Such an effect does not influence
the ack decision or redelivery.

=== "Macros"

    ```rust
    --8<-- "examples/context.rs:handler"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/context.rs:handler"
    ```

The handler above ends with `ctx.after_ack(..)`. The continuation runs only after the broker has
acked the message, and off the delivery path, so it delays neither the ack nor the next delivery.

Three forms, all additive:

- `ctx.after(outcome).then(fut)` - runs only if the message settles by `outcome`. Outcomes are
  matched **by kind**, and there are four kinds: `ack()`, `drop()` (nack, no requeue), `retry()`
  (nack, requeue) and `retry_after()` (matched at any delay). A hook on `drop()` does not fire when
  the message settles by `retry()`, and the other way round.
- `ctx.after_ack(fut)` - sugar for `ctx.after(HandlerOutcome::ack()).then(fut)`.
- `ctx.after_settle(fut)` - runs after the message settles, whatever the outcome.

A continuation can also be attached to the return value: `.and_after(fut)` exists on any outcome,
and that is how a batch handler gets a continuation per element. That form is described in
[Post-settle continuations](subscribers.md#post-settle-continuations); everything below applies to
both.

Registrations accumulate, and every matching one runs, off the delivery path in a tracked task set.

The semantics are **at-most-once**: the message settles before any hook starts, so a panic in a
hook, or a crash of the process, does not cause a redelivery. Do not put work into a hook when its
loss must redeliver the message: settle the message by the right outcome and let the broker retry.

A graceful shutdown waits for in-flight hooks within `shutdown_timeout`; an aborted shutdown may
lose them.

On the batch path a `Context` is one per *batch*, so a hook runs after the whole batch has settled.
A batch has an outcome per element, so selection by outcome is undefined there: only `after_settle`
hooks fire, and `after(..)` and `after_ack` are ignored on a batch.

## Context in middleware

Every middleware form receives the same `&mut Context` the handler gets:

- A static layer - through `Handler::handle(&self, msg, ctx)`, as in the example above.
- A dynamic middleware - through `DynMiddleware::handle(&self, input, ctx, next)`: it reads or
  enriches the context, then calls `next.run(input, ctx)`.

The middleware forms themselves are covered in [Middleware](middleware.md). The full program for
this page is
[`examples/context.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/context.rs).
