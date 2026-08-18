# Routing

As a service grows, handlers move out of `main.rs` into their own modules. A `Router` collects a
module's handlers into one mountable group; `include_router` mounts the whole group on a broker
scope.

## Building a router

A `Router` mirrors the broker scope: `include` and `include_batch` mount every definition form,
picked by the definition itself, next to `with_codec` (switches the chain's decode codec, see
[Codecs](codecs.md#per-handler)) and the manual `handle` / `subscribe` registrations. The
subscription source always comes from the definition - `#[subscriber(..)]` takes the broker's own
source expression, builder chain included - so there is nothing to name at the mount site. Every
call consumes the router and returns a new one, so registrations chain:

```rust title="routes.rs"
use ruststream::runtime::Router;

--8<-- "examples/routing.rs:builders"
```

<!-- inline-rust: minimal mount fragment with placeholder routes module; the full compiled program is examples/routing.rs (merge form pulled in below) -->
```rust title="main.rs"
RustStream::new(info).with_broker(broker, |b| {
    b.include_router(routes::orders());
});
```

Handlers that need an attachment - a reply publisher, an
[`Out`](publishing.md#publishing-from-inside-a-handler) slot - register on the router the same way
as on the scope, except that the registration commits
through an explicit terminal: `.publisher(policy)` names the wiring, `.mount()` takes the broker's
own default publish policy, and `.out(marker, policy)` binds one named slot before `.mount()`. A
consuming builder cannot commit when it goes out of scope the way the scope's does, since dropping
it cannot hand back the router the registration grew into - so a forgotten terminal never becomes a
router, and the chain fails to compile. The policies stay pure declaration, so the router still
needs no broker:

```rust title="routes.rs"
--8<-- "examples/tutorial/routes.rs:routes"
```

## Router middleware

A router can carry its own layer stack: `Router::layer` wraps every handler in that router when it
is mounted. The application's global stack (added with `RustStream::layer`) wraps around it at
`include_router` - scopes nest, app outermost:

```rust title="main.rs"
--8<-- "examples/logging_middleware.rs:layered_router"
```

Because a router hides its handlers' concrete types, a layer reaching them must be a
`BlanketLayer`. Both scopes, the `BlanketLayer` requirement, and writing your own layer are covered
in [Middleware](middleware.md#middleware-scopes).

## Composing and mounting

Build routers per module, then combine them however suits the service:

<!-- inline-rust: illustrative multi-router composition with placeholder route modules; the compiled merge form is examples/routing.rs:merge, pulled in below -->
```rust
// Mount several routers on one broker - include_router can be called more than once.
RustStream::new(info).with_broker(broker, |b| {
    b.include_router(routes::orders());
    b.include_router(routes::shipping());
});
```

Or merge groups into one router before mounting (the whole program is
[`examples/routing.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/routing.rs)):

```rust
--8<-- "examples/routing.rs:merge"
```

`merge` appends another router's registrations in order. Each router keeps its own codec and layer
stack; when the result is mounted, the outer router's layers (and the app's global stack) wrap
around the merged router's own.

## Next

- The handler contract and the `#[subscriber]` macro: [Subscribers](subscribers.md).
- How the decode codec is resolved for `include`: [Codecs](codecs.md).
