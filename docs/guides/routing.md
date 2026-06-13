# Routing

As a service grows, handlers move out of `main.rs` into their own modules. A `Router` collects a
module's handlers into one mountable group; `include_router` mounts the whole group on a broker
scope.

## Building a router

A `Router` mirrors the broker scope: alongside `include` / `include_on` and `include_publishing` /
`include_publishing_on` it has `with_codec` (switches the chain's decode codec, see
[Codecs](codecs.md#per-handler)) and the manual `handle` / `subscribe` registrations. Every call
consumes the router and returns a new one, so registrations chain:

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

Handlers that publish a reply register on the router the same way as on the scope, with a
`TypedPublisher` built from the broker:

```rust title="routes.rs"
--8<-- "examples/tutorial/routes.rs:routes"
```

## Router middleware

The application's global middleware (added with `RustStream::layer`) wraps router handlers too:
`include_router` applies the app stack around each handler when the router is mounted. A layer used
this way must be a `BlanketLayer` - the router hides its handlers' concrete types, so the wrap
happens through one generic method; every bundled layer qualifies.

```rust title="main.rs"
--8<-- "examples/logging_middleware.rs:layered_router"
```

A router can also carry its own stack: `Router::layer` wraps every handler in that router when it
is mounted, inside the app's global stack (scopes nest, app outermost):

```rust title="routes.rs"
--8<-- "examples/middleware_router_scope.rs:router_scope"
```

See [Middleware](middleware.md) for what a layer is and how to write one, and
[`examples/logging_middleware.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/logging_middleware.rs)
for the app-scope side in a running service.

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
