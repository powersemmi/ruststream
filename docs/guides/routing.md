# Routing

As a service grows, handlers move out of `main.rs` into their own modules. A `Router` collects a
module's handlers into one mountable group; `include_router` mounts the whole group on a broker
scope.

## Building a router

A `Router` mirrors the broker scope: alongside `include` and `include_publishing` it has
`with_codec` (a per-handler codec, see [Codecs](codecs.md#per-handler)) and the manual `handle` /
`subscribe` registrations:

```rust title="routes.rs"
use ruststream::runtime::Router;

--8<-- "examples/routing.rs:builders"
```

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

The application's global middleware (added with `RustStream::layer`) does not wrap router handlers,
since a router is finalized independently. Give the router its own stack with `Router::layer`,
applied to every handler registered after it (the same composition as `RustStream::layer`). It
changes the router's type, so let the builder function's return type follow from it:

```rust title="routes.rs"
use ruststream::runtime::{Identity, Router, Stack};
use ruststream::runtime::layers::TracingLayer;

--8<-- "examples/logging_middleware.rs:layered_router"
```

See [Middleware](middleware.md) for what a layer is and how to write one, and
[`examples/logging_middleware.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/logging_middleware.rs)
for this router in a running service.

!!! warning "Planned for 0.3: routers inherit the application scope"
    In 0.2 the router stack and the application stack are separate (`RustStream::layer` does not
    wrap router handlers). In 0.3 routers will inherit the application scope, so app-level layers
    will wrap router handlers too, composing outside the router's own stack. See
    [middleware scopes](middleware.md#middleware-scopes).

## Composing and mounting

Build routers per module, then combine them however suits the service:

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

`merge` appends another router's registrations in order; the merged router's own layer was already
baked into its handlers, so the two need not share a middleware stack.

## Next

- The handler contract and the `#[subscriber]` macro: [Subscribers](subscribers.md).
- How the decode codec is resolved for `include`: [Codecs](codecs.md).
