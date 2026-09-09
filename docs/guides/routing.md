# Routing

As a service grows, handlers move out of `main.rs` into their own modules. A `Router` collects one
module's handlers into a single group. `include_router` mounts that group on a broker scope.

## Building a router

A `Router` mirrors the broker scope. `include` is the one entry point. It mounts a definition of any
form: plain, raw, batch, reply-publishing, injected. The definition itself picks the form.
`with_codec` switches the decode codec for the registrations that follow it; the ones already
mounted keep theirs (see [Codecs](codecs.md#per-handler)).

The subscription source comes from the definition. `#[subscriber(..)]` takes the broker's own source
expression, builder chain included, so you name no source at the mount site. Every call consumes the
router and returns a new one, so registrations chain:

=== "Macros"

    ```rust title="routes.rs"
    use ruststream::runtime::Router;

    --8<-- "examples/routing.rs:builders"
    ```

=== "Manual"

    ```rust title="routes.rs"
    use ruststream::runtime::Router;

    --8<-- "examples/manual/routing.rs:builders"
    ```

<!-- inline-rust: minimal mount fragment with placeholder routes module; the full compiled program is examples/routing.rs (merge form pulled in below) -->
```rust title="main.rs"
RustStream::new(info).with_broker(broker, |b| {
    b.include_router(routes::orders());
});
```

Some handlers need an attachment: a reply publisher or an
[`Out`](publishing.md#publishing-from-inside-a-handler) slot. They register on the router the same
way as on the scope, with one difference: an explicit `.build()` commits the registration.
`.out(marker, policy)` names the publish policy of one position: `Reply` for the reply, the slot's
marker for an `Out` slot. When the chain names no `.out(Reply, ..)`, `.build()` takes the broker's
own default publish policy for the reply.

A chain without `.build()` never becomes a router, so it does not compile. The policies stay pure
declaration, so a router with attachments still needs no broker:

=== "Macros"

    ```rust title="routes.rs"
    --8<-- "examples/tutorial/routes.rs:routes"
    ```

=== "Manual"

    ```rust title="routes.rs"
    --8<-- "examples/manual/tutorial/routes.rs:routes"
    ```

## Router middleware

A router can carry its own layer stack: `Router::layer` wraps every handler of that router when the
router is mounted. At `include_router` the application's global stack, added with
`RustStream::layer`, wraps around the router's stack. Scopes nest, and the application is outermost:

=== "Macros"

    ```rust title="main.rs"
    --8<-- "examples/logging_middleware.rs:layered_router"
    ```

=== "Manual"

    ```rust title="main.rs"
    --8<-- "examples/manual/logging_middleware.rs:layered_router"
    ```

A router hides its handlers' concrete types, so a layer that wraps them must be a `BlanketLayer`.
Both scopes, the `BlanketLayer` requirement and writing your own layer are covered in
[Middleware](middleware.md#middleware-scopes).

## Composing and mounting

Build one router per module, then combine them however the service needs:

<!-- inline-rust: illustrative multi-router composition with placeholder route modules; the compiled merge form is examples/routing.rs:merge, pulled in below -->
```rust
// Mount several routers on one broker - include_router can be called more than once.
RustStream::new(info).with_broker(broker, |b| {
    b.include_router(routes::orders());
    b.include_router(routes::shipping());
});
```

Or merge the groups into one router before mounting (the whole program is
[`examples/routing.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/routing.rs)):

=== "Macros"

    ```rust
    --8<-- "examples/routing.rs:merge"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/routing.rs:merge"
    ```

`merge` appends another router's registrations in order. Each router keeps its own codec and its own
layer stack. At mount time the outer router's layers, and the application's global stack, wrap
around the merged router's layers.

## Next

- The handler contract and the `#[subscriber]` macro: [Subscribers](subscribers.md).
- How the decode codec is resolved for `include`: [Codecs](codecs.md).
