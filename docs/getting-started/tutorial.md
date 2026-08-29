# Tutorial: build your first service

By the end of this page you have a running orders service: a message type, a handler, a reply, and a
router that collects them. It runs on the in-memory broker, so there is nothing external to start.
Swapping in a real broker is a one-line change, covered at the end.

## 1. Create the crate

```bash
cargo new orders-service
cd orders-service
```

```toml title="Cargo.toml"
[package]
name = "orders-service"
version = "0.1.0"
edition = "2024"

[dependencies]
ruststream = { version = "0.7", features = ["macros", "memory", "json", "asyncapi"] }
serde = { version = "1", features = ["derive"] }
```

## 2. Define a message and a handler

A handler is an `async fn` whose first parameter is the decoded payload. The `#[subscriber]` macro
turns it into a mountable definition named after the function.

=== "Macros"

    ```rust title="src/orders.rs"
    --8<-- "examples/tutorial/orders.rs:order"
    ```

=== "Manual"

    ```rust title="src/orders.rs"
    --8<-- "examples/manual/tutorial/orders.rs:order"
    ```

A handler returns a [`HandlerResult`](../guides/subscribers.md#acking): `Ack`, or a `nack` that drops
or requeues the message. Returning `()` or `Result<(), E>` also works - they convert into a result
(`Ok` acks, `Err` drops).

The `JsonSchema` derive is what puts the payload's schema in the AsyncAPI document of step 6, and
the type's doc comment becomes the message description. It needs no dependency of its own: the
`asyncapi` feature re-exports `schemars`.

## 3. Wire it into an app

=== "Macros"

    ```rust title="src/main.rs"
    --8<-- "examples/tutorial/first_app.rs:app"
    ```

=== "Manual"

    ```rust title="src/main.rs"
    --8<-- "examples/manual/tutorial/first_app.rs:app"
    ```

The macro turns `handle` into a value named after the function, so you import and pass it directly.

!!! tip "Codec defaults"
    `include` decodes with the default codec - `json` if enabled, otherwise `cbor`, otherwise
    `msgpack` - so it needs no codec argument. To decode with a different one everywhere, set it
    once with `with_broker_codec(broker, codec, |b| ...)`. See
    [Codecs](../guides/codecs.md) for the full resolution rules.

Run it:

```bash
cargo run -- run
```

## 4. Reply to messages

To publish a reply, return the reply value and name the destination with `publish(..)`:

=== "Macros"

    ```rust title="src/orders.rs"
    --8<-- "examples/tutorial/orders.rs:confirm"
    ```

=== "Manual"

    ```rust title="src/orders.rs"
    --8<-- "examples/manual/tutorial/orders.rs:confirm"
    ```

Mount it next to `handle`, with the same plain `include`; the reply goes out through the broker's
default publish policy under the default codec:

=== "Macros"

    ```rust title="src/main.rs"
    --8<-- "examples/tutorial/reply_app.rs:reply"
    ```

=== "Manual"

    ```rust title="src/main.rs"
    --8<-- "examples/manual/tutorial/reply_app.rs:reply"
    ```

See [Publishing & replies](../guides/publishing.md) for the full picture, including publishing from
inside a handler.

## 5. Organize with a router

As handlers grow, keep them in their own module and collect them into a
[`Router`](../guides/routing.md):

=== "Macros"

    ```rust title="src/routes.rs"
    --8<-- "examples/tutorial/routes.rs:routes"
    ```

=== "Manual"

    ```rust title="src/routes.rs"
    --8<-- "examples/manual/tutorial/routes.rs:routes"
    ```

A registration on a router ends in an explicit terminal. `.publisher(..)` names the reply wiring -
a publish policy is pure declaration, so the router still needs no broker - and `.mount()` takes
the broker's own default publish policy, the explicit spelling of what step 4 got by default.
[Routing](../guides/routing.md) covers the rest of the router surface.

=== "Macros"

    ```rust title="src/main.rs"
    --8<-- "examples/tutorial/main.rs:main"
    ```

=== "Manual"

    ```rust title="src/main.rs"
    --8<-- "examples/manual/tutorial/main.rs:main"
    ```

## 6. Inspect the AsyncAPI document

```bash
cargo run -- asyncapi gen
```

Every subscriber becomes a channel and a `receive` operation. `handle` and `confirm` share the
`orders` channel and still get one operation each, because they open separate subscriptions; the
reply adds a `send` operation on `confirmations`. Both payload types derive `schemars::JsonSchema`,
so the document carries their schemas under `components.messages`, each with the type's doc comment
as its description. The output flags (`-o`, `--yaml`) and the document itself are covered in
[AsyncAPI](../guides/asyncapi.md).

## 7. Swap in a real broker

Nothing above is tied to the in-memory broker. The broker is chosen at `with_broker`, so swapping
is a one-line change: add the broker crate as a dependency and construct it there (for example
`NatsBroker::new("nats://localhost:4222")` instead of `MemoryBroker::new()`); the handlers, router,
and codecs are unchanged. The available brokers and the side-by-side swap for each of them are in
[Brokers](../brokers/index.md#switching-brokers).

!!! info "The complete service is a compiled example"
    Every snippet on this page is embedded from
    [`examples/tutorial`](https://github.com/powersemmi/ruststream/tree/main/examples/tutorial)
    in the repository, which CI builds on every change: `first_app.rs` and `reply_app.rs` are the
    service as steps 3 and 4 leave it, `main.rs` the finished one. Run it yourself with
    `cargo run --example tutorial --features macros,memory,json,asyncapi -- run`.

## Next steps

- [Middleware](../guides/middleware.md) - cross-cutting logic around handlers.
- [Lifespan](../guides/lifespan.md) - shared state and startup/shutdown hooks.
- [Testing](../guides/testing.md) - test the handlers you just wrote, in-process.
- [Metrics](../guides/metrics.md) - Prometheus counters and histograms.
