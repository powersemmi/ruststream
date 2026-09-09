# Tutorial: build your first service

By the end of this page you have a running orders service: a message type, a handler, a reply, and a
router that collects them. It runs on the in-memory broker, so there is nothing external to start.
Swapping in a real broker is a one-line change, and step 7 shows it.

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
turns it into a subscriber definition named after the function.

=== "Macros"

    ```rust title="src/orders.rs"
    --8<-- "examples/tutorial/orders.rs:order"
    ```

=== "Manual"

    ```rust title="src/orders.rs"
    --8<-- "examples/manual/tutorial/orders.rs:order"
    ```

A handler returns a [`HandlerOutcome`](../guides/subscribers.md#acking): an `ack`, or a `nack` that
drops or requeues the message. You can return `()` or `Result<(), E>` instead, where `Ok` acks and
`Err` drops.

The `JsonSchema` derive puts the payload's schema into the AsyncAPI document of step 6. The type's
doc comment becomes the message description there. You need no extra dependency for it: the
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

!!! tip "Codec defaults"
    `include` decodes with the default codec, so it needs no codec argument. The default is `json`
    when the feature is enabled, otherwise `cbor`, otherwise `msgpack`. You can set one codec for
    all the broker's handlers at once with `with_broker_codec(broker, codec, |b| ...)`. See
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

Mount `confirm` next to `handle` with the same `include`. The reply is published with the broker's
default publish policy and encoded with the default codec.

=== "Macros"

    ```rust title="src/main.rs"
    --8<-- "examples/tutorial/reply_app.rs:reply"
    ```

=== "Manual"

    ```rust title="src/main.rs"
    --8<-- "examples/manual/tutorial/reply_app.rs:reply"
    ```

Publishing from inside a handler and the other ways to publish are in
[Publishing & replies](../guides/publishing.md).

## 5. Organize with a router

As the number of handlers grows, keep them in their own module and collect them into a
[`Router`](../guides/routing.md):

=== "Macros"

    ```rust title="src/routes.rs"
    --8<-- "examples/tutorial/routes.rs:routes"
    ```

=== "Manual"

    ```rust title="src/routes.rs"
    --8<-- "examples/manual/tutorial/routes.rs:routes"
    ```

`include` adds a plain handler to the router directly. A handler that publishes a reply hands back
a mount chain instead: `.out(Reply, ..)` names the reply's publish policy, and `.build()` finishes
the registration. Without `.out(Reply, ..)`, `.build()` takes the broker's default publish policy -
the same one `include` took in step 4. [Routing](../guides/routing.md) covers the rest of the router
surface.

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

Every subscriber adds a channel and a `receive` operation to the document. `handle` and `confirm`
share the `orders` channel and get one operation each, because their subscriptions are separate. The
reply adds a `send` operation on `confirmations`.

The document keeps the payload schemas under `components.messages`. The output flags (`-o`,
`--yaml`) and the document itself are covered in [AsyncAPI](../guides/asyncapi.md).

## 7. Swap in a real broker

Nothing above is tied to the in-memory broker: the broker is chosen at `with_broker`, so the swap is
a one-line change. Add the broker crate as a dependency and construct it there instead of
`MemoryBroker::new()`, for example `NatsBroker::new("nats://localhost:4222")`. The handlers, the
router and the codecs stay as they are. [Brokers](../brokers/index.md#switching-brokers) lists the
available brokers and the swap for each of them.

!!! info "The complete service is a compiled example"
    Every snippet on this page comes from
    [`examples/tutorial`](https://github.com/powersemmi/ruststream/tree/main/examples/tutorial)
    in the repository, which CI builds on every change. `first_app.rs` and `reply_app.rs` are the
    service as steps 3 and 4 leave it, and `main.rs` is the finished one. Run it yourself with
    `cargo run --example tutorial --features macros,memory,json,asyncapi -- run`.

## Next steps

- [Middleware](../guides/middleware.md) - cross-cutting logic around handlers.
- [Lifespan](../guides/lifespan.md) - shared state and startup/shutdown hooks.
- [Testing](../guides/testing.md) - test the handlers you just wrote, in-process.
- [Metrics](../guides/metrics.md) - Prometheus counters and histograms.
