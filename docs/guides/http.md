# HTTP frameworks

A service that serves an HTTP API and consumes messages runs both sides in one process, on one
tokio runtime: your HTTP framework (axum, actix-web, or any other tokio-based stack) beside the
RustStream app. RustStream is not an HTTP framework. The wiring below is axum, and the pattern
that keeps the two sides consistent is a transactional outbox.

The full compiled example lives at
[`examples/http_outbox.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/http_outbox.rs):

```text
cargo run --example http_outbox --features macros,memory,json
```

## Running beside an HTTP server

Both sides come up in `main`. `start()` brings the messaging side up in the background and returns
a `RunningApp` handle that coordinates the two lifetimes:

=== "Macros"

    ```rust
    --8<-- "examples/http_outbox.rs:wiring"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/http_outbox.rs:wiring"
    ```

`start()` runs the state producer, connects the brokers and opens the subscriptions. It resolves
once the service is running, so a startup failure surfaces before the HTTP side accepts traffic.

`stopping()` is an owned future that resolves if the messaging side tears itself down on a
fail-fast failure. Plug it into axum's `with_graceful_shutdown` and the process stops serving HTTP
instead of answering requests behind a dead consumer. `shutdown()` is the explicit graceful
teardown, run once the HTTP server has stopped: the `on_shutdown` hooks, a drain of in-flight
handlers bounded by the [shutdown timeout](lifespan.md#shutdown-timeout), then broker shutdown.

The publisher arrives through a bound token. `.bindable()` wraps the broker and `bind(..)` mints
the token before the app consumes it; `running.publisher(token)` pairs it once `start()` has
connected the broker. The paired publisher is a plain value, safe to clone into whatever state
the HTTP framework carries.

## A healthz endpoint

`start()` is the readiness gate; the health probe covers everything after it.
`RunningApp::health()` hands out a cheap, cloneable `HealthProbe` that a route can own:

```rust
--8<-- "examples/http_outbox.rs:healthz"
```

`state()` is a lock-free snapshot backed by a watch channel: `Running`, `ShuttingDown`, `Stopped`,
or `Failed { reason }` carrying the fail-fast diagnostic. The probe outlives `shutdown()`, so the
route keeps answering with the terminal state. That closes the gap `stopping()` alone leaves: when
the messaging side fail-fasts and a sibling task keeps the process alive, `/healthz` flips to 503
instead of serving a permanent 200 for a dead consumer.

The route carries its own state (`get(healthz).with_state(running.health())`), so it composes
with whatever state the rest of the router holds - the full wiring above registers it beside
`/orders`.

The subscriber side is an ordinary handler; the same service consumes what its HTTP endpoints
produce, and any other service subscribed to the broker sees the events too:

=== "Macros"

    ```rust
    --8<-- "examples/http_outbox.rs:handler"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/http_outbox.rs:handler"
    ```

## Publishing straight from a request

The simplest integration puts the publisher into the HTTP framework's state and publishes on the
request path, exactly like [publishing from inside a handler](publishing.md):
`publisher.message(&event).publish().await`. The
[metrics guide's complete server](metrics.md) does this to drive its counters.

The trade-off is coupling: a broker outage now fails or stalls HTTP requests, and a crash after
the database write but before the publish loses the event (or publishes an event for a write that
rolled back, in the opposite order). If the endpoint also writes to a database, that gap is a
consistency bug waiting for a deploy window. The fix is the transactional outbox.

## Transactional outbox

The endpoint records the event beside the business write and a relay moves it to the broker
afterwards, so neither side can happen without the other. The pattern is not specific to HTTP and
has a page of its own: [transactional outbox](transactional-outbox.md). The example this guide
runs, `examples/http_outbox.rs`, is the same one.

## Try it

```text
curl -X POST http://127.0.0.1:8080/orders \
  -H 'content-type: application/json' -d '{"id":1,"item":"book"}'
```

The response returns as soon as the store commits; the `fulfil` handler logs the order a moment
later, when the relay has published the event.
