# HTTP frameworks

RustStream is not an HTTP framework. A service that serves an HTTP API and consumes messages runs
both sides in one process, on one tokio runtime. Your HTTP framework (axum, actix-web, or any other
tokio-based stack) runs beside the RustStream app. The wiring below uses axum. A transactional
outbox keeps the two sides consistent.

The full compiled example lives at
[`examples/http_outbox.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/http_outbox.rs):

```text
cargo run --example http_outbox --features macros,memory,json
```

## Running beside an HTTP server

Both sides start in `main`. `start()` runs the messaging side in the background and returns a
`RunningApp` handle that coordinates the two lifetimes:

=== "Macros"

    ```rust
    --8<-- "examples/http_outbox.rs:wiring"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/http_outbox.rs:wiring"
    ```

`start()` connects the brokers and opens the subscriptions. It resolves once the service is
running, so you get a startup error before the HTTP side accepts traffic.

`stopping()` returns an owned future. It resolves when the messaging side has stopped itself on a
fail-fast failure. You can plug it into axum's `with_graceful_shutdown`, and the HTTP server stops
with it.

`shutdown()` is the explicit graceful teardown. Call it once the HTTP server has stopped. The
teardown sequence and the [shutdown timeout](lifespan.md#shutdown-timeout) are in the lifespan
guide.

The HTTP side gets the publisher through a binding token. `.bindable()` wraps the broker,
`bind(..)` issues the token before the app consumes the broker, and `running.publisher(token)`
pairs the token with the connected broker. The paired publisher is a plain value, and you can
clone it into whatever state the HTTP framework holds.

## A healthz endpoint

`start()` reports readiness at startup. The health probe reports the service state after that.
`RunningApp::health()` hands out a cloneable `HealthProbe`:

```rust
--8<-- "examples/http_outbox.rs:healthz"
```

`state()` returns a lock-free snapshot: `Running`, `ShuttingDown`, `Stopped`, or
`Failed { reason }` carrying the fail-fast diagnostic. The probe keeps working after `shutdown()`
and reports the terminal state. On a fail-fast failure `/healthz` answers 503, even while sibling
tasks keep the process alive.

The route has its own state (`get(healthz).with_state(running.health())`), so the rest of the
router can hold any state at all. The wiring above registers `/healthz` beside `/orders`.

The subscriber side is an ordinary handler. The same service consumes the events its HTTP
endpoints produce, and any other service subscribed to the broker sees them too:

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
request path: `publisher.message(&event).publish().await`, exactly as when
[publishing from inside a handler](publishing.md). The
[metrics guide's complete server](metrics.md) is built this way.

The price is coupling. When the broker is unavailable, the HTTP request returns an error or waits.
If the endpoint also writes to a database, a gap opens between the write and the publish. A crash
in that gap loses the event. With the two steps in the opposite order, it leaves an event published
for a write that rolled back. That is a consistency bug, and it shows up at the next deploy. The
transactional outbox closes the gap.

## Transactional outbox

The endpoint records the event beside the business write, and a relay moves it to the broker
afterwards, so the write and the event only appear together. The pattern is not specific to HTTP
and has [a page of its own](transactional-outbox.md). That page works through the same example,
`examples/http_outbox.rs`.

## Try it

```text
curl -X POST http://127.0.0.1:8080/orders \
  -H 'content-type: application/json' -d '{"id":1,"item":"book"}'
```

The response returns as soon as the store commits the write. The `fulfil` handler logs the order a
moment later, once the relay has published the event.
