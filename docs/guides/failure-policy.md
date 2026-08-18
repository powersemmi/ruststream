# Failure policy

Two things can go wrong before the handler logic runs: the handler body can
**panic**, and an incoming payload can fail to **decode**. RustStream settles both through one
vocabulary, set per subscriber with the `on_failure(..)` clause, but with different defaults,
because the two failures mean different things.

## Defaults

With no clause, a subscriber uses the built-in defaults:

- **panic = `fail_fast`**: a panic is an internal bug. The runtime logs a loud error naming the
  subscription, then starts a graceful shutdown (it cancels the shutdown token and runs the
  shutdown hooks) and makes [`run`](../index.md) return `Err` with a non-zero exit. An orchestrator
  restarts the service and the operator lands in the logs, instead of a subscriber that silently
  stopped consuming or an invisible redelivery loop.
- **decode = `drop`**: a decode failure is usually bad external input. Dropping the one bad message
  (a nack without requeue) keeps a single malformed payload from taking the consumer down, which on
  an untrusted topic would be a poison-message or denial-of-service footgun. The same policy covers
  a [typed header contract](headers.md) that fails to parse - headers are the same class of
  external input as the payload, so one `decode` key settles both.

```rust
--8<-- "examples/failure_policy.rs:defaults"
```

## Setting a policy

`on_failure(panic = .., decode = ..)` overrides either key (both are optional; an omitted key keeps
its default):

```rust
--8<-- "examples/failure_policy.rs:tuned"
```

The policy values are:

| Value                 | Effect                                                                |
|-----------------------|-----------------------------------------------------------------------|
| `fail_fast`           | Log, start a graceful shutdown, and make `run` return `Err`.           |
| `drop`                | Drop the message (`nack` without requeue).                             |
| `retry`               | Requeue the message (`nack` with requeue).                            |
| `retry_after(<dur>)`  | Requeue after a delay (see the delayed-redelivery section in [Subscribers](subscribers.md)). |
| `skip`                | Acknowledge the failed message to move past it. Not success: the message is gone, unprocessed. |

`skip` is the deliberate poison-message escape hatch: it advances past a message that cannot be
processed rather than dropping or retrying it. Pick `retry` for decode failures with care: a
payload that can never decode will redeliver forever unless the broker has a dead-letter or
max-deliveries policy.

```rust
--8<-- "examples/failure_policy.rs:skip"
```

## How it behaves

- A panic is caught (`catch_unwind`), so a panicking handler never kills the dispatch loop. Under
  `fail_fast` the message is left unsettled, so a broker with redelivery hands it back after the
  restart; under the other policies it is settled and the subscriber keeps consuming. Catching only
  applies under an unwinding panic profile; with `panic = "abort"` the process is already gone.
- A decode failure surfaces as a `Result`, so no unwinding is involved; the `decode` policy settles
  the message directly. The same policy can also be set when building a handler by hand: the typed
  adapter `typed(codec, handler)` returns a `Typed` wrapper whose `on_decode_failure` accepts a
  `FailurePolicy` (see [Codecs](codecs.md#decode-failures)).
- On the batch path the policy applies per batch decode (each element decodes independently) and to
  a panic in the batch handler. There is no per-element panic handling.

This is the full example: [`examples/failure_policy.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/failure_policy.rs).
