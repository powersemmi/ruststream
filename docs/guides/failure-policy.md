# Failure policy

A message goes unhandled for two reasons: the handler body **panics**, or the incoming payload does
not **decode**. One set of policy values covers both cases. You can set the value per subscriber
with the `on_failure(..)` clause. The defaults for panic and decode differ, because the two failures
mean different things.

## Defaults

With no clause, a subscriber uses the built-in defaults:

- **panic = `fail_fast`**: a panic is a bug in the code. The runtime logs an error naming the
  subscription and starts a graceful shutdown: it cancels the shutdown token and runs the shutdown
  hooks. [`run`](../index.md) returns `Err` with a non-zero exit code, which an orchestrator uses to
  restart the service.
- **decode = `drop`**: a decode failure usually means bad external input. The runtime drops the one
  bad message (a nack without requeue) and keeps the service running: on an untrusted topic,
  stopping it would be a denial of service. The same key settles the message when a
  [typed header contract](headers.md) does not parse. It also applies when a payload type that
  [deserializes itself](subscribers.md#raw-subscribers) (`#[derive(Deserialized)]`) rejects the
  bytes in its own constructor.

=== "Macros"

    ```rust
    --8<-- "examples/failure_policy.rs:defaults"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/failure_policy.rs:defaults"
    ```

## Setting a policy

`on_failure(panic = .., decode = ..)` sets the value for either key. A key you omit keeps its
default:

=== "Macros"

    ```rust
    --8<-- "examples/failure_policy.rs:tuned"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/failure_policy.rs:tuned"
    ```

The policy values are:

| Value                 | Effect                                                                |
|-----------------------|-----------------------------------------------------------------------|
| `fail_fast`           | Log, start a graceful shutdown, and make `run` return `Err`.           |
| `drop`                | Drop the message (`nack` without requeue).                             |
| `retry`               | Requeue the message (`nack` with requeue).                            |
| `retry_after(<dur>)`  | Requeue after a delay (see the delayed-redelivery section in [Subscribers](subscribers.md)). |
| `skip`                | Acknowledge the failed message to move past it. Not success: the message is gone, unprocessed. |

Pick `retry` for decode failures with care: a payload that never decodes is redelivered forever,
unless the broker has a dead-letter or max-deliveries policy. `skip` is the deliberate escape hatch
for a poison message.

=== "Macros"

    ```rust
    --8<-- "examples/failure_policy.rs:skip"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/failure_policy.rs:skip"
    ```

## How it behaves

- The runtime catches a panic (`catch_unwind`), so a panicking handler never stops the dispatch
  loop. Under `fail_fast` the message stays unsettled, and a broker with redelivery delivers it
  again after the restart. Under the other policies the runtime settles the message and the
  subscriber keeps consuming. Catching works only when a panic unwinds: in a build with
  `panic = "abort"` the process is already gone.
- Decoding returns a `Result` instead of panicking, so nothing unwinds here. The `decode` key
  settles the message directly (see [Codecs](codecs.md#decode-failures)).
- On the batch path each element decodes independently, and the `decode` key applies to each of
  them. The `panic` key applies to a panic in the batch handler. There is no per-element panic
  handling.

The full example: [`examples/failure_policy.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/failure_policy.rs).
