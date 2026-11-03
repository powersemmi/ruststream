---
title: Runtimes
description: "A service is written once and runs on tokio, on compio or on embassy: the handlers are the same, and the app picks its runner."
---

# Runtimes

A service is written once, with the same handlers and the same mount chain, and it runs on one of three runners: the multi-thread runner on tokio, the runner on compio with one runtime per core, or the one-task runner on embassy for a device.
The facade has a feature for each, `tokio`, `compio` and `embassy`, and they add up: a build can enable `tokio` beside either of the others.

```toml
# A server: tokio is in the default features.
ruststream = "0.8"

# A server with one single-threaded runtime per core, on io_uring where the kernel has it.
ruststream = { version = "0.8", default-features = false, features = ["compio", "memory", "macros", "json"] }

# A device with a heap and no operating system: embassy alone, tokio never enters the build.
ruststream = { version = "0.8", default-features = false, features = ["embassy", "macros", "json"] }

# A device with no heap: the build allocates nothing, at startup or per message.
ruststream = { version = "0.8", default-features = false, features = ["embassy", "memory", "macros", "postcard"] }

# A broker crate on tokio, which turns the defaults off.
ruststream = { version = "0.8", default-features = false, features = ["tokio"] }
```

## The app picks the runner

`#[ruststream::app]` builds a `RustStream`, the app of the tokio runner.
`#[ruststream::app(runtime = compio, cores = n)]` builds the compio runner's `RustStream`, which runs on `n` cores, or on every available core without `cores`.
`#[ruststream::app(runtime = embassy)]` builds an `EmbeddedApp`, the app of the embassy runner and one embassy task.
Any other runtime name is a compile error.
The handlers carry no runtime: `#[subscriber]` expands to the same code under each, and the runner states what it asks of a handler where it mounts it.

## What each runner asks of a handler

The tokio runner moves a handler between threads at each `.await`, so the future of a handler body must be `Send`.
A body that holds an `Rc` across an `.await` fails at the `include` that mounts it, and the error names the handler.
The compio runner hands a delivery to a lane on another core, so it asks the same: handler state and the future of a body are `Send`, and a body that holds an `Rc` across an `.await` fails at the `include`.
A broker's connection stays on core 0, where the subscriptions are received.
The embassy runner polls every handler on its one task and asks nothing of the future, so the same body mounts there.
A continuation that runs after the settlement (`and_after`, a context hook) follows the same rule: its future is `Send` when the tokio runtime is on and carries no `Send` when only embassy is, so a device continuation may publish through the local bus.

## One trait for every runner

A broker implements one `Publisher`, one `Settle` and one set of capability traits, and every runner uses them.
Their futures and supertraits ask for `Send` (and `Sync`) when `tokio` is on and for nothing when only `embassy` is, so a broker on a device implements the same traits as a broker on a server.
A broker's option steps bound `PublishSink<Options = ..>` and apply on either runner.
A build with both features takes the stricter bound: a broker type that holds an `Rc` does not compile next to tokio, and the family's local memory bus shares its state through thread-safe pointers there.

## What each runner offers

The tokio runner offers `workers(n)`, `threads(n)`, the signals, metrics, OpenTelemetry and the CLI.
The compio runner offers `workers(n)`, `threads(n)` and `by_key` placed across the cores, `Core` to tell a handler where it runs, the signals, graceful shutdown, health and `asyncapi gen` through `run_main`.
Core 0 receives every subscription, and a `threads(n)` mount holds `n` cores of its own from core 1 up.
The component page [Compio runtime](../components/application/compio-runtime.md) shows a service and the placement.
The embassy runner offers constant-count workers (`.pool::<N>()` and `.keyed::<N>()`), a retry store with a capacity declared at the mount site (`.retry_capacity::<CAP>()`), continuations after the settlement, and shutdown and health through `Control` and `Health`.
An item one runner does not offer is a compile error on the other.

When a delayed retry finds the embassy store full, the copy has nowhere to wait.
That is a failed publish like any other, with its own error, "no free record": the handler's `.error` continuation is told, and without one the failure is logged and the copy is lost.
The delivery is settled either way, so the handler never runs again because the store was full.

## The embassy runner without a heap

The embassy runner builds in two forms, and one build holds one of them.
With the `alloc` feature it keeps headers, payloads, continuations and failures on the heap.
Without it the same runner keeps them in storage of a fixed size and allocates nothing, at startup or per message, so a device with no heap runs the framework.
The `tokio`, `json`, `cbor` and `msgpack` features turn `alloc` on, and so does any crate of the dependency graph that asks for it: features add up, and a graph with `alloc` on is the heap form throughout.

A service is written the same way in both forms: `#[subscriber]`, `#[ruststream::app(runtime = embassy)]`, `.then`, `.error`, `Out` slots and typed headers.
What differs is the form of the parts whose size the heap would decide:

| Part | With `alloc` | Without `alloc` |
|---|---|---|
| Headers of a message | `HeaderMap` | `FixedHeaders`: 8 entries and 256 bytes, and `insert` returns an error when it is full |
| Codec | `Codec`: `json`, `cbor`, `msgpack` | `SliceCodec`: `postcard` and `json-core`, which encode into a slice |
| Continuation (`and_after`, `.then`, `.error`) | boxed | held in a room of 128 bytes; a larger capture does not compile |
| Failures that `.error` reads | passed by value | borrowed from the subscription's fixed buffer |
| `ctx.after(..)` | registers | returns a `Result`, because a delivery holds four registrations |
| Batch | `Vec` of outcomes | a size up to 16, named at the mount with `.batch::<N>()`; the outcomes are `Outcomes<HandlerOutcome, N>`, and retries, replies and `Out` slots work as with `alloc` |
| Local bus | `LocalMemoryBroker::new()` | a `LocalMemoryBus` that the board declares as a `static` |
| Logger the app selects | `TracingLogger`, `LogLogger` or `DefmtLogger` | `LogLogger` or `DefmtLogger`, whichever the build enables |

The form is chosen by the build, so a handler never branches on it.
A heap type used in the fixed form does not resolve, and a capacity that is exceeded is a compile error that names it.
The component page [Embedded runtime](../components/application/embedded-runtime.md) shows a whole device service in the fixed form and every capacity it names.

## Testing

`TestApp` runs a tokio app, `testing::compio::TestApp` runs a compio app on the test's own compio runtime with the runner's virtual clock, and `testing::embedded::TestApp` runs an embedded app on the host with embassy-time's mock clock.
Each is in the facade when `testing` and its runner's feature are on, beside the others.
See [Testing a compio app](../components/application/testing/compio-app.md).
With `testing` on, every broker an embedded app registers implements the in-process mode, `InProcess`.
The embedded `TestApp` follows the form of the build: without `alloc` it drives the fixed form, and a test binary can install an allocator that fails every allocation the framework makes.
