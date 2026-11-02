# A service on the compio runtime

A compio service is written with the same handlers as a tokio one: `#[subscriber]`, replies, `Out`
slots, retries and `workers(n)`. The app is built by `#[ruststream::app(runtime = compio, cores = n)]`;
without `cores` it runs on every core the process may use. Component pages:
[Compio runtime](https://powersemmi.github.io/ruststream/latest/components/application/compio-runtime/) and
[Testing a compio app](https://powersemmi.github.io/ruststream/latest/components/application/testing/compio-app/).

## What the runner does

- One single-threaded compio runtime per core, `io_uring` on Linux. Core 0 runs on the calling
  thread, owns the brokers and receives every subscription.
- A sequential subscription is handled on core 0. `workers(n)` spreads its lanes over the shared
  cores; `threads(n)` holds `n` cores of its own from core 1 up. `by_key` keeps a key on one lane.
- The `compio` feature turns `send` on: handler state and the future of a body are `Send`, as on
  tokio. A body holding an `Rc` across an `.await` fails at `include`.
- `Ctx(core): Ctx<Core>` (`ruststream::runtime::compio::Core`) tells a handler which core it runs on; `Core::current()` does the same outside a handler signature.
  `ruststream::runtime::compio::time::sleep` sleeps on the app's clock.
- `RustStream::run`, `run_until` and `start` run it; the attribute's `main` understands `run` and
  `asyncapi gen`. `ruststream::asyncapi::build_spec` takes a compio app.
- The runner builds on compio 0.20.0-beta.1 and moves to 0.20 when it ships.

## Imports

The memory prelude belongs to the tokio runner. A compio service names its items:
`ruststream::runtime::compio::{App, AppInfo, RustStream}`, `ruststream::runtime::HandlerOutcome`,
and the broker's own types. On `compio` alone the in-process bus is
`ruststream::memory::local::LocalMemoryBroker<N>` with `LocalMemorySource` and `LocalMemoryPublish`.

## Tests

`ruststream::testing::compio::TestApp` runs the app on the test's own compio runtime as the one
core: `Runtime::new()?.block_on(async { .. })` in a plain `#[test]`, then `start`, `broker::<B>()`,
`settle`, `advance` on the runner's virtual clock, `out::<M>()` and `shutdown`. One `TestApp` runs
per thread. A handler that calls `compio::time::sleep` directly is not advanced; use
`ruststream::runtime::compio::time::sleep`.
