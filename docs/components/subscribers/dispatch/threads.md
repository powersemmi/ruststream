---
sidebar_position: 2
title: Threads
description: "threads(n) gives a subscription n threads of its own, so a handler that computes leaves the app's runtime free for I/O."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Threads

`threads(n)` gives a subscription `n` threads of its own, so a handler that computes leaves the app's runtime free for I/O.

A handler that computes holds up the thread it runs on.
On the app's runtime, that thread also serves the broker clients' I/O, the timers and every other subscription.
`threads(n)` starts `n` threads with the subscription, each running a current-thread runtime.
The subscription's loop stays on the app's runtime and hands deliveries to the threads in turn, through a bounded ring per thread.
It stops reading the broker only when every ring is full.
Decoding, the handler, encoding a reply and settling run on the thread that got the delivery, and only there.
So does everything the delivery leaves behind: a timer, a plain `tokio::spawn`, a continuation, the timer of a [`retry_after`](../messages/retry-after.md) copy.
[`MainRuntime`](api:ruststream::runtime::MainRuntime) is the way back to the app's runtime: `Ctx<MainRuntime>` as a parameter, or [`main_runtime()`](api:ruststream::runtime::MainRuntimeExt::main_runtime) on the context of a manual body.
[`spawn`](api:ruststream::runtime::MainRuntime::spawn) starts a task there, and [`run`](api:ruststream::runtime::MainRuntime::run) runs one there and returns its output; what they send must be `Send`.
`threads(n, by_key)` sends each key to a fixed thread, so a key keeps its order, as with [workers](./workers.md).
At the mount site the setting is [`.threads(n)`](api:ruststream::runtime::SubscriberSettings::threads) or [`.threads_by_key(n)`](api:ruststream::runtime::SubscriberSettings::threads_by_key); it fills the position `workers` fills, so a subscription takes one of them.
A delivery allocates nothing on its way to a thread.
On shutdown the loop stops reading, the threads finish their rings and wait for what the deliveries left behind, all within the app's [`shutdown_timeout`](api:ruststream::runtime::RustStream::shutdown_timeout).
A plain `tokio::spawn` is not waited for and ends with its thread; work that must outlive the subscription goes through `MainRuntime::spawn`.
The threads pay from about ten microseconds of computation per delivery; on lighter handlers they cost more CPU than they save.
[Diagnostics](./poll-diagnostics.md) find the handlers that need them.
Under [`TestApp`](api:ruststream::testing::TestApp) the threads run as `n` workers on the test's runtime.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/threads.rs#threads"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/threads.rs#threads"
```

</TabItem>
</Tabs>

## The app's runtime

The dedicated threads and the app runtime's threads share the machine's cores, and Tokio starts one worker per core by default.
A service with `threads(n)` sizes the app's runtime for its I/O.
[`#[ruststream::app]`](api:ruststream::app) takes the runtime arguments of `#[tokio::main]`: `flavor = "current_thread"` or `worker_threads = n`.
A current-thread runtime beside dedicated threads gives the rest of the service the lowest latency.
In the manual form, `main` passes an [`AppRuntime`](api:ruststream::runtime::cli::AppRuntime) to [`run_main_on`](api:ruststream::runtime::cli::run_main_on).

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/threads.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/threads.rs#app"
```

</TabItem>
</Tabs>
