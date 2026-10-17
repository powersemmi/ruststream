---
sidebar_position: 2
title: Until
description: "run_until serves until a future of yours resolves, then shuts the service down gracefully."
---

# Until

[`run_until`](api:ruststream::runtime::RustStream::run_until) serves until a future of yours resolves, then shuts the service down gracefully.

It starts the service the way [`run`](./run.md) does, and installs no signal handlers.
The future starts once the service has started, so it may publish to the service and wait for the answer.
A subscriber that fails fast ends the run before the future resolves, and `run_until` returns that failure as its error.

## Example

{/* single-form: the call is the same with and without the macros; the Manual tab of the full file differs only in the handler */}
```rust reference="examples/app_running.rs#run_until"
```
