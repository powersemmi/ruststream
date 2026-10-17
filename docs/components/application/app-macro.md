---
sidebar_position: 2
title: App
description: "The attribute turns a builder function into the service's main: a binary that runs the service or prints its AsyncAPI document."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# App

The [`#[ruststream::app]`](api:ruststream::app) attribute turns a builder function into the service's `main`: a binary that runs the service or prints its AsyncAPI document.

The attribute keeps the function and adds a `main` that hands it to [`run_main`](api:ruststream::runtime::cli::run_main).
The function is synchronous, takes no arguments and returns `impl App` or a `RustStream`; any other signature is a compile error.
The function is the [application](./index.md) builder, unchanged.
The binary reads its first argument as a command, the same commands the [CLI](./cli.md) drives through `cargo run`.
`run`, or no argument, [runs](./running/run.md) the service until `SIGINT` or `SIGTERM`.
`asyncapi gen` prints the AsyncAPI document; `-o <file>` writes it to a file and `--yaml` switches the format from JSON.
`asyncapi gen` needs the crate's `asyncapi` feature.
With the `logging` feature, `run` installs the console logger first, unless the service installed a subscriber of its own.
An error is printed to stderr and the process exits with a failure code.
The attribute takes the runtime arguments of `#[tokio::main]`: `flavor = "current_thread"` and `worker_threads = n`.
The default is a multi-threaded runtime with one worker per core.
A hand-written `main` calls [`run_main_on`](api:ruststream::runtime::cli::run_main_on) to choose the runtime as an [`AppRuntime`](api:ruststream::runtime::cli::AppRuntime) value.
A test runs the same builder function on [`TestApp`](api:ruststream::testing::TestApp), as the example does.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/app_macro.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/app_macro.rs#app"
```

</TabItem>
</Tabs>
