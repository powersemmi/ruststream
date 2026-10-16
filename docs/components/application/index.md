---
description: "The application value, RustStream, holds a service's brokers, hooks and middleware, and runs them as one process."
---

# Application

The application value, [`RustStream`](api:ruststream::runtime::RustStream), holds a service's brokers, hooks and middleware, and runs them as one process.

- [Application](./app.md): `RustStream`, `AppInfo` and `with_broker`.
- [`#[ruststream::app]`](./app-macro.md): the generated `main`.
- [Lifecycle hooks](./lifecycle.md): code around the brokers' connection.
- [Running the service](./running.md): `run`, `run_until`, `start` and `RunningApp`.
- [The `ruststream` command](./cli.md): `run`, `asyncapi gen` and scaffolding.
