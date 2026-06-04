# Quick start

The fastest way to a running service is the CLI scaffolder.

## Scaffold a project

```bash
cargo install ruststream-cli
ruststream new my-service
cd my-service
```

This writes an idiomatic, multi-file project:

```
my-service/
├── Cargo.toml
└── src/
    ├── main.rs      # #[ruststream::app] builds the service and mounts the router
    ├── orders.rs    # handlers as #[subscriber] functions (one publishes a reply)
    ├── routes.rs    # collects the handlers into a Router
    └── stream.rs    # a broker-specific subscription descriptor
```

## Run it

`#[ruststream::app]` generates `main`, so the binary already understands the framework commands:

```bash
ruststream run                  # or: cargo run -- run
```

`ruststream run` shells out to `cargo run -- run`, which starts a tokio runtime and runs the service
until you press ++ctrl+c++. The scaffold uses the in-memory broker, so it runs with no external
dependencies.

## Generate the AsyncAPI document

```bash
ruststream asyncapi gen                 # prints JSON to stdout
ruststream asyncapi gen -o asyncapi.json
ruststream asyncapi gen --yaml
```

## What the entry point looks like

```rust title="src/main.rs"
mod orders;
mod routes;
mod stream;

use ruststream::memory::MemoryBroker;
use ruststream::runtime::{AppInfo, RustStream};

#[ruststream::app]
fn app() -> RustStream {
    RustStream::new(AppInfo::new("my-service", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        let router = routes::orders(b.broker());
        b.include_router(router);
    })
}
```

You write a function that builds the service; the macro turns it into a `main` that dispatches
`run` and `asyncapi gen`.

## Next

- Understand each piece in the [tutorial](tutorial.md).
- Learn the handler forms in [Subscribers & publishers](../guides/subscribers.md).
- Drive everything from the [CLI](../guides/cli.md).
