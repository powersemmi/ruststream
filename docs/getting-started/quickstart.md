# Quick start

The fastest way to a running service is to scaffold one with `cargo generate`.

## Scaffold a project

```bash
cargo install cargo-generate
cargo generate --git https://github.com/powersemmi/ruststream templates/memory --name my-service
cd my-service
```

Scaffolding needs only `cargo generate`, not the `ruststream` CLI. `templates/memory` is the
in-memory starter (no external broker); each broker crate ships its own template (for example
`--git https://github.com/powersemmi/ruststream-nats templates/nats`). This writes an idiomatic,
multi-file project:

```
my-service/
├── Cargo.toml
└── src/
    ├── main.rs      # #[ruststream::app] builds the service and mounts the router
    ├── orders.rs    # handlers as #[subscriber] functions (one publishes a reply)
    └── routes.rs    # collects the handlers into a Router
```

## Run it

`#[ruststream::app]` generates `main`, so the binary already understands the framework commands:

```bash
cargo run -- run                # or: ruststream run, with the CLI installed
```

`cargo run -- run` starts a tokio runtime and runs the service until you press ++ctrl+c++ (the
`ruststream run` CLI is a convenience that forwards to it). The scaffold uses the in-memory broker,
so it runs with no external dependencies.

## Generate the AsyncAPI document

```bash
cargo run -- asyncapi gen
```

This prints the AsyncAPI document as JSON; the output flags (`-o`, `--yaml`) and the document
itself are covered in the [AsyncAPI guide](../guides/asyncapi.md).

## What the entry point looks like

=== "Macros"

    ```rust title="src/main.rs"
    --8<-- "examples/tutorial/main.rs:main"
    ```

=== "Manual"

    ```rust title="src/main.rs"
    --8<-- "examples/manual/tutorial/main.rs:main"
    ```

You write a function that builds the service; the macro turns it into a `main` that dispatches
`run` and `asyncapi gen`.

## Next

- Understand each piece in the [tutorial](tutorial.md).
- Learn the handler forms in [Subscribers](../guides/subscribers.md).
- Drive everything from the [CLI](../guides/cli.md).
