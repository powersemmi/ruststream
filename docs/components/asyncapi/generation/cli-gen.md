---
sidebar_position: 3
title: Command
description: "asyncapi gen prints the document of a service from the main that #[ruststream::app] generates."
---

# Command

`asyncapi gen` prints the document of a service from the `main` that `#[ruststream::app]` generates.

The command builds the app with the builder the attribute names and prints what `build_spec` returns.
It prints JSON by default; `--yaml` prints YAML, and `-o <file>` writes to a file instead of standard output.
The service crate enables the `asyncapi` feature; without it the command fails with an error that names the feature.
The [`ruststream` command](../../application/cli.md) runs it through `cargo run` against a crate, and `cargo run -- asyncapi gen` does the same without it.
A `main` written by hand gets the same commands from [`run_main`](api:ruststream::runtime::cli::run_main).

## Example

In the service crate:

```bash
cargo run -- asyncapi gen
cargo run -- asyncapi gen --yaml -o asyncapi.yaml
```

With the `ruststream` command installed:

```bash
ruststream asyncapi gen -o asyncapi.json
```
