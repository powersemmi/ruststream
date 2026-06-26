# Template contract

How a crate ships a [`cargo generate`](https://github.com/cargo-generate/cargo-generate) scaffold
that stays in sync with its API. The contract is the scaffolding parallel of the
[conformance harness](../broker-authors/conformance.md): a template is a CI-compiled artifact owned by the
crate whose broker it wires, not hand-maintained strings in the core CLI. Core ships only the
in-memory `templates/memory`; each broker crate owns the templates for its transports.

## Shape

A template is a directory rendered by `cargo generate`:

```
templates/<name>/
├── cargo-generate.toml   # manifest: description, any declared placeholders
├── Cargo.toml            # name = "{{project-name}}"; pins ruststream + the broker crate
└── src/
    ├── main.rs           # the #[ruststream::app] builder
    ├── orders.rs         # #[subscriber] handlers
    └── routes.rs         # a Router collecting the handlers
```

- Placeholders use cargo-generate's Liquid syntax; `{{project-name}}` (the `--name` value) is
  built in, so a minimal template declares none.
- `Cargo.toml` pins `ruststream` to the supported minor and the broker crate to its own version.
- One template per broker transport or topology (for example `nats` vs `nats-js`, or
  `redis-stream` / `redis-pubsub` / `redis-list`), mirroring the one-kind-per-template model.

The template sources carry `{{...}}` placeholders, so they are not valid Rust/TOML until rendered
and must stay out of the crate's cargo workspace (`exclude = ["templates"]`).

## CI-compiled (the contract)

Each owning repo's CI renders every template and compiles it against the pinned versions, so an API
change that breaks a scaffold fails the owning repo's CI - where the fix belongs - not a user's
first `cargo build`. The drift job:

1. installs `cargo-generate`,
2. renders the template into a temp dir (`cargo generate --path templates/<name> --name smoke`),
3. runs `cargo check` on the rendered project.

Until the supported `ruststream` is published, the job injects a `[patch.crates-io]` into the
rendered project pointing at the local checkout (the sibling-checkout layout the broker CI already
uses), so the scaffold compiles against the unpublished version.

## Additive-only authoring

Feature blocks only ADD; no `{% else %}` or `{% if not flag %}` negative branches. The no-flag
render is then a strict subset of the all-features render, so a single all-features `cargo check`
per template catches every API-drift break - nothing can hide in an off branch. Off-flag
combinations are static authoring concerns (a dangling `use`, an unfilled slot), checked locally,
not in CI. This additive-only rule is part of the contract.

## Ownership

- Core (`ruststream`) owns only `templates/memory` (the in-process broker it ships), so a default
  `cargo generate` works offline with no broker dependency.
- Each broker crate owns the templates for its transports and runs the drift job in its own CI.
- This split keeps broker-specific API out of the core CLI: core knows nothing about a broker's
  descriptors; the template lives with the crate that defines them.
