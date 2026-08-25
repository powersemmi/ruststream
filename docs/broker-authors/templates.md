# Template contract

How a crate ships a [`cargo generate`](https://github.com/cargo-generate/cargo-generate) scaffold
that stays in sync with its API. A template is a CI-compiled artifact owned by the crate whose
broker it wires. Core ships only the in-memory `templates/memory`; each broker crate owns the
templates for its transports.

## Shape

A template is a directory rendered by `cargo generate`:

```
templates/<name>/
├── cargo-generate.toml   # manifest: description, any declared placeholders
├── Cargo.toml.liquid     # name = "{{project-name}}"; pins ruststream + the broker crate
└── src/
    ├── main.rs           # the #[ruststream::app] builder
    ├── orders.rs         # #[subscriber] handlers
    └── routes.rs         # a Router collecting the handlers
```

- Placeholders use cargo-generate's Liquid syntax; `{{project-name}}` (the `--name` value) is
  built in, so a minimal template declares none.
- The manifest is named `Cargo.toml.liquid`, and cargo-generate drops the `.liquid` suffix once it
  has rendered the file. The suffix is not cosmetic: cargo's git-source package discovery parses
  every `Cargo.toml` in a repository regardless of `exclude`, and a placeholder package name makes
  it reject the manifest, so anyone depending on the crate by git source sees the error. Name any
  other templated file cargo would parse the same way.
- The manifest pins `ruststream` to the supported minor and the broker crate to its own version.
- One template per broker transport or topology (for example `nats` vs `nats-js`, or
  `redis-stream` / `redis-pubsub` / `redis-list`), mirroring the one-kind-per-template model.

The template sources carry `{{...}}` placeholders, so they are not valid Rust/TOML until rendered
and must stay out of the crate's cargo workspace (`exclude = ["templates"]`), on top of the
`.liquid` naming that hides the manifest from cargo entirely.

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
per template catches every API-drift break. Off-flag combinations are static authoring concerns
(a dangling `use`, an unfilled slot), checked locally, not in CI.

## Ownership

- Core (`ruststream`) owns only `templates/memory` (the in-process broker it ships), so a default
  `cargo generate` works offline with no broker dependency.
- Each broker crate owns the templates for its transports and runs the drift job in its own CI.
