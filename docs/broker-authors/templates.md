# Template contract

A crate ships a [`cargo generate`](https://github.com/cargo-generate/cargo-generate) template for
its broker. The crate's own CI checks the template, so the scaffold does not drift from the crate's
API.

## Shape

A template is a directory that `cargo generate` renders into a scaffold:

```
templates/<name>/
├── cargo-generate.toml   # manifest: description, any declared placeholders
├── Cargo.toml.liquid     # name = "{{project-name}}"; pins ruststream + the broker crate
└── src/
    ├── main.rs           # the #[ruststream::app] builder
    ├── orders.rs         # #[subscriber] handlers
    └── routes.rs         # a Router collecting the handlers
```

- Placeholders are written in cargo-generate's Liquid syntax. `{{project-name}}` is built in and
  takes the `--name` value, so a minimal template declares none of its own.
- The package manifest is named `Cargo.toml.liquid`, and cargo-generate drops the `.liquid` suffix
  once it has rendered the file. The suffix is not decoration. When cargo discovers packages in a
  git source, it parses every `Cargo.toml` in the repository and ignores `exclude`. A placeholder in
  the package name makes cargo reject the manifest, and anyone who depends on the crate by git
  source sees that error. Name any other templated file cargo would parse the same way.
- The manifest pins `ruststream` to the supported minor version and the broker crate to its own.
- One template per broker transport or topology: for example `nats` and `nats-js`, or
  `redis-stream`, `redis-pubsub` and `redis-list`.

Template sources carry `{{...}}` placeholders, so they parse as neither Rust nor TOML until they
are rendered. Keep them out of the crate's cargo workspace with `exclude = ["templates"]`.

## CI-compiled (the contract)

The owning crate's CI renders every template and compiles the scaffold against the pinned versions.
An API change that breaks a scaffold stops that crate's CI, not a user's first build. The job:

1. installs `cargo-generate`,
2. renders the template into a temporary directory
   (`cargo generate --path templates/<name> --name smoke`),
3. runs `cargo check` in the scaffold.

The job edits the rendered manifest twice. First it rewrites the `ruststream` version requirement to
the version being built, which is what lets an unpublished pre-release resolve at all:
`[patch.crates-io]` redirects where a crate comes from, not which versions a requirement accepts,
and cargo keeps a pre-release out of a range that does not name one. Then it adds the
`[patch.crates-io]` entry itself, pointing at the local `ruststream` checkout. That is the
sibling-checkout layout the broker CI already uses.

## Additive-only authoring

A feature block only adds code: `{% else %}` and negative `{% if not flag %}` branches are not
allowed in a template. The scaffold rendered without flags is then a strict subset of the scaffold
rendered with all of them, so one all-features `cargo check` per template catches every drift from
the API.

With the flags off, only authoring mistakes are possible: a dangling `use`, an unfilled slot. Check
for them locally.

## Ownership

- Core (`ruststream`) owns one template, `templates/memory`, for the in-memory broker it ships, so a
  default `cargo generate` works offline with no broker dependency.
- A broker crate owns the templates for its transports and runs the same job in its own CI.
