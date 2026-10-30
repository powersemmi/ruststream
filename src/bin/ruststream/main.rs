//! The `ruststream` command-line tool.
//!
//! A thin driver over `cargo`. A `RustStream` service is a normal binary whose `main` is generated
//! by `#[ruststream::app]`; this tool does not introspect it. Instead:
//!
//! - `ruststream run` shells out to `cargo run -- run` against the target crate.
//! - `ruststream asyncapi gen` shells out to `cargo run -- asyncapi gen`.
//!
//! Scaffolding a new project is `cargo generate` against a template: the in-memory starter lives
//! in this repo under `templates/memory`, and brokers own theirs.

#![forbid(unsafe_code)]

use anyhow::Result;
use ruststream_cli::tool;

fn main() -> Result<()> {
    tool::main()
}
