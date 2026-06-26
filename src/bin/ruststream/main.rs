//! The `ruststream` command-line tool.
//!
//! A thin driver over `cargo`. A `RustStream` service is a normal binary whose `main` is generated
//! by `#[ruststream::app]`; this tool does not introspect it. Instead:
//!
//! - `ruststream run` shells out to `cargo run -- run` against the target crate.
//! - `ruststream asyncapi gen` shells out to `cargo run -- asyncapi gen`.
//!
//! Scaffolding a new project is `cargo generate` against a template (the in-memory starter lives in
//! this repo under `templates/memory`; brokers own theirs); this tool no longer ships its own `new`.

use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};

mod cargo;

/// `RustStream` command-line tool.
#[derive(Debug, Parser)]
#[command(name = "ruststream", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run a RustStream service (cargo run -- run against the target crate).
    Run {
        /// Path to the service crate or its Cargo.toml.
        #[arg(short = 'p', long, default_value = ".")]
        manifest_path: PathBuf,
        /// Build and run in release mode.
        #[arg(long)]
        release: bool,
        /// Extra arguments forwarded to the service after `run`.
        #[arg(trailing_var_arg = true)]
        args: Vec<String>,
    },
    /// AsyncAPI tooling.
    Asyncapi {
        #[command(subcommand)]
        command: AsyncApiCommand,
    },
}

#[derive(Debug, Subcommand)]
enum AsyncApiCommand {
    /// Generate the AsyncAPI document (cargo run -- asyncapi gen against the target crate).
    Gen {
        /// Path to the service crate or its Cargo.toml.
        #[arg(short = 'p', long, default_value = ".")]
        manifest_path: PathBuf,
        /// Build and run in release mode.
        #[arg(long)]
        release: bool,
        /// Write the spec to this file instead of stdout.
        #[arg(short = 'o', long)]
        out: Option<PathBuf>,
        /// Emit YAML instead of JSON.
        #[arg(long)]
        yaml: bool,
    },
}

fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Run {
            manifest_path,
            release,
            args,
        } => cargo::run(&manifest_path, release, &args),
        Command::Asyncapi {
            command:
                AsyncApiCommand::Gen {
                    manifest_path,
                    release,
                    out,
                    yaml,
                },
        } => cargo::asyncapi_gen(&manifest_path, release, out.as_deref(), yaml),
    }
}
