//! Shells out to `cargo run` against a target service crate, forwarding a `RustStream` subcommand.
//!
//! The service's generated `main` (from `#[ruststream::app]`) understands `run` and `asyncapi gen`;
//! we just build the argument list and inherit stdio so the spec lands on our stdout.

use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result, bail};

/// Resolves `path` to a `Cargo.toml`: appends the file name when given a directory.
fn manifest(path: &Path) -> PathBuf {
    if path.is_dir() {
        path.join("Cargo.toml")
    } else {
        path.to_path_buf()
    }
}

/// Builds the common `cargo run --manifest-path <..> [--release] --` prefix.
fn base(manifest_path: &Path, release: bool) -> Command {
    let mut cmd = Command::new("cargo");
    cmd.arg("run")
        .arg("--manifest-path")
        .arg(manifest(manifest_path));
    if release {
        cmd.arg("--release");
    }
    cmd.arg("--");
    cmd
}

/// Runs the service: `cargo run -- run [args...]`.
///
/// # Errors
///
/// Fails if `cargo` cannot be spawned or the service exits with a non-zero status.
pub(crate) fn run(manifest_path: &Path, release: bool, args: &[String]) -> Result<()> {
    let mut cmd = base(manifest_path, release);
    cmd.arg("run").args(args);
    exec(cmd)
}

/// Generates the `AsyncAPI` document: `cargo run -- asyncapi gen [-o <out>] [--yaml]`.
///
/// # Errors
///
/// Fails if `cargo` cannot be spawned or the service exits with a non-zero status.
pub(crate) fn asyncapi_gen(
    manifest_path: &Path,
    release: bool,
    out: Option<&Path>,
    yaml: bool,
) -> Result<()> {
    let mut cmd = base(manifest_path, release);
    cmd.args(["asyncapi", "gen"]);
    if let Some(out) = out {
        cmd.arg("-o").arg(out);
    }
    if yaml {
        cmd.arg("--yaml");
    }
    exec(cmd)
}

fn exec(mut cmd: Command) -> Result<()> {
    let status = cmd
        .status()
        .context("failed to spawn `cargo`; is it on PATH?")?;
    if !status.success() {
        bail!("cargo exited with {status}");
    }
    Ok(())
}
