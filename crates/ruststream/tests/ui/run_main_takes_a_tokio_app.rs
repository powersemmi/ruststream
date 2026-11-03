//! The CLI drives a tokio app: handing `run_main` anything else names the trait it needs and where
//! an app for the other runtime runs instead.
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;

/// Not an app at all: what a builder returns when it forgot the `RustStream` it was building.
struct Settings;

fn main() -> ExitCode {
    run_main(|| Settings)
}
