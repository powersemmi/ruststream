//! The command-line entry point of [RustStream](https://docs.rs/ruststream) services: the
//! `main` that `#[ruststream::app]` generates runs [`run_main`], which parses the service's
//! arguments and runs it or prints its `AsyncAPI` document.
//!
//! Services use it through the `ruststream` facade, as
//! [`ruststream::runtime::cli`](https://docs.rs/ruststream/latest/ruststream/runtime/cli/), whose
//! overview teaches it. The `tool` feature adds the driver of the `ruststream` binary.

#[cfg(feature = "compio")]
mod compio_entry;
#[cfg(feature = "tokio")]
mod entry;
#[cfg(feature = "tool")]
#[doc(hidden)]
pub mod tool;

#[cfg(feature = "tokio")]
pub use entry::{AppRuntime, run_main, run_main_on};

/// The compio entry point: what `#[ruststream::app(runtime = compio)]` expands to.
#[cfg(feature = "compio")]
pub mod compio {
    pub use crate::compio_entry::run_main;
}

#[cfg(feature = "asyncapi")]
use std::fs;
use std::io;
use std::process::ExitCode;

use thiserror::Error;

#[cfg(feature = "asyncapi")]
use ruststream_asyncapi as asyncapi;
use ruststream_runtime::Described;
#[cfg(feature = "compio")]
use ruststream_runtime_compio::RustStreamError as CompioRunError;
#[cfg(feature = "tokio")]
use ruststream_runtime_tokio::RustStreamError;

/// Errors surfaced by the generated CLI entry point.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum CliError {
    /// The Tokio runtime could not be built.
    #[error("failed to build the async runtime: {0}")]
    Runtime(#[source] io::Error),
    /// The service returned an error while running (see [`RustStreamError`]).
    #[cfg(feature = "tokio")]
    #[error(transparent)]
    Run(#[from] RustStreamError),
    /// The compio service returned an error while running.
    #[cfg(feature = "compio")]
    #[error(transparent)]
    RunCompio(#[from] CompioRunError),
    /// A compio app was asked to run on zero cores.
    #[cfg(feature = "compio")]
    #[error("a compio service needs at least one core")]
    NoCores,
    /// The first argument was not a recognized command.
    #[error("unknown command {0:?}; expected `run` or `asyncapi gen`")]
    UnknownCommand(String),
    /// `asyncapi` was followed by something other than `gen`.
    #[error("unknown `asyncapi` subcommand {0:?}; expected `gen`")]
    UnknownAsyncApi(String),
    /// An unrecognized flag was passed to `asyncapi gen`.
    #[error("unknown option {0:?} for `asyncapi gen`")]
    UnknownOption(String),
    /// `asyncapi gen` was requested but the crate does not enable the `asyncapi` feature.
    #[error("`asyncapi gen` requires the crate to enable ruststream's `asyncapi` feature")]
    AsyncApiDisabled,
    /// The generated spec could not be written to the requested file.
    #[error("could not write the spec to {path}: {source}")]
    WriteSpec {
        /// The output path that failed.
        path: String,
        /// The underlying I/O error.
        #[source]
        source: io::Error,
    },
    /// The spec could not be serialized to JSON.
    #[cfg(feature = "asyncapi")]
    #[error("could not serialize the AsyncAPI spec to JSON: {0}")]
    SerializeJson(#[from] serde_json::Error),
    /// The spec could not be serialized to YAML.
    #[cfg(feature = "asyncapi")]
    #[error("could not serialize the AsyncAPI spec to YAML: {0}")]
    SerializeYaml(#[source] serde_norway::Error),
}

/// The command parsed from the process arguments.
#[doc(hidden)]
#[derive(Debug, PartialEq, Eq)]
pub enum Command {
    /// Run the service until interrupted.
    Run,
    /// Generate the `AsyncAPI` document.
    AsyncApiGen {
        /// Output file, or stdout when `None`.
        out: Option<String>,
        /// Emit YAML instead of JSON.
        yaml: bool,
    },
}

/// Maps a dispatch outcome onto the process exit code, printing the error to stderr. Split from
/// the entry points so the mapping is reachable without the process arguments.
#[doc(hidden)]
#[must_use]
pub fn report(outcome: Result<(), CliError>) -> ExitCode {
    match outcome {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("ruststream: {err}");
            ExitCode::FAILURE
        }
    }
}

#[doc(hidden)]
pub fn parse(args: &[String]) -> Result<Command, CliError> {
    match args.first().map(String::as_str) {
        None | Some("run") => Ok(Command::Run),
        Some("asyncapi") => parse_asyncapi(&args[1..]),
        Some(other) => Err(CliError::UnknownCommand(other.to_owned())),
    }
}

fn parse_asyncapi(args: &[String]) -> Result<Command, CliError> {
    if args.first().map(String::as_str) != Some("gen") {
        let found = args.first().cloned().unwrap_or_default();
        return Err(CliError::UnknownAsyncApi(found));
    }
    let mut out = None;
    let mut yaml = false;
    let mut rest = args[1..].iter();
    while let Some(arg) = rest.next() {
        match arg.as_str() {
            "-o" | "--out" => out = rest.next().cloned(),
            "--yaml" => yaml = true,
            other => return Err(CliError::UnknownOption(other.to_owned())),
        }
    }
    Ok(Command::AsyncApiGen { out, yaml })
}

#[cfg(feature = "asyncapi")]
#[doc(hidden)]
pub fn generate_spec<A: Described>(app: &A, out: Option<&str>, yaml: bool) -> Result<(), CliError> {
    let spec = asyncapi::build_spec(app);
    let text = if yaml {
        spec.to_yaml().map_err(CliError::SerializeYaml)?
    } else {
        spec.to_json()?
    };
    if let Some(path) = out {
        fs::write(path, text).map_err(|source| CliError::WriteSpec {
            path: path.to_owned(),
            source,
        })
    } else {
        println!("{text}");
        Ok(())
    }
}

#[cfg(not(feature = "asyncapi"))]
#[doc(hidden)]
pub fn generate_spec<A: Described>(
    _app: &A,
    _out: Option<&str>,
    _yaml: bool,
) -> Result<(), CliError> {
    Err(CliError::AsyncApiDisabled)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    #[cfg(feature = "asyncapi")]
    use std::env;
    #[cfg(feature = "asyncapi")]
    use std::fs;
    #[cfg(feature = "asyncapi")]
    use std::process;
    use std::process::ExitCode;

    use ruststream_core::ServerSpec;
    use ruststream_runtime::{AppInfo, HandlerMetadata};

    use super::{CliError, Command, Described, generate_spec, parse, report};

    fn args(parts: &[&str]) -> Vec<String> {
        parts.iter().map(|s| (*s).to_owned()).collect()
    }

    /// `ExitCode` is opaque (no `PartialEq`), so compare the rendered form.
    fn code_of(outcome: Result<(), CliError>) -> String {
        format!("{:?}", report(outcome))
    }

    #[test]
    fn empty_args_default_to_run() {
        assert_eq!(parse(&args(&[])).unwrap(), Command::Run);
        assert_eq!(parse(&args(&["run"])).unwrap(), Command::Run);
    }

    #[test]
    fn asyncapi_gen_parses_output_and_format() {
        let cmd = parse(&args(&["asyncapi", "gen", "-o", "spec.yaml", "--yaml"])).unwrap();
        assert_eq!(
            cmd,
            Command::AsyncApiGen {
                out: Some("spec.yaml".to_owned()),
                yaml: true,
            }
        );
    }

    #[test]
    fn asyncapi_gen_defaults_to_json_stdout() {
        let cmd = parse(&args(&["asyncapi", "gen"])).unwrap();
        assert_eq!(
            cmd,
            Command::AsyncApiGen {
                out: None,
                yaml: false,
            }
        );
    }

    #[test]
    fn unknown_command_is_rejected() {
        assert!(parse(&args(&["frobnicate"])).is_err());
        assert!(parse(&args(&["asyncapi", "lint"])).is_err());
        assert!(parse(&args(&["asyncapi", "gen", "--nope"])).is_err());
    }

    #[test]
    fn the_exit_code_follows_the_dispatch_outcome() {
        assert_eq!(code_of(Ok(())), format!("{:?}", ExitCode::SUCCESS));
        assert_eq!(
            code_of(Err(CliError::UnknownCommand("frobnicate".to_owned()))),
            format!("{:?}", ExitCode::FAILURE),
        );
    }

    /// A built service of no runner at all: the generator reads only what `Described` offers.
    struct Stub {
        info: AppInfo,
        servers: BTreeMap<String, ServerSpec>,
        handlers: Vec<HandlerMetadata>,
    }

    impl Stub {
        fn new() -> Self {
            Self {
                info: AppInfo::new("cli-demo", "0.1.0"),
                servers: BTreeMap::new(),
                handlers: Vec::new(),
            }
        }
    }

    impl Described for Stub {
        fn info(&self) -> &AppInfo {
            &self.info
        }

        fn servers(&self) -> &BTreeMap<String, ServerSpec> {
            &self.servers
        }

        fn handlers(&self) -> &[HandlerMetadata] {
            &self.handlers
        }
    }

    #[cfg(feature = "asyncapi")]
    #[test]
    fn asyncapi_gen_writes_json_and_yaml_to_a_file() {
        let dir = env::temp_dir();
        let json_path = dir.join(format!("ruststream-cli-{}-spec.json", process::id()));
        let json_out = json_path.to_str().expect("utf-8 temp path");
        generate_spec(&Stub::new(), Some(json_out), false).expect("the spec must be written");
        let text = fs::read_to_string(&json_path).expect("the spec file exists");
        let spec: serde_json::Value = serde_json::from_str(&text).expect("valid JSON");
        assert_eq!(spec["info"]["title"], "cli-demo");
        fs::remove_file(&json_path).expect("cleanup");

        let yaml_path = dir.join(format!("ruststream-cli-{}-spec.yaml", process::id()));
        let yaml_out = yaml_path.to_str().expect("utf-8 temp path");
        generate_spec(&Stub::new(), Some(yaml_out), true).expect("the spec must be written");
        let text = fs::read_to_string(&yaml_path).expect("the spec file exists");
        let spec: serde_norway::Value = serde_norway::from_str(&text).expect("valid YAML");
        assert_eq!(spec["info"]["title"].as_str(), Some("cli-demo"));
        fs::remove_file(&yaml_path).expect("cleanup");
    }

    #[cfg(feature = "asyncapi")]
    #[test]
    fn asyncapi_gen_defaults_to_stdout() {
        // Nothing to read back (the document goes to stdout), so the assertion is that the
        // stdout arm reports success rather than falling through to a write error.
        generate_spec(&Stub::new(), None, false).expect("the spec must reach stdout");
    }

    #[cfg(feature = "asyncapi")]
    #[test]
    fn asyncapi_gen_reports_the_path_it_could_not_write() {
        let missing = env::temp_dir()
            .join("ruststream-cli-absent-directory")
            .join("spec.json");
        let out = missing.to_str().expect("utf-8 temp path");
        let err = generate_spec(&Stub::new(), Some(out), false)
            .expect_err("a spec cannot be written into a missing directory");
        let CliError::WriteSpec { path, .. } = err else {
            panic!("expected a write error, got: {err:?}");
        };
        assert_eq!(path, out, "the error names the output path");
    }

    #[cfg(not(feature = "asyncapi"))]
    #[test]
    fn asyncapi_gen_without_the_feature_is_rejected() {
        let err = generate_spec(&Stub::new(), None, false)
            .expect_err("the spec needs the asyncapi feature");
        assert!(matches!(err, CliError::AsyncApiDisabled), "got: {err:?}");
    }
}
