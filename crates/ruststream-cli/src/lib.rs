//! The command-line entry point of [RustStream](https://docs.rs/ruststream) services: the
//! `main` that `#[ruststream::app]` generates runs [`run_main`], which parses the service's
//! arguments and runs it or prints its `AsyncAPI` document.
//!
//! Services use it through the `ruststream` facade, as
//! [`ruststream::runtime::cli`](https://docs.rs/ruststream/latest/ruststream/runtime/cli/), whose
//! overview teaches it. The `tool` feature adds the driver of the `ruststream` binary.

#[cfg(feature = "tool")]
#[doc(hidden)]
pub mod tool;

use std::env;
#[cfg(feature = "asyncapi")]
use std::fs;
use std::io;
use std::num::NonZeroUsize;
use std::process::ExitCode;

use thiserror::Error;
use tokio::runtime::{Builder, Runtime};

#[cfg(feature = "asyncapi")]
use ruststream_asyncapi as asyncapi;
#[cfg(feature = "logging")]
use ruststream_observability::logging;
use ruststream_runtime_tokio::{App, RustStreamError};

/// Errors surfaced by the generated CLI entry point.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum CliError {
    /// The Tokio runtime could not be built.
    #[error("failed to build the async runtime: {0}")]
    Runtime(#[source] io::Error),
    /// The service returned an error while running (see [`RustStreamError`]).
    #[error(transparent)]
    Run(#[from] RustStreamError),
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
enum Command {
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

/// The Tokio runtime the generated `main` runs the service on: the `flavor` and `worker_threads`
/// arguments of [`#[ruststream::app]`](https://docs.rs/ruststream/latest/ruststream/attr.app.html), which mirror `#[tokio::main]`'s.
///
/// A service whose CPU-bound subscriptions run on dedicated threads (`threads(n)`) sizes the app's
/// runtime for its I/O alone: the dedicated threads and the runtime's workers share the machine's
/// cores. A current-thread runtime next to dedicated threads gives the rest of the service the
/// lowest latency.
///
/// # Examples
///
/// ```no_run
/// # {
/// use ruststream::memory::MemoryBroker;
/// use ruststream::runtime::cli::{AppRuntime, run_main_on};
/// use ruststream::runtime::{App, AppInfo, RustStream};
///
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("svc", "0.1.0")).register_broker(MemoryBroker::new())
/// }
///
/// // What `#[ruststream::app(flavor = "current_thread")]` expands to.
/// fn main() -> std::process::ExitCode {
///     run_main_on(AppRuntime::CurrentThread, app)
/// }
/// # }
/// ```
///
/// The attribute form, with a subscription that computes on eight threads of its own and reaches
/// the current-thread runtime only through [`MainRuntime`](ruststream_runtime_tokio::MainRuntime):
///
/// ```
/// # mod demo {
/// use ruststream::memory::prelude::*;
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Resize {
///     id: u64,
/// }
///
/// fn render(_job: &Resize) {}
///
/// async fn notify(_id: u64) {}
///
/// async fn lookup(id: u64) -> u64 {
///     id
/// }
///
/// #[subscriber("images.resize", threads(8))]
/// async fn resize(job: &Resize, Ctx(main): Ctx<MainRuntime>) -> HandlerOutcome {
///     // Runs on one of the subscription's eight threads, from the first poll to the end.
///     render(job);
///     // The explicit ways to the app's runtime; what they send must be `Send`.
///     main.spawn(notify(job.id));
///     match main.run(lookup(job.id)).await {
///         Ok(_) => HandlerOutcome::ack(),
///         Err(_) => HandlerOutcome::retry(),
///     }
/// }
///
/// #[ruststream::app(flavor = "current_thread")]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("images", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
///         b.include(resize);
///     })
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum AppRuntime {
    /// A multi-threaded runtime with Tokio's default of one worker per core.
    #[default]
    MultiThread,
    /// A multi-threaded runtime with this many workers (`worker_threads = n`).
    MultiThreadWorkers(NonZeroUsize),
    /// A runtime on the thread that calls `main` (`flavor = "current_thread"`).
    CurrentThread,
}

impl AppRuntime {
    /// Builds the runtime, with every driver the framework uses enabled.
    fn build(self) -> io::Result<Runtime> {
        let mut builder = match self {
            Self::MultiThread => Builder::new_multi_thread(),
            Self::MultiThreadWorkers(count) => {
                let mut builder = Builder::new_multi_thread();
                builder.worker_threads(count.get());
                builder
            }
            Self::CurrentThread => Builder::new_current_thread(),
        };
        builder.enable_all().build()
    }
}

/// Runs the generated CLI for `build`'s service, returning a process exit code.
///
/// Called by the [`#[ruststream::app]`](https://docs.rs/ruststream/latest/ruststream/attr.app.html) expansion; you rarely call it directly.
/// It reads the process arguments, dispatches to `run` or `asyncapi gen`, and prints any error to
/// stderr.
///
/// # Examples
///
/// ```no_run
/// # {
/// use ruststream::memory::MemoryBroker;
/// use ruststream::runtime::{App, AppInfo, RustStream};
///
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("svc", "0.1.0")).register_broker(MemoryBroker::new())
/// }
///
/// fn main() -> std::process::ExitCode {
///     ruststream::runtime::cli::run_main(app)
/// }
/// # }
/// ```
#[must_use]
pub fn run_main<A, F>(build: F) -> ExitCode
where
    A: App,
    F: FnOnce() -> A,
{
    run_main_on(AppRuntime::default(), build)
}

/// [`run_main`] on the runtime `runtime` names: what `#[ruststream::app]` expands to when it
/// carries `flavor` or `worker_threads`.
///
/// # Examples
///
/// ```no_run
/// # {
/// use ruststream::memory::MemoryBroker;
/// use ruststream::nonzero;
/// use ruststream::runtime::cli::{AppRuntime, run_main_on};
/// use ruststream::runtime::{App, AppInfo, RustStream};
///
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("svc", "0.1.0")).register_broker(MemoryBroker::new())
/// }
///
/// fn main() -> std::process::ExitCode {
///     run_main_on(AppRuntime::MultiThreadWorkers(nonzero!(2)), app)
/// }
/// # }
/// ```
#[must_use]
pub fn run_main_on<A, F>(runtime: AppRuntime, build: F) -> ExitCode
where
    A: App,
    F: FnOnce() -> A,
{
    let args: Vec<String> = env::args().skip(1).collect();
    report(execute(&args, runtime, build))
}

/// Maps a dispatch outcome onto the process exit code, printing the error to stderr. Split from
/// [`run_main`] so the mapping is reachable without the process arguments.
fn report(outcome: Result<(), CliError>) -> ExitCode {
    match outcome {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("ruststream: {err}");
            ExitCode::FAILURE
        }
    }
}

fn execute<A, F>(args: &[String], runtime: AppRuntime, build: F) -> Result<(), CliError>
where
    A: App,
    F: FnOnce() -> A,
{
    match parse(args)? {
        Command::Run => {
            // Install the colored console logger so a freshly scaffolded service prints logs out
            // of the box. Ignore the error: it only fails if the app already installed its own
            // subscriber, which we must not replace.
            #[cfg(feature = "logging")]
            let _ = logging::init();
            let app = build();
            let runtime = runtime.build().map_err(CliError::Runtime)?;
            runtime.block_on(app.run())?;
            Ok(())
        }
        Command::AsyncApiGen { out, yaml } => generate_spec(&build(), out.as_deref(), yaml),
    }
}

fn parse(args: &[String]) -> Result<Command, CliError> {
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
fn generate_spec<A: App>(app: &A, out: Option<&str>, yaml: bool) -> Result<(), CliError> {
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
fn generate_spec<A: App>(_app: &A, _out: Option<&str>, _yaml: bool) -> Result<(), CliError> {
    Err(CliError::AsyncApiDisabled)
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "asyncapi")]
    use std::env;
    #[cfg(feature = "asyncapi")]
    use std::fs;
    use std::io;
    #[cfg(feature = "asyncapi")]
    use std::process;
    use std::process::ExitCode;

    use ruststream::memory::MemoryBroker;
    use ruststream_runtime_tokio::{App, AppInfo, RustStream, RustStreamError};

    // `execute` drives a built service, so only the tests that can build one reach it.
    use super::execute;
    use super::{CliError, Command, parse, report};

    /// `execute` on the runtime `run_main` builds.
    fn execute_default<A, F>(args: &[String], build: F) -> Result<(), CliError>
    where
        A: App,
        F: FnOnce() -> A,
    {
        execute(args, super::AppRuntime::default(), build)
    }

    fn args(parts: &[&str]) -> Vec<String> {
        parts.iter().map(|s| (*s).to_owned()).collect()
    }

    /// `ExitCode` is opaque (no `PartialEq`), so compare the rendered form.
    fn code_of(outcome: Result<(), CliError>) -> String {
        format!("{:?}", report(outcome))
    }

    #[test]
    fn empty_args_default_to_run() {
        assert!(matches!(parse(&args(&[])).unwrap(), Command::Run));
        assert!(matches!(parse(&args(&["run"])).unwrap(), Command::Run));
    }

    #[test]
    fn asyncapi_gen_parses_output_and_format() {
        let cmd = parse(&args(&["asyncapi", "gen", "-o", "spec.yaml", "--yaml"])).unwrap();
        let Command::AsyncApiGen { out, yaml } = cmd else {
            panic!("expected asyncapi gen");
        };
        assert_eq!(out.as_deref(), Some("spec.yaml"));
        assert!(yaml);
    }

    #[test]
    fn asyncapi_gen_defaults_to_json_stdout() {
        let cmd = parse(&args(&["asyncapi", "gen"])).unwrap();
        let Command::AsyncApiGen { out, yaml } = cmd else {
            panic!("expected asyncapi gen");
        };
        assert!(out.is_none());
        assert!(!yaml);
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

    /// The service the dispatch tests generate a spec for: one broker, no handlers.
    fn demo_app() -> impl App {
        RustStream::new(AppInfo::new("cli-demo", "0.1.0")).register_broker(MemoryBroker::new())
    }

    #[test]
    fn an_unknown_command_surfaces_before_the_service_runs() {
        let outcome = execute_default(&args(&["frobnicate"]), demo_app);
        let err = outcome.expect_err("an unrecognized command must not run the service");
        assert!(matches!(err, CliError::UnknownCommand(cmd) if cmd == "frobnicate"));
        assert_eq!(
            code_of(execute_default(&args(&["asyncapi", "lint"]), demo_app)),
            format!("{:?}", ExitCode::FAILURE),
        );
    }

    /// The `run` arm builds its own runtime and drives the service to completion, so a plain
    /// `#[test]` (no ambient runtime) is required. A failing startup is what makes the arm
    /// observable without a termination signal.
    #[test]
    fn run_surfaces_a_startup_failure() {
        let build = || {
            RustStream::new(AppInfo::new("cli-broken", "0.1.0"))
                .on_startup(async move |()| {
                    Err::<(), _>(io::Error::other("state producer refused"))
                })
                .with_broker(MemoryBroker::new(), |_b| {})
        };
        let err = execute_default(&args(&["run"]), build).expect_err("the failing state producer");
        assert!(
            matches!(err, CliError::Run(RustStreamError::Startup(_))),
            "got: {err:?}",
        );
    }

    #[cfg(feature = "asyncapi")]
    #[test]
    fn asyncapi_gen_writes_json_and_yaml_to_a_file() {
        let dir = env::temp_dir();
        let json_path = dir.join(format!("ruststream-cli-{}-spec.json", process::id()));
        let json_out = json_path.to_str().expect("utf-8 temp path").to_owned();
        execute_default(
            &args(&["asyncapi", "gen", "-o", json_out.as_str()]),
            demo_app,
        )
        .expect("the spec must be written");
        let text = fs::read_to_string(&json_path).expect("the spec file exists");
        let spec: serde_json::Value = serde_json::from_str(&text).expect("valid JSON");
        assert_eq!(spec["info"]["title"], "cli-demo");
        fs::remove_file(&json_path).expect("cleanup");

        let yaml_path = dir.join(format!("ruststream-cli-{}-spec.yaml", process::id()));
        let yaml_out = yaml_path.to_str().expect("utf-8 temp path").to_owned();
        execute_default(
            &args(&["asyncapi", "gen", "-o", yaml_out.as_str(), "--yaml"]),
            demo_app,
        )
        .expect("the spec must be written");
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
        execute_default(&args(&["asyncapi", "gen"]), demo_app).expect("the spec must reach stdout");
    }

    #[cfg(feature = "asyncapi")]
    #[test]
    fn asyncapi_gen_reports_the_path_it_could_not_write() {
        let missing = env::temp_dir()
            .join("ruststream-cli-absent-directory")
            .join("spec.json");
        let out = missing.to_str().expect("utf-8 temp path").to_owned();
        let err = execute_default(&args(&["asyncapi", "gen", "-o", out.as_str()]), demo_app)
            .expect_err("a spec cannot be written into a missing directory");
        let CliError::WriteSpec { path, .. } = err else {
            panic!("expected a write error, got: {err:?}");
        };
        assert_eq!(path, out, "the error names the output path");
    }

    #[cfg(not(feature = "asyncapi"))]
    #[test]
    fn asyncapi_gen_without_the_feature_is_rejected() {
        let err = execute_default(&args(&["asyncapi", "gen"]), demo_app)
            .expect_err("the spec needs the asyncapi feature");
        assert!(matches!(err, CliError::AsyncApiDisabled), "got: {err:?}");
    }

    /// The runtime the generated `main` builds, per `#[ruststream::app]` argument.
    mod runtimes {
        use std::sync::{Arc, Barrier, mpsc};
        use std::time::Duration;

        use tokio::runtime::RuntimeFlavor;

        use super::super::AppRuntime;
        use ruststream::nonzero;

        #[test]
        fn the_default_is_multi_threaded() {
            let runtime = AppRuntime::default().build().expect("the runtime builds");
            assert_eq!(
                runtime.handle().runtime_flavor(),
                RuntimeFlavor::MultiThread
            );
        }

        #[test]
        fn a_current_thread_flavor_builds_a_current_thread_runtime() {
            let runtime = AppRuntime::CurrentThread
                .build()
                .expect("the runtime builds");
            assert_eq!(
                runtime.handle().runtime_flavor(),
                RuntimeFlavor::CurrentThread
            );
        }

        /// Three tasks that each hold a worker until all three arrive pass only with three
        /// workers.
        #[test]
        fn a_worker_count_starts_that_many_workers() {
            let runtime = AppRuntime::MultiThreadWorkers(nonzero!(3))
                .build()
                .expect("the runtime builds");
            assert_eq!(
                runtime.handle().runtime_flavor(),
                RuntimeFlavor::MultiThread
            );
            let barrier = Arc::new(Barrier::new(3));
            let (passed, arrivals) = mpsc::channel();
            for _ in 0..3 {
                let (barrier, passed) = (Arc::clone(&barrier), passed.clone());
                runtime.spawn(async move {
                    barrier.wait();
                    let _ = passed.send(());
                });
            }
            let arrived = (0..3).all(|_| arrivals.recv_timeout(Duration::from_secs(5)).is_ok());
            // Workers stuck on the barrier would hold a dropped runtime forever.
            runtime.shutdown_background();
            assert!(arrived, "three workers hold the barrier at once");
        }
    }
}
