//! The tokio entry point: the runtime `main` runs the service on, and the dispatch of a parsed
//! command onto a tokio app.

use std::env;
use std::io;
use std::num::NonZeroUsize;
use std::process::ExitCode;

use tokio::runtime::{Builder, Runtime};

#[cfg(feature = "logging")]
use ruststream_observability::logging;
use ruststream_runtime_tokio::{App, AppDescription};

use crate::{CliError, Command, generate_spec, parse, report};

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
///         b.include(resize)
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
        Command::AsyncApiGen { out, yaml } => {
            generate_spec(&AppDescription(&build()), out.as_deref(), yaml)
        }
    }
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

    use super::execute;
    use crate::{CliError, report};

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
                .with_broker(MemoryBroker::new(), |b| b)
        };
        let err = execute_default(&args(&["run"]), build).expect_err("the failing state producer");
        assert!(
            matches!(err, CliError::Run(RustStreamError::Startup(_))),
            "got: {err:?}",
        );
    }

    #[cfg(feature = "asyncapi")]
    #[test]
    fn a_tokio_app_generates_its_spec_through_the_neutral_generator() {
        let out = env::temp_dir().join(format!("ruststream-cli-{}-tokio.json", process::id()));
        let path = out.to_str().expect("utf-8 temp path").to_owned();
        execute_default(&args(&["asyncapi", "gen", "-o", path.as_str()]), demo_app)
            .expect("the spec must be written");
        let text = fs::read_to_string(&out).expect("the spec file exists");
        assert!(text.contains("cli-demo"));
        fs::remove_file(&out).expect("cleanup");
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
