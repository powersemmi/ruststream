//! The compio entry point: the core count the generated `main` runs the service on, and the
//! dispatch of a parsed command onto a compio app.

use std::env;
use std::num::NonZeroUsize;
use std::process::ExitCode;

#[cfg(feature = "logging")]
use ruststream_observability::logging;
use ruststream_runtime_compio::{App, AppDescription, Cores};

use crate::{CliError, Command, generate_spec, parse, report};

/// Runs the generated CLI for `build`'s compio service on `cores` cores (every core the process
/// may use when `None`), returning a process exit code.
///
/// Called by the `#[ruststream::app(runtime = compio)]` expansion; you rarely call it directly.
/// It reads the process arguments, dispatches to `run` or `asyncapi gen`, and prints any error to
/// stderr.
///
/// # Examples
///
/// ```no_run
/// # {
/// use ruststream::memory::local::LocalMemoryBroker;
/// use ruststream::runtime::compio::{App, AppInfo, RustStream, run_main};
///
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("svc", "0.1.0")).register_broker(LocalMemoryBroker::<8>::new())
/// }
///
/// // What `#[ruststream::app(runtime = compio, cores = 2)]` expands to.
/// fn main() -> std::process::ExitCode {
///     run_main(Some(2), app)
/// }
/// # }
/// ```
#[must_use]
pub fn run_main<A, F>(cores: Option<usize>, build: F) -> ExitCode
where
    A: App,
    F: FnOnce() -> A,
{
    let args: Vec<String> = env::args().skip(1).collect();
    report(cores_of(cores).and_then(|cores| execute(&args, cores, build)))
}

/// The launcher for the count the expansion names: open means every core available.
fn cores_of(count: Option<usize>) -> Result<Cores, CliError> {
    count.map_or_else(
        || Ok(Cores::available()),
        |count| {
            NonZeroUsize::new(count)
                .map(Cores::new)
                .ok_or(CliError::NoCores)
        },
    )
}

fn execute<A, F>(args: &[String], cores: Cores, build: F) -> Result<(), CliError>
where
    A: App,
    F: FnOnce() -> A,
{
    match parse(args)? {
        Command::Run => {
            // The console logger, as on tokio; it only fails when the app installed its own.
            #[cfg(feature = "logging")]
            let _ = logging::init();
            build().run(cores)?;
            Ok(())
        }
        Command::AsyncApiGen { out, yaml } => {
            generate_spec(&AppDescription(&build()), out.as_deref(), yaml)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::io;
    use std::num::NonZeroUsize;
    use std::process::{self, Command, ExitCode};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use ruststream_memory::local::LocalMemoryBroker;
    use ruststream_runtime_compio::{App, AppInfo, Cores, RustStream, RustStreamError, yield_now};

    use super::{cores_of, execute};
    use crate::{CliError, report};

    fn args(parts: &[&str]) -> Vec<String> {
        parts.iter().map(|s| (*s).to_owned()).collect()
    }

    /// `ExitCode` is opaque (no `PartialEq`), so compare the rendered form.
    fn code_of(outcome: Result<(), CliError>) -> String {
        format!("{:?}", report(outcome))
    }

    fn one_core() -> Cores {
        Cores::new(NonZeroUsize::MIN).unpinned()
    }

    fn demo_app() -> impl App {
        RustStream::new(AppInfo::new("cli-demo", "0.1.0"))
            .register_broker(LocalMemoryBroker::<8>::new())
    }

    #[test]
    fn an_unknown_command_surfaces_before_the_service_runs() {
        let built = Arc::new(AtomicBool::new(false));
        let seen = Arc::clone(&built);
        let outcome = execute(&args(&["frobnicate"]), one_core(), move || {
            seen.store(true, Ordering::Relaxed);
            demo_app()
        });
        let err = outcome.expect_err("an unrecognized command must not run the service");
        assert!(matches!(err, CliError::UnknownCommand(cmd) if cmd == "frobnicate"));
        assert!(
            !built.load(Ordering::Relaxed),
            "the service is not even built"
        );
        assert_eq!(
            code_of(execute(&args(&["asyncapi", "lint"]), one_core(), demo_app)),
            format!("{:?}", ExitCode::FAILURE),
        );
    }

    /// A failing startup is what makes the `run` arm observable without a termination signal.
    #[test]
    fn run_surfaces_a_startup_failure_as_a_failing_exit() {
        let build = || {
            RustStream::new(AppInfo::new("cli-broken", "0.1.0"))
                .on_startup(async move |()| {
                    Err::<(), _>(io::Error::other("state producer refused"))
                })
                .with_broker(LocalMemoryBroker::<8>::new(), |b| b)
        };
        let err = execute(&args(&["run"]), one_core(), build).expect_err("the failing producer");
        assert!(
            matches!(err, CliError::RunCompio(RustStreamError::Startup(_))),
            "got: {err:?}",
        );
        assert_eq!(code_of(Err(err)), format!("{:?}", ExitCode::FAILURE));
    }

    /// `kill` sends the signal from the hook, once the cores are up and the signal is watched.
    #[cfg(unix)]
    #[test]
    fn run_serves_until_the_termination_signal_then_returns_cleanly() {
        let reached = Arc::new(AtomicBool::new(false));
        let seen = Arc::clone(&reached);
        let build = move || {
            RustStream::new(AppInfo::new("cli-serving", "0.1.0"))
                .after_startup(async move |_state: Arc<()>| {
                    // The signal listener is a task of this runtime: let it register first.
                    for _ in 0..8 {
                        yield_now().await;
                    }
                    seen.store(true, Ordering::Relaxed);
                    let sent = Command::new("kill")
                        .args(["-s", "TERM", &process::id().to_string()])
                        .status()
                        .expect("kill runs");
                    assert!(sent.success());
                    Ok::<_, Infallible>(())
                })
                .register_broker(LocalMemoryBroker::<8>::new())
        };
        execute(&args(&["run"]), one_core(), build).expect("the signal ends the run cleanly");
        assert!(
            reached.load(Ordering::Relaxed),
            "the service started before it was stopped"
        );
    }

    #[cfg(feature = "asyncapi")]
    #[test]
    fn a_compio_app_generates_its_spec_through_the_neutral_generator() {
        use std::{env, fs};

        let out = env::temp_dir().join(format!("ruststream-cli-{}-compio.json", process::id()));
        let path = out.to_str().expect("utf-8 temp path").to_owned();
        execute(
            &args(&["asyncapi", "gen", "-o", path.as_str()]),
            one_core(),
            demo_app,
        )
        .expect("the spec must be written");
        let text = fs::read_to_string(&out).expect("the spec file exists");
        assert!(text.contains("cli-demo"));
        fs::remove_file(&out).expect("cleanup");
    }

    #[cfg(not(feature = "asyncapi"))]
    #[test]
    fn asyncapi_gen_without_the_feature_is_rejected() {
        let err = execute(&args(&["asyncapi", "gen"]), one_core(), demo_app)
            .expect_err("the spec needs the asyncapi feature");
        assert!(matches!(err, CliError::AsyncApiDisabled), "got: {err:?}");
    }

    #[test]
    fn the_core_count_names_the_launcher() {
        assert_eq!(cores_of(Some(3)).expect("three cores").count().get(), 3);
        assert_eq!(cores_of(None).expect("every core"), Cores::available());
        assert!(matches!(cores_of(Some(0)), Err(CliError::NoCores)));
    }
}
