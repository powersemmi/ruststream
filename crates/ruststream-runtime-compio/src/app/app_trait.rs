//! The sealed [`App`] trait: the functional surface a built service exposes, so a builder can
//! return `impl App` instead of spelling the full `RustStream<Layers, State, Pipeline>` type.

// The start and stop path runs on the task that awaits it and is never spawned.
#![allow(clippy::future_not_send)]

use std::collections::BTreeMap;

use crate::ServerSpec;
use crate::{Cores, Described, HandlerMetadata};

use super::{AppInfo, RunningApp, RustStream, RustStreamError};

mod sealed {
    #[cfg(feature = "testing")]
    use crate::{InProcessStart, RunningApp, StartFailure};

    /// Closes [`App`](super::App) to [`RustStream`](super::RustStream), and carries what the test
    /// harness needs from an app a builder hands it as `impl App`.
    pub trait Sealed {
        /// The state the app's handlers read, which the test harness is typed by.
        #[cfg(feature = "testing")]
        type State: Send + Sync + 'static;

        /// Starts the app in process on the calling runtime.
        #[cfg(feature = "testing")]
        fn start_in_process(
            self,
            start: InProcessStart<Self::State>,
        ) -> impl Future<Output = Result<RunningApp, StartFailure>>;
    }

    #[cfg(not(feature = "testing"))]
    impl<Layers, State, Pipeline, Phase> Sealed for super::RustStream<Layers, State, Pipeline, Phase> {}

    // The inherent method shares the name; spelling the type makes the call delegate to it.
    #[cfg(feature = "testing")]
    #[allow(clippy::use_self)]
    impl<Layers: Send, State, Pipeline, Phase> Sealed
        for super::RustStream<Layers, State, Pipeline, Phase>
    where
        State: Send + Sync + 'static,
    {
        type State = State;

        fn start_in_process(
            self,
            start: InProcessStart<State>,
        ) -> impl Future<Output = Result<RunningApp, StartFailure>> {
            super::RustStream::start_in_process(self, start)
        }
    }
}

/// The functional surface of a built [`RustStream`] service: run it, and read the metadata the
/// [`AsyncAPI`](https://docs.rs/ruststream/latest/ruststream/asyncapi/index.html) generator and
/// the generated CLI need.
///
/// Implemented only by [`RustStream`] (the trait is sealed), so a builder function can hide the
/// composed middleware / state / publish-pipeline type parameters behind `impl App` instead of
/// naming `RustStream<Stack<..>, State, PublishStack<..>>` in full:
///
/// ```
/// use ruststream_memory::local::LocalMemoryBroker;
/// use ruststream_runtime::AppInfo;
/// use ruststream_runtime_compio::{App, RustStream};
///
/// // The return type stays `impl App` however many layers the body composes onto the pipeline.
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("svc", "0.1.0")).register_broker(LocalMemoryBroker::<4>::new())
/// }
///
/// let app = app();
/// assert_eq!(app.info().title, "svc");
/// ```
///
/// The inherent `RustStream::run` / `RustStream::info` methods stay, so naming the concrete type
/// keeps working; this trait only adds the type-erased surface the CLI and spec generator consume.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a compio app",
    note = "an app built with `ruststream_runtime_compio::RustStream` is one; the tokio app runs \
            on the tokio runner"
)]
pub trait App: sealed::Sealed + Described + Sized {
    /// Runs the service on `cores` until the process is asked to end (`SIGINT` / `SIGTERM`), then
    /// shuts down gracefully. See [`RustStream::run`].
    ///
    /// # Errors
    ///
    /// Returns [`RustStreamError`] if the mounts do not fit the cores, a broker fails to connect, a
    /// subscription fails to open, a dispatch task panics, a core fails, or a broker fails to shut
    /// down.
    fn run(self, cores: Cores) -> Result<(), RustStreamError>;

    /// Runs the service on `cores` until `shutdown` resolves, then shuts down gracefully. See
    /// [`RustStream::run_until`].
    ///
    /// # Errors
    ///
    /// Returns [`RustStreamError`] under the same conditions as [`run`](Self::run).
    fn run_until<Signal>(self, cores: Cores, shutdown: Signal) -> Result<(), RustStreamError>
    where
        Signal: Future<Output = ()> + Send;

    /// Starts the service in the background of the calling runtime and hands back a
    /// [`RunningApp`] handle. See [`RustStream::start`].
    ///
    /// # Errors
    ///
    /// Returns [`RustStreamError`] if the mounts do not fit one core, the state producer or an
    /// `after_startup` hook fails, a broker fails to connect, or a subscription fails to open.
    fn start(self) -> impl Future<Output = Result<RunningApp, RustStreamError>>;

    /// The service metadata (title, version, description): the `AsyncAPI` `info` object.
    fn info(&self) -> &AppInfo;

    /// The registered `AsyncAPI` servers, keyed by name.
    fn servers(&self) -> &BTreeMap<String, ServerSpec>;

    /// Metadata for every registered handler, in registration order: input to the `AsyncAPI`
    /// generator.
    fn handlers(&self) -> &[HandlerMetadata];
}

// Each method names `RustStream` explicitly rather than `Self`: the inherent methods share these
// names with the trait, so spelling the type makes clear the call delegates to the inherent method
// (which wins by priority) and does not recurse into the trait method.
#[allow(clippy::use_self)]
impl<Layers: Send, State: Send + Sync + 'static, Pipeline: Send, Phase> App
    for RustStream<Layers, State, Pipeline, Phase>
{
    fn run(self, cores: Cores) -> Result<(), RustStreamError> {
        RustStream::run(self, cores)
    }

    fn run_until<Signal>(self, cores: Cores, shutdown: Signal) -> Result<(), RustStreamError>
    where
        Signal: Future<Output = ()> + Send,
    {
        RustStream::run_until(self, cores, shutdown)
    }

    fn start(self) -> impl Future<Output = Result<RunningApp, RustStreamError>> {
        RustStream::start(self)
    }

    fn info(&self) -> &AppInfo {
        RustStream::info(self)
    }

    fn servers(&self) -> &BTreeMap<String, ServerSpec> {
        RustStream::servers(self)
    }

    fn handlers(&self) -> &[HandlerMetadata] {
        RustStream::handlers(self)
    }
}
