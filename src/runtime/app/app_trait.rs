//! The sealed [`App`] trait: the functional surface a built service exposes, so a builder can
//! return `impl App` instead of spelling the full `RustStream<Layers, State, Pipeline>` type.

use std::collections::BTreeMap;
use std::future::Future;

use crate::ServerSpec;
use crate::runtime::metadata::HandlerMetadata;

use super::{AppInfo, RunningApp, RustStream, RustStreamError};

mod sealed {
    #[cfg(feature = "testing")]
    use crate::runtime::TestParts;

    /// Closes [`App`](super::App) to [`RustStream`](super::RustStream), and carries what the test
    /// harness needs from an app a builder hands it as `impl App`.
    pub trait Sealed {
        /// The state the app's handlers read, which the test harness is typed by.
        #[cfg(feature = "testing")]
        type State: Send + Sync + 'static;

        /// Decomposes the app into what the test harness drives.
        #[cfg(feature = "testing")]
        fn into_test_parts(self) -> TestParts<Self::State>;
    }

    #[cfg(not(feature = "testing"))]
    impl<Layers, State, Pipeline, Phase> Sealed for super::RustStream<Layers, State, Pipeline, Phase> {}

    // The inherent method shares the name, and spelling the type makes clear the call delegates
    // to it rather than recursing into this one, as in the `App` impl below.
    #[cfg(feature = "testing")]
    #[allow(clippy::use_self)]
    impl<Layers, State, Pipeline, Phase> Sealed for super::RustStream<Layers, State, Pipeline, Phase>
    where
        State: Send + Sync + 'static,
    {
        type State = State;

        fn into_test_parts(self) -> TestParts<State> {
            super::RustStream::into_test_parts(self)
        }
    }
}

/// The functional surface of a built [`RustStream`] service: run it, and read the metadata the
/// [`AsyncAPI`](crate::asyncapi) generator and the generated CLI need.
///
/// Implemented only by [`RustStream`] (the trait is sealed), so a builder function can hide the
/// composed middleware / state / publish-pipeline type parameters behind `impl App` instead of
/// naming `RustStream<Stack<..>, State, PublishStack<..>>` in full:
///
/// ```no_run
/// # #[cfg(feature = "memory")]
/// # {
/// use ruststream::memory::MemoryBroker;
/// use ruststream::runtime::{App, AppInfo, RustStream};
///
/// // The return type stays `impl App` however many layers the body composes onto the pipeline.
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("svc", "0.1.0")).register_broker(MemoryBroker::new())
/// }
/// # let _ = app;
/// # }
/// ```
///
/// The inherent `RustStream::run` / `RustStream::info` methods stay, so naming the concrete type
/// keeps working; this trait only adds the type-erased surface the CLI and spec generator consume.
/// The run futures are `Send` because the service is driven across task boundaries (the CLI's
/// `block_on`, `tokio::spawn` in tests), so the bound belongs in the signature.
pub trait App: sealed::Sealed + Sized {
    /// Runs the service until an interrupt (`SIGINT` / `SIGTERM`), then shuts down gracefully.
    ///
    /// # Errors
    ///
    /// Returns [`RustStreamError`] if a broker fails to connect, a subscription fails to open, a
    /// dispatch task panics, or a broker fails to shut down.
    fn run(self) -> impl Future<Output = Result<(), RustStreamError>> + Send;

    /// Runs the service until `shutdown` resolves, then shuts down gracefully.
    ///
    /// # Errors
    ///
    /// Returns [`RustStreamError`] under the same conditions as [`run`](Self::run).
    fn run_until<F>(self, shutdown: F) -> impl Future<Output = Result<(), RustStreamError>> + Send
    where
        F: Future<Output = ()> + Send;

    /// Starts the service in the background and hands back a [`RunningApp`] handle: the
    /// side-by-side form for hosting the service next to another foreground server. See
    /// [`RustStream::start`].
    ///
    /// # Errors
    ///
    /// Returns [`RustStreamError`] if the state producer or an `after_startup` hook fails, a
    /// broker fails to connect, or a subscription fails to open.
    fn start(self) -> impl Future<Output = Result<RunningApp, RustStreamError>> + Send;

    /// The service metadata (title, version, description): the `AsyncAPI` `info` object.
    fn info(&self) -> &AppInfo;

    /// The registered `AsyncAPI` servers, keyed by name.
    fn servers(&self) -> &BTreeMap<String, ServerSpec>;

    /// Metadata for every registered handler, in registration order: input to the `AsyncAPI`
    /// generator.
    fn handlers(&self) -> &[HandlerMetadata];
}

// `Pipeline: Send` is what makes the run futures `Send`: `run` consumes `self`, which holds the
// publish pipeline. `State: 'static` mirrors the inherent impl in run.rs: `start` needs it to bind
// the state into the shutdown hooks.
//
// Each method names `RustStream` explicitly rather than `Self`: the inherent methods share these
// names with the trait, so spelling the type makes clear the call delegates to the inherent method
// (which wins by priority) and does not recurse into the trait method.
#[allow(clippy::use_self)]
impl<Layers: Send, State: Send + Sync + 'static, Pipeline: Send, Phase> App
    for RustStream<Layers, State, Pipeline, Phase>
{
    fn run(self) -> impl Future<Output = Result<(), RustStreamError>> + Send {
        RustStream::run(self)
    }

    fn run_until<F>(self, shutdown: F) -> impl Future<Output = Result<(), RustStreamError>> + Send
    where
        F: Future<Output = ()> + Send,
    {
        RustStream::run_until(self, shutdown)
    }

    fn start(self) -> impl Future<Output = Result<RunningApp, RustStreamError>> + Send {
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
