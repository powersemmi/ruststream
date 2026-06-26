//! The sealed [`App`] trait: the functional surface a built service exposes, so a builder can
//! return `impl App` instead of spelling the full `RustStream<L, St, PP>` type.

use std::collections::BTreeMap;
use std::future::Future;

use crate::ServerSpec;
use crate::runtime::metadata::HandlerMetadata;

use super::{AppInfo, RustStream, RustStreamError};

mod sealed {
    pub trait Sealed {}
    impl<L, St, PP> Sealed for super::RustStream<L, St, PP> {}
}

/// The functional surface of a built [`RustStream`] service: run it, and read the metadata the
/// [`AsyncAPI`](crate::asyncapi) generator and the generated CLI need.
///
/// Implemented only by [`RustStream`] (the trait is sealed), so a builder function can hide the
/// composed middleware / state / publish-pipeline type parameters behind `impl App` instead of
/// naming `RustStream<Stack<..>, St, PublishStack<..>>` in full:
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

    /// The service metadata (title, version, description): the `AsyncAPI` `info` object.
    fn info(&self) -> &AppInfo;

    /// The registered `AsyncAPI` servers, keyed by name.
    fn servers(&self) -> &BTreeMap<String, ServerSpec>;

    /// Metadata for every registered handler, in registration order: input to the `AsyncAPI`
    /// generator.
    fn handlers(&self) -> &[HandlerMetadata];
}

// `PP: Send` is what makes the run futures `Send`: `run` consumes `self`, which holds the publish
// pipeline. Every real pipeline is `Send` (the `PublishLayer` trait requires `Send + Sync`), so the
// bound never gets in the way; it just lets the type system see what is already true.
//
// Each method names `RustStream` explicitly rather than `Self`: the inherent methods share these
// names with the trait, so spelling the type makes clear the call delegates to the inherent method
// (which wins by priority) and does not recurse into the trait method.
#[allow(clippy::use_self)]
impl<L: Send, St: Send + Sync, PP: Send> App for RustStream<L, St, PP> {
    fn run(self) -> impl Future<Output = Result<(), RustStreamError>> + Send {
        RustStream::run(self)
    }

    fn run_until<F>(self, shutdown: F) -> impl Future<Output = Result<(), RustStreamError>> + Send
    where
        F: Future<Output = ()> + Send,
    {
        RustStream::run_until(self, shutdown)
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
