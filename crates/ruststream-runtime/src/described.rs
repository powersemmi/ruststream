//! [`Described`]: what a built service tells about itself, whatever runner hosts it.

use alloc::collections::BTreeMap;
use alloc::string::String;

use crate::{AppInfo, HandlerMetadata, ServerSpec};

/// What a built service documents about itself: its metadata, its servers and its handlers.
///
/// The `AsyncAPI` generator reads only this, so one document builder serves every runner. A runner's
/// app type implements it; a service rarely names it, because the runner's own app trait already
/// requires it.
///
/// # Examples
///
/// ```
/// use ruststream::memory::MemoryBroker;
/// use ruststream::runtime::{AppInfo, RustStream};
/// use ruststream_runtime::Described;
///
/// /// The title and the number of handlers of any built service.
/// fn summary(app: &impl Described) -> String {
///     format!("{}: {} handlers", app.info().title, app.handlers().len())
/// }
///
/// let app = RustStream::new(AppInfo::new("orders", "0.1.0")).register_broker(MemoryBroker::new());
/// assert_eq!(summary(&app), "orders: 0 handlers");
/// ```
pub trait Described {
    /// The service metadata (title, version, description): the `AsyncAPI` `info` object.
    fn info(&self) -> &AppInfo;

    /// The registered servers, keyed by name.
    fn servers(&self) -> &BTreeMap<String, ServerSpec>;

    /// Metadata for every registered handler, in registration order.
    fn handlers(&self) -> &[HandlerMetadata];

    /// Applies the logger the service selected, so a tool that reads the service without running
    /// it (the `AsyncAPI` generator) reports through it too. A service that selected none, and a
    /// description that is not a runner's app, report nothing.
    #[doc(hidden)]
    fn select_logger(&self) {}
}
