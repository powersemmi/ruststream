//! The [`RustStream`] application object: binds brokers, handlers and lifecycle into one runnable
//! service.

mod app_trait;
mod health;
mod include;
mod run;
mod scope;
mod service;

pub use app_trait::App;
pub use health::{HealthProbe, HealthState};
#[doc(hidden)]
pub use include::{IncludeMount, ScopeCommit};
pub use include::{Mounting, MountingSlots};
pub use run::RunningApp;
pub use scope::BrokerScope;
#[cfg(feature = "testing")]
pub(crate) use service::{RegisteredBroker, TestParts};
pub use service::{RustStream, Setup, Wired};

use std::sync::Arc;

use thiserror::Error;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::describe::{AppId, Contact, ExternalDocs, License, Tag};
use crate::runtime::failure::ErrorShutdown;
use crate::runtime::lifecycle::{BoxError, BoxFuture};

/// A registration deferred until [`RustStream::run`]: given the app's error-shutdown handle and the
/// shutdown token, it opens the subscription (after the broker is connected) and spawns the dispatch
/// task. The broker, source and handler are captured and type-erased.
pub(crate) type Starter<St> = Box<
    dyn FnOnce(
            Arc<St>,
            ErrorShutdown,
            CancellationToken,
        ) -> BoxFuture<'static, Result<JoinHandle<()>, BoxError>>
        + Send,
>;

/// The state initializer: produces the app state `St` once at startup (before brokers connect).
/// The `on_startup` producer chain; a failing initializer aborts startup. The default is the unit
/// state `()`.
pub(crate) type StateInit<St> =
    Box<dyn FnOnce() -> BoxFuture<'static, Result<St, BoxError>> + Send>;

/// A read-only lifespan hook (`after_startup` / `on_shutdown` / `after_shutdown`): runs once at the
/// corresponding lifecycle point with a shared `Arc<St>` handle to the app state.
pub(crate) type LifecycleHook<St> =
    Box<dyn FnOnce(Arc<St>) -> BoxFuture<'static, Result<(), BoxError>> + Send>;

/// Which read-only lifecycle hook list a hook is appended to.
#[derive(Clone, Copy)]
enum LifecyclePhase {
    AfterStartup,
    OnShutdown,
    AfterShutdown,
}

/// Service-level metadata, surfaced to the `AsyncAPI` generator as the spec `Info` object.
///
/// The title and the version are what a service must state; everything else is optional and
/// reaches the document only when it is filled in.
///
/// # Examples
///
/// ```
/// use ruststream::runtime::AppInfo;
/// use ruststream::{Contact, License, Tag};
///
/// let info = AppInfo::new("orders", "1.4.0")
///     .with_description("everything the order domain publishes")
///     .with_contact(Contact::new().with_email("payments@example.com"))
///     .with_license(License::new("Apache-2.0"))
///     .with_tag(Tag::new("payments"));
///
/// assert_eq!(info.tags.len(), 1);
/// ```
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct AppInfo {
    /// Human-readable service title.
    pub title: String,
    /// Service version string.
    pub version: String,
    /// Optional longer description.
    pub description: Option<String>,
    /// The service's own identifier, a URI. Emitted as the document's root `id`.
    pub id: Option<AppId>,
    /// Where the terms of service are published.
    pub terms_of_service: Option<String>,
    /// Who to contact about the service.
    pub contact: Contact,
    /// The licence the service is published under.
    pub license: Option<License>,
    /// Labels grouping the service among its neighbours.
    pub tags: Vec<Tag>,
    /// Where the prose about this service lives.
    pub external_docs: Option<ExternalDocs>,
}

impl AppInfo {
    /// Creates info with a title and version and nothing else.
    #[must_use]
    pub fn new(title: impl Into<String>, version: impl Into<String>) -> Self {
        Self {
            title: title.into(),
            version: version.into(),
            description: None,
            id: None,
            terms_of_service: None,
            contact: Contact::new(),
            license: None,
            tags: Vec::new(),
            external_docs: None,
        }
    }

    /// Sets the description.
    #[must_use]
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Sets the service's own identifier.
    ///
    /// The identifier is a URI, which [`AppId`] checks on construction, so the document cannot
    /// carry a title where a reader expects an identifier.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::AppId;
    /// use ruststream::runtime::AppInfo;
    ///
    /// let info = AppInfo::new("orders", "1.0.0").with_id("urn:example:orders".parse::<AppId>()?);
    ///
    /// assert_eq!(info.id.as_ref().map(AppId::as_str), Some("urn:example:orders"));
    /// # Ok::<_, ruststream::AppIdError>(())
    /// ```
    #[must_use]
    pub fn with_id(mut self, id: AppId) -> Self {
        self.id = Some(id);
        self
    }

    /// Sets where the terms of service are published.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::runtime::AppInfo;
    ///
    /// let info = AppInfo::new("orders", "1.0.0").with_terms_of_service("https://example.com/tos");
    ///
    /// assert!(info.terms_of_service.is_some());
    /// ```
    #[must_use]
    pub fn with_terms_of_service(mut self, url: impl Into<String>) -> Self {
        self.terms_of_service = Some(url.into());
        self
    }

    /// Sets who to contact about the service.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::Contact;
    /// use ruststream::runtime::AppInfo;
    ///
    /// let info = AppInfo::new("orders", "1.0.0")
    ///     .with_contact(Contact::new().with_name("Payments team"));
    ///
    /// assert_eq!(info.contact.name.as_deref(), Some("Payments team"));
    /// ```
    #[must_use]
    pub fn with_contact(mut self, contact: Contact) -> Self {
        self.contact = contact;
        self
    }

    /// Sets the licence the service is published under.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::License;
    /// use ruststream::runtime::AppInfo;
    ///
    /// let info = AppInfo::new("orders", "1.0.0").with_license(License::new("MIT"));
    ///
    /// assert_eq!(info.license.map(|license| license.name).as_deref(), Some("MIT"));
    /// ```
    #[must_use]
    pub fn with_license(mut self, license: License) -> Self {
        self.license = Some(license);
        self
    }

    /// Adds one label to the service. Call repeatedly for several.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::Tag;
    /// use ruststream::runtime::AppInfo;
    ///
    /// let info = AppInfo::new("orders", "1.0.0")
    ///     .with_tag(Tag::new("payments"))
    ///     .with_tag(Tag::new("public"));
    ///
    /// assert_eq!(info.tags.len(), 2);
    /// ```
    #[must_use]
    pub fn with_tag(mut self, tag: Tag) -> Self {
        self.tags.push(tag);
        self
    }

    /// Sets where the prose about this service lives.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::ExternalDocs;
    /// use ruststream::runtime::AppInfo;
    ///
    /// let info = AppInfo::new("orders", "1.0.0")
    ///     .with_external_docs(ExternalDocs::new("https://example.com/orders"));
    ///
    /// assert!(info.external_docs.is_some());
    /// ```
    #[must_use]
    pub fn with_external_docs(mut self, docs: ExternalDocs) -> Self {
        self.external_docs = Some(docs);
        self
    }
}

/// Errors surfaced while running a [`RustStream`] service.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum RustStreamError {
    /// A broker failed to [`connect`](crate::Broker::connect) at startup.
    #[error("broker connect failed: {0}")]
    Connect(#[source] BoxError),
    /// An `on_startup` or `after_startup` lifespan hook failed.
    #[error("startup hook failed: {0}")]
    Startup(#[source] BoxError),
    /// A subscription failed to open after connect.
    #[error("subscription failed: {0}")]
    Subscribe(#[source] BoxError),
    /// A broker failed to [`shutdown`](crate::ConnectedBroker::shutdown) during graceful
    /// shutdown.
    #[error("broker shutdown failed: {0}")]
    Shutdown(#[source] BoxError),
    /// A dispatch task panicked or was aborted.
    #[error("dispatch task failed: {0}")]
    Join(#[source] tokio::task::JoinError),
    /// A subscriber hit a fail-fast failure (a handler panic, or a decode failure under
    /// `on_failure(decode = fail_fast)`) and tore the service down. The string names the
    /// subscription and the reason.
    #[error("dispatch failed: {0}")]
    Dispatch(String),
}

/// Boxing helpers for scope-registered lifecycle hooks.
pub(super) mod lifecycle_hooks {
    use std::{error::Error as StdError, future::Future, sync::Arc};

    use crate::runtime::lifecycle::{BoxError, ConnectedSlot};
    use crate::runtime::publish_source::pair_bound;
    use crate::{Broker, Connected, PublishPolicy};

    use super::LifecycleHook;

    /// Erases a scope-level startup publish (pair `source`, run `hook` with the live publisher)
    /// into an app lifecycle hook. The state handle is ignored: the hook's input is the
    /// publisher, and app state stays reachable through the app-level `after_startup`.
    pub(crate) fn box_startup_publish<B, State, Source, Hook, Fut, E>(
        slot: ConnectedSlot<B>,
        source: Source,
        hook: Hook,
    ) -> LifecycleHook<State>
    where
        B: Broker + 'static,
        Source: PublishPolicy<Connected<B>> + Send + 'static,
        Source::Live: Send,
        Hook: FnOnce(Source::Live) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
        E: StdError + Send + Sync + 'static,
    {
        Box::new(move |_state: Arc<State>| {
            Box::pin(async move {
                let live = pair_bound::<B, Source>(&slot, source)
                    .await
                    .map_err(|e| Box::new(e) as BoxError)?;
                hook(live).await.map_err(|e| Box::new(e) as BoxError)
            })
        })
    }
}
