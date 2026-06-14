//! Per-delivery [`Context`] and the app-level [`State`] type-map.
//!
//! A `Context` is built for each delivery and threaded (as `&mut`) through the middleware chain
//! into the handler. It carries the channel the message arrived on, a working copy of the
//! headers (middleware may enrich them), and shared application state. The copy is lazy: the
//! message headers are borrowed until the first [`headers_mut`](Context::headers_mut), so a
//! delivery whose middleware never touches them pays no clone.

use std::any::{Any, TypeId};
use std::collections::HashMap;

use crate::Headers;

use super::dispatch::Delivery;
use super::failure::ErrorShutdown;
use super::publish::ScopedPublisher;

/// App-level shared state: a type-map holding one value per type.
///
/// Put shared resources (database pools, HTTP clients, configuration) in here with
/// [`RustStream::insert_state`](super::RustStream::insert_state); handlers and middleware read them
/// from the [`Context`] with [`Context::get`].
#[derive(Default)]
pub struct State {
    map: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
}

impl State {
    /// Inserts `value`, replacing any previous value of the same type.
    pub fn insert<T: Any + Send + Sync>(&mut self, value: T) {
        self.map.insert(TypeId::of::<T>(), Box::new(value));
    }

    /// Returns the stored value of type `T`, if any.
    #[must_use]
    pub fn get<T: Any + Send + Sync>(&self) -> Option<&T> {
        self.map.get(&TypeId::of::<T>())?.downcast_ref::<T>()
    }
}

impl std::fmt::Debug for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("State")
            .field("entries", &self.map.len())
            .finish_non_exhaustive()
    }
}

/// Per-delivery context, threaded through middleware and into the handler.
///
/// Carries the channel ([`name`](Self::name)), a working copy of the message
/// [`headers`](Self::headers) (middleware may enrich them for the handler; the broker message
/// itself is untouched), shared application [state](Self::get), and access to named
/// [`publisher`](Self::publisher)s for publishing from inside a handler. The copy is made lazily
/// on the first [`headers_mut`](Self::headers_mut) call. Outgoing messages do not inherit it:
/// replies and manual publishes start from fresh headers, shaped by the publish pipeline.
#[derive(Debug)]
pub struct Context<'a> {
    name: &'a str,
    original: &'a Headers,
    modified: Option<Headers>,
    state: &'a State,
    delivery: &'a Delivery,
    failfast: Option<&'a ErrorShutdown>,
}

impl<'a> Context<'a> {
    /// Creates a context for one delivery, borrowing the message headers until first mutation.
    pub(crate) fn new(
        name: &'a str,
        headers: &'a Headers,
        state: &'a State,
        delivery: &'a Delivery,
    ) -> Self {
        Self {
            name,
            original: headers,
            modified: None,
            state,
            delivery,
            failfast: None,
        }
    }

    /// Attaches the runtime's error-shutdown handle, so a fail-fast decode policy can tear the
    /// service down from inside the handler. The dispatch loop sets this; contexts built in tests
    /// leave it unset (a fail-fast there logs but cannot reach the run loop).
    #[must_use]
    pub(crate) fn with_failfast(mut self, failfast: &'a ErrorShutdown) -> Self {
        self.failfast = Some(failfast);
        self
    }

    /// Triggers a fail-fast shutdown for `reason` if a handle is attached, naming this delivery's
    /// subscription. Used by the decode path when its policy is
    /// [`FailFast`](super::FailurePolicy::FailFast).
    pub(crate) fn fail_fast(&self, reason: &str) {
        if let Some(failfast) = self.failfast {
            failfast.signal(self.name, reason);
        }
    }

    /// The channel (name / subject) the message arrived on.
    #[must_use]
    pub fn name(&self) -> &str {
        self.name
    }

    /// Resolves a named publisher (registered with
    /// [`RustStream::publisher`](super::RustStream::publisher)) to publish from this handler.
    ///
    /// Sends through it run the scope's publish middleware (envelope, metrics) - the same chain as a
    /// macro reply - so a manual publish is not a hole in the pipeline. Returns `None` if no
    /// publisher is registered under `name`.
    #[must_use]
    pub fn publisher(&self, name: &str) -> Option<ScopedPublisher<'_>> {
        let publisher = self.delivery.publishers.get(name)?;
        Some(ScopedPublisher::new(
            publisher.as_ref(),
            &self.delivery.pipeline,
        ))
    }

    /// The working copy of the message headers.
    #[must_use]
    pub fn headers(&self) -> &Headers {
        self.modified.as_ref().unwrap_or(self.original)
    }

    /// The working copy of the message headers, mutably. The first call clones the message
    /// headers; later calls return the same copy.
    pub fn headers_mut(&mut self) -> &mut Headers {
        self.modified.get_or_insert_with(|| self.original.clone())
    }

    /// Returns shared application state of type `T`, if registered.
    #[must_use]
    pub fn get<T: Any + Send + Sync>(&self) -> Option<&T> {
        self.state.get::<T>()
    }
}

#[cfg(test)]
mod tests {
    use super::{Context, State};
    use crate::Headers;
    use crate::runtime::dispatch::Delivery;

    #[test]
    fn headers_clone_only_on_first_mutation() {
        let mut original = Headers::new();
        original.insert("k", "v");
        let state = State::default();
        let delivery = Delivery::empty();
        let mut ctx = Context::new("test", &original, &state, &delivery);

        // Untouched: the context borrows the message headers, no copy exists.
        assert!(std::ptr::eq(ctx.headers(), &raw const original));

        ctx.headers_mut().insert("added", "1");
        ctx.headers_mut().insert("added2", "2");

        // Mutations land in one lazily-made copy; the original is untouched.
        assert!(!std::ptr::eq(ctx.headers(), &raw const original));
        assert_eq!(ctx.headers().get("added"), Some(&b"1"[..]));
        assert_eq!(ctx.headers().get("k"), Some(&b"v"[..]));
        assert_eq!(original.get("added"), None);
    }
}
