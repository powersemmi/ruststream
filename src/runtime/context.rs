//! Per-delivery [`Context`] and the app-level [`State`] type-map.
//!
//! A `Context` is built for each delivery and threaded (as `&mut`) through the middleware chain
//! into the handler. It carries the channel the message arrived on, a mutable working copy of the
//! headers (middleware may enrich them), and shared application state.

use std::any::{Any, TypeId};
use std::collections::HashMap;

use crate::Headers;

use super::dispatch::Delivery;
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
/// Carries the channel ([`name`](Self::name)), a mutable working copy of the message
/// [`headers`](Self::headers) (middleware may enrich them for the handler; the broker message
/// itself is untouched), shared application [state](Self::get), and access to named
/// [`publisher`](Self::publisher)s for publishing from inside a handler. Outgoing messages do not
/// inherit the copy: replies and manual publishes start from fresh headers, shaped by the publish
/// pipeline.
#[derive(Debug)]
pub struct Context<'a> {
    name: &'a str,
    headers: Headers,
    state: &'a State,
    delivery: &'a Delivery,
}

impl<'a> Context<'a> {
    /// Creates a context for one delivery.
    pub(crate) fn new(
        name: &'a str,
        headers: Headers,
        state: &'a State,
        delivery: &'a Delivery,
    ) -> Self {
        Self {
            name,
            headers,
            state,
            delivery,
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
        &self.headers
    }

    /// The working copy of the message headers, mutably.
    pub fn headers_mut(&mut self) -> &mut Headers {
        &mut self.headers
    }

    /// Returns shared application state of type `T`, if registered.
    #[must_use]
    pub fn get<T: Any + Send + Sync>(&self) -> Option<&T> {
        self.state.get::<T>()
    }
}
