//! What a handler's `.error` continuation receives when a publish the runtime makes for its
//! outcome fails, and the reaction the runtime takes when the outcome carries none.

use alloc::borrow::ToOwned;
use alloc::boxed::Box;
use alloc::string::String;
use alloc::sync::Arc;
use bytes::Bytes;
use core::fmt;
#[cfg(feature = "tokio")]
use core::mem;
#[cfg(feature = "tokio")]
use std::sync::{Mutex, PoisonError};
use thiserror::Error;
use tracing::warn;

use crate::HeaderMap;
use crate::handler::{Continuation, ContinuationFuture, MaybeSend};
use crate::lifecycle::BoxError;

/// The error of a retry copy the embedded runner has nowhere to hold: every record of the mount's
/// store is taken.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("no free record in the store of retry copies (capacity {capacity})")]
pub struct NoFreeRecordError {
    capacity: usize,
}

impl NoFreeRecordError {
    /// The error of a store of `capacity` records, all taken.
    #[doc(hidden)]
    #[must_use]
    pub const fn new(capacity: usize) -> Self {
        Self { capacity }
    }

    /// How many records the store has.
    #[must_use]
    pub const fn capacity(&self) -> usize {
        self.capacity
    }
}

/// The message a runtime publish failed to send: the destination it was bound for, its bytes and
/// its headers.
///
/// A handler's `.error` continuation receives it beside the [`BoxError`]. The runtime builds
/// it only for an outcome that registered `.error`, so a failure nobody asked about copies
/// nothing.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "json")]
/// # mod demo {
/// use std::time::Duration;
///
/// use ruststream::prelude::*;
/// use ruststream::runtime::FailedMessage;
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Order {
///     id: u64,
/// }
///
/// fn record(order: u64, lost: &FailedMessage) {
///     tracing::error!(order, destination = lost.destination(), bytes = lost.payload().len(), "retry lost");
/// }
///
/// #[subscriber("orders")]
/// async fn accept(order: &Order) -> HandlerOutcome {
///     let id = order.id;
///     HandlerOutcome::retry_after(Duration::from_secs(5))
///         .error(async move |_err, lost| record(id, &lost))
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Debug, Clone)]
pub struct FailedMessage {
    destination: String,
    payload: Bytes,
    headers: HeaderMap,
}

impl FailedMessage {
    /// A message bound for `destination`.
    #[doc(hidden)]
    #[must_use]
    pub fn new(destination: &str, payload: Bytes, headers: HeaderMap) -> Self {
        Self {
            destination: destination.to_owned(),
            payload,
            headers,
        }
    }

    /// Where the message was going.
    #[must_use]
    pub fn destination(&self) -> &str {
        &self.destination
    }

    /// The message's bytes, as they were to leave.
    #[must_use]
    pub const fn payload(&self) -> &Bytes {
        &self.payload
    }

    /// The message's headers, the retry count included.
    #[must_use]
    pub const fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    /// Splits the message into its destination, bytes and headers.
    #[must_use]
    pub fn into_parts(self) -> (String, Bytes, HeaderMap) {
        (self.destination, self.payload, self.headers)
    }
}

/// The `.error` closure, erased: `Send` where the runtime crate's `tokio` feature is on.
#[cfg(feature = "tokio")]
trait Reactor: Send {
    fn react(self: Box<Self>, error: BoxError, message: FailedMessage) -> Continuation;
    fn share(self: Box<Self>) -> Arc<dyn Shared>;
}

/// See the `tokio` form: without the feature the closure carries no `Send`.
#[cfg(not(feature = "tokio"))]
trait Reactor {
    fn react(self: Box<Self>, error: BoxError, message: FailedMessage) -> Continuation;
    fn share(self: Box<Self>) -> Arc<dyn Shared>;
}

/// One `.error` closure shared by the elements of a batch, which each take a copy of it when
/// their own publish fails.
#[cfg(feature = "tokio")]
trait Shared: Send + Sync {
    fn react(&self, error: BoxError, message: FailedMessage) -> Continuation;
}

/// See the `tokio` form.
#[cfg(not(feature = "tokio"))]
trait Shared {
    fn react(&self, error: BoxError, message: FailedMessage) -> Continuation;
}

/// The closure as the one registration holds it.
struct Owned<React>(React);

impl<React, Reaction> Reactor for Owned<React>
where
    React: FnOnce(BoxError, FailedMessage) -> Reaction + Clone + MaybeSend + 'static,
    Reaction: ContinuationFuture,
{
    fn react(self: Box<Self>, error: BoxError, message: FailedMessage) -> Continuation {
        Box::pin((self.0)(error, message))
    }

    fn share(self: Box<Self>) -> Arc<dyn Shared> {
        Arc::new(Holder::new(self.0))
    }
}

/// The closure behind the `Arc` a batch shares. Under Tokio the copy is made under a lock held
/// for the clone alone, which lets the closure be shared without being `Sync`.
#[cfg(feature = "tokio")]
struct Holder<React>(Mutex<React>);

#[cfg(feature = "tokio")]
impl<React> Holder<React> {
    const fn new(react: React) -> Self {
        Self(Mutex::new(react))
    }

    fn copy(&self) -> React
    where
        React: Clone,
    {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

/// See the `tokio` form: without `Send` the closure is cloned in place.
#[cfg(not(feature = "tokio"))]
struct Holder<React>(React);

#[cfg(not(feature = "tokio"))]
impl<React> Holder<React> {
    const fn new(react: React) -> Self {
        Self(react)
    }

    fn copy(&self) -> React
    where
        React: Clone,
    {
        self.0.clone()
    }
}

impl<React, Reaction> Shared for Holder<React>
where
    React: FnOnce(BoxError, FailedMessage) -> Reaction + Clone + MaybeSend + 'static,
    Reaction: ContinuationFuture,
{
    fn react(&self, error: BoxError, message: FailedMessage) -> Continuation {
        Box::pin(self.copy()(error, message))
    }
}

/// The `.error` continuation of one [`HandlerOutcome`](crate::HandlerOutcome), until a publish
/// the runtime makes for it fails. Machinery; never named directly.
#[doc(hidden)]
pub struct ErrorHook(Inner);

/// An [`ErrorHook`]: a closure held by one registration, or one shared by a batch.
enum Inner {
    One(Box<dyn Reactor>),
    Shared(Arc<dyn Shared>),
}

impl ErrorHook {
    #[must_use]
    pub fn new<React, Reaction>(react: React) -> Self
    where
        React: FnOnce(BoxError, FailedMessage) -> Reaction + Clone + MaybeSend + 'static,
        Reaction: ContinuationFuture,
    {
        Self(Inner::One(Box::new(Owned(react))))
    }

    /// Hands the failure to the handler's continuation, which the caller runs.
    pub fn react(self, error: BoxError, message: FailedMessage) -> Continuation {
        match self.0 {
            Inner::One(owned) => owned.react(error, message),
            Inner::Shared(shared) => shared.react(error, message),
        }
    }

    /// Moves the closure behind an `Arc` for the elements of a batch to share: one allocation
    /// for the batch, and nothing is cloned until an element's publish fails.
    pub(crate) fn into_shared(self) -> SharedHook {
        match self.0 {
            Inner::One(owned) => SharedHook(owned.share()),
            Inner::Shared(shared) => SharedHook(shared),
        }
    }
}

/// An [`ErrorHook`] shared by the elements of a batch.
#[derive(Clone)]
pub(crate) struct SharedHook(Arc<dyn Shared>);

impl SharedHook {
    /// The hook of one element: a reference count, not a copy of the closure.
    pub(crate) fn hook(&self) -> ErrorHook {
        ErrorHook(Inner::Shared(Arc::clone(&self.0)))
    }
}

impl fmt::Debug for ErrorHook {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ErrorHook").finish_non_exhaustive()
    }
}

/// What a publish the runtime makes for a delivery hands its result to: the handler's `.error`
/// continuation for a failure, and the `.then` continuation registered beside it for a success.
///
/// With an `.error` registered, `.then` waits for the publish: it runs if the publish succeeded
/// and never if it failed, and `.error` runs if it failed and never if it succeeded. A delivery
/// that registered no `.error` has no reaction: its `.then` runs once the delivery is settled.
/// Machinery; never named directly.
#[doc(hidden)]
pub struct Reaction(Box<Registered>);

/// What a [`Reaction`] holds, behind the one pointer an outcome without it pays eight bytes for.
struct Registered {
    error: ErrorHook,
    then: Option<Continuation>,
    /// The headers of the failed copy, kept here (boxed with the registration) so the task that
    /// publishes the copy carries one pointer for them and nothing when no `.error` was set.
    #[cfg(feature = "tokio")]
    headers: HeaderMap,
}

impl fmt::Debug for Reaction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Reaction").finish_non_exhaustive()
    }
}

impl Reaction {
    /// A reaction handing a failure to `error` and a success to `then`.
    #[must_use]
    pub fn new(error: ErrorHook, then: Option<Continuation>) -> Self {
        Self(Box::new(Registered {
            error,
            then,
            #[cfg(feature = "tokio")]
            headers: HeaderMap::new(),
        }))
    }

    /// The continuation registered beside `.error`, which a delivery that made no publish runs
    /// once it is settled.
    /// Keeps the headers the failed copy carries, for the [`FailedMessage`] a failure builds.
    #[cfg(feature = "tokio")]
    pub(crate) fn hold_headers(&mut self, headers: HeaderMap) {
        self.0.headers = headers;
    }

    /// Takes the headers kept with [`hold_headers`](Self::hold_headers).
    #[cfg(feature = "tokio")]
    pub(crate) fn take_headers(&mut self) -> HeaderMap {
        mem::take(&mut self.0.headers)
    }

    pub(crate) fn into_then(self) -> Option<Continuation> {
        self.0.then
    }

    /// What follows a publish that succeeded: the `.then` continuation, if one was registered.
    #[must_use]
    pub fn succeeded(self) -> Option<Continuation> {
        self.0.then
    }

    /// What follows a publish that failed: the `.error` continuation, given the cause.
    fn failed(self, error: BoxError, message: FailedMessage) -> Continuation {
        self.0.error.react(error, message)
    }
}

/// What follows a publish that succeeded: the `.then` continuation registered beside `.error`,
/// returned for the caller to run beside the delivery.
#[doc(hidden)]
#[must_use]
pub fn publish_done(reaction: Option<Reaction>) -> Option<Continuation> {
    reaction.and_then(Reaction::succeeded)
}

/// What happens to a copy or reply whose publish failed: the handler's `.error` continuation,
/// returned for the caller to run, or, with none, a warning and the message lost.
///
/// `message` builds the failed message, and is called only when there is a continuation to hand
/// it to.
#[doc(hidden)]
pub fn publish_lost(
    reaction: Option<Reaction>,
    subscription: &str,
    destination: &str,
    input: &str,
    error: BoxError,
    message: impl FnOnce() -> FailedMessage,
) -> Option<Continuation> {
    if let Some(reaction) = reaction {
        return Some(reaction.failed(error, message()));
    }
    warn!(
        target: "ruststream::dispatch",
        subscription = %subscription,
        destination = %destination,
        message_type = %input,
        error = %error,
        "publishing the message failed; message lost",
    );
    None
}
