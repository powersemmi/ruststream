//! What a handler's `.error` continuation receives when a publish the runtime makes for its
//! outcome fails, and the reaction the runtime takes when the outcome carries none.

use alloc::borrow::ToOwned;
use alloc::boxed::Box;
use alloc::string::String;
use bytes::Bytes;
use core::fmt;
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

/// The boxed `.error` closure: `Send` where the runtime crate's `tokio` feature is on.
#[cfg(feature = "tokio")]
type Hook = Box<dyn FnOnce(BoxError, FailedMessage) -> Continuation + Send>;

/// See the `tokio` form: without the feature the closure carries no `Send`.
#[cfg(not(feature = "tokio"))]
type Hook = Box<dyn FnOnce(BoxError, FailedMessage) -> Continuation>;

/// The `.error` continuation of one [`HandlerOutcome`](crate::HandlerOutcome), until a publish
/// the runtime makes for it fails. Machinery; never named directly.
#[doc(hidden)]
pub struct ErrorHook(Box<Hook>);

impl ErrorHook {
    #[must_use]
    pub fn new<React, Reaction>(react: React) -> Self
    where
        React: FnOnce(BoxError, FailedMessage) -> Reaction + MaybeSend + 'static,
        Reaction: ContinuationFuture,
    {
        // A box around the box: the closure is a fat pointer, and an outcome that carries none
        // pays eight bytes for it rather than sixteen.
        Self(Box::new(Box::new(move |error, message| {
            Box::pin(react(error, message))
        })))
    }

    /// Hands the failure to the handler's continuation, which the caller runs.
    pub fn react(self, error: BoxError, message: FailedMessage) -> Continuation {
        (*self.0)(error, message)
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
            headers: HeaderMap::new(),
        }))
    }

    /// The continuation registered beside `.error`, which a delivery that made no publish runs
    /// once it is settled.
    /// Keeps the headers the failed copy carries, for the [`FailedMessage`] a failure builds.
    pub(crate) fn hold_headers(&mut self, headers: HeaderMap) {
        self.0.headers = headers;
    }

    /// Takes the headers kept with [`hold_headers`](Self::hold_headers).
    pub(crate) fn take_headers(&mut self) -> HeaderMap {
        core::mem::take(&mut self.0.headers)
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
