//! What a handler's `.error` continuation receives when a publish the runtime makes for its
//! outcome fails, and the reaction the runtime takes when the outcome carries none.

#[cfg(ruststream_alloc)]
use alloc::boxed::Box;
#[cfg(ruststream_alloc)]
use alloc::sync::Arc;
use core::error::Error as StdError;
use core::fmt;
#[cfg(ruststream_alloc)]
use core::mem;

use ruststream_core::{EncodeBuf, KeptPayload, Lease, Places, SentName, Unsent, sent_name};
use thiserror::Error;

use crate::copied::{CopiedName, CopiedPayload, copy_name};
use crate::lifecycle::BoxError;
use crate::publish::SinkRefusal;
use crate::{
    CallerName, HeaderSet, MessageHeaders, MessageWire, NoHeaders, Outgoing, OutgoingDestination,
    OutgoingFor, PayloadForm, PublishMessageError, Serialized, SerializedWire, WireBytes,
};

/// The error of sending on a [`FailedMessage`] whose bytes are gone: the publisher that refused
/// it consumed the buffer and gave nothing back, so there is no message left to send.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("the failed message has no bytes to send: its publisher consumed them")]
#[non_exhaustive]
pub struct MissingPayloadError;

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

/// The destination a failed message reports: the name with `alloc`, and without it `None` where
/// the name was longer than the fixed room.
pub(crate) type DestinationName = SentName;

/// The message a runtime publish failed to send: the destination it was bound for, its bytes and
/// its headers.
///
/// The bytes are `None` only where the publisher took the payload buffer and did not give it back;
/// the destination and the headers the runtime held are then all there is.
///
/// A handler's `.error` continuation receives it inside a [`Failed`], beside the error. The runtime builds
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
///     let bytes = lost.payload().map_or(0, |payload| payload.len());
///     tracing::error!(order, destination = lost.destination(), bytes, "retry lost");
/// }
///
/// #[subscriber("orders")]
/// async fn accept(order: &Order) -> HandlerOutcome {
///     let id = order.id;
///     HandlerOutcome::retry_after(Duration::from_secs(5))
///         .error(async move |failures: Failures| {
///             for failed in failures {
///                 record(id, failed.message());
///             }
///         })
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Debug, Clone)]
pub struct FailedMessage {
    #[cfg(ruststream_alloc)]
    destination: CopiedName,
    // Without `alloc` a name past the fixed room is lost, and the message says so with `None`:
    // an empty name reported as the destination would send a republish to the wrong place.
    #[cfg(not(ruststream_alloc))]
    destination: Option<CopiedName>,
    payload: Option<CopiedPayload>,
    headers: HeaderSet,
}

impl FailedMessage {
    /// A message bound for `destination`.
    #[doc(hidden)]
    #[must_use]
    pub fn new(destination: &str, payload: Option<CopiedPayload>, headers: HeaderSet) -> Self {
        Self {
            #[cfg(ruststream_alloc)]
            destination: copy_name(destination).unwrap_or_default(),
            #[cfg(not(ruststream_alloc))]
            destination: copy_name(destination),
            payload,
            headers,
        }
    }

    /// Where the message was going.
    #[must_use]
    #[cfg(ruststream_alloc)]
    pub fn destination(&self) -> &str {
        &self.destination
    }

    /// Where the message was going; `None` where the name was longer than the fixed room of a
    /// build without `alloc`.
    #[must_use]
    #[cfg(not(ruststream_alloc))]
    pub fn destination(&self) -> Option<&str> {
        self.destination.as_deref()
    }

    /// The message's bytes, as they were to leave, or `None` when the publisher consumed the
    /// buffer and did not give it back.
    #[must_use]
    pub const fn payload(&self) -> Option<&CopiedPayload> {
        self.payload.as_ref()
    }

    /// The message's headers, the retry count included.
    #[must_use]
    pub const fn headers(&self) -> &HeaderSet {
        &self.headers
    }

    /// Splits the message into its destination, bytes and headers.
    #[must_use]
    pub fn into_parts(self) -> (DestinationName, Option<CopiedPayload>, HeaderSet) {
        (self.destination, self.payload, self.headers)
    }
}

/// The message a runtime publish failed to send is a [`Serialized`] value: its bytes leave as they
/// are, so another `Out` publishes it unchanged. A message whose bytes the publisher consumed is
/// refused with [`MissingPayloadError`] rather than sent empty.
impl Serialized for FailedMessage {
    type Error = MissingPayloadError;

    fn wire_bytes(&self, _buf: &mut EncodeBuf) -> Result<WireBytes<'_>, MissingPayloadError> {
        self.payload
            .as_deref()
            .map(WireBytes::Own)
            .ok_or(MissingPayloadError)
    }
}

// Left out of the compiler's suggestions: a user who forgot a derive is not pointed at this type.
#[diagnostic::do_not_recommend]
impl MessageWire for FailedMessage {
    type Wire = SerializedWire;
}

// The destination the message was going to belongs to the failure, not to another publish: the
// call site that republishes it names where it goes now.
impl OutgoingDestination for FailedMessage {
    type Form = CallerName;
}

// Its headers travel with it, as a map the call site hands over with `with_headers`.
impl MessageHeaders for FailedMessage {
    type Contract = NoHeaders;
}

/// A publish the runtime made that failed, with whatever the publisher handed back.
///
/// The paths that publish for a handler (a reply, a retry copy, a dead-letter copy) return it, and
/// the `.error` continuation's [`Failed`] is built from it: the message that comes back is the
/// one that was sent, moved, with no copy made beforehand. The refusal holds a lease on the
/// message: a box with `alloc`, and without it a place of the subscription's, so a publish that
/// went out returns the cause and a pointer-sized handle through every layer of the reply and the
/// copy. Machinery; never named directly.
#[doc(hidden)]
#[derive(Debug)]
pub struct Refusal<'p> {
    error: BoxError,
    unsent: Option<Lease<'p>>,
}

impl<'p> Refusal<'p> {
    /// A refusal that carries no message.
    #[must_use]
    pub fn new(error: impl Into<BoxError>) -> Self {
        Self {
            error: error.into(),
            unsent: None,
        }
    }

    /// The refusal of a publish in the form `Form`, with the message the publisher returned moved
    /// into one of `places`.
    ///
    /// A lent payload stays where it was lent from; whoever owns the bytes resolves it in
    /// [`into_message`](Self::into_message).
    pub fn returned<Form: PayloadForm>(
        failed: PublishMessageError<impl Into<BoxError>, OutgoingFor<'_, Form>>,
        places: Places<'p>,
    ) -> Self {
        let (error, message) = failed.into_parts();
        Self {
            error: error.into(),
            unsent: message.and_then(|message| {
                let (_, payload, headers) = message.into_parts();
                places.hold(|place| {
                    // Handed straight to the publisher: the destination is the one the caller
                    // named.
                    place.sent = None;
                    place.payload = Form::kept(payload);
                    take_headers(&mut place.headers, headers);
                })
            }),
        }
    }

    /// The refusal of a sink, whose message is already in its place.
    #[must_use]
    pub(crate) fn from_sink<Error: Into<BoxError>>(refusal: SinkRefusal<'p, Error>) -> Self {
        let (error, unsent) = refusal.into_lease();
        Self {
            error: error.into(),
            unsent,
        }
    }

    /// The refusal of a publish that failed with `error` while `out` held the message: what it
    /// holds is the message that was sent, and moves into one of `places`.
    ///
    /// A buffer the stages wrote moves as it is; bytes `out` only lends stay where they are, for
    /// the owner of the lender to take in [`into_message`](Self::into_message).
    pub fn of(error: BoxError, out: &mut Outgoing<'_>, places: Places<'p>) -> Self {
        if out.is_consumed() {
            return Self::new(error);
        }
        let unsent = places.hold(|place| {
            place.sent = None;
            place.payload = out.take_owned_payload().map_or_else(
                || KeptPayload::Lent {
                    addr: out.payload().as_ptr() as usize,
                    len: out.payload().len(),
                },
                KeptPayload::Owned,
            );
            take_out_headers(&mut place.headers, out);
        });
        Self { error, unsent }
    }

    /// Records that the message went to `sent` rather than the `declared` destination, as it
    /// does when a naming transform redirected it. The name is copied only then.
    #[must_use]
    pub(crate) fn sent_to(mut self, declared: &str, sent: &str) -> Self {
        if sent != declared
            && let Some(unsent) = self.unsent.as_mut()
        {
            unsent.sent = Some(sent_name(sent));
        }
        self
    }

    /// The refusal as a sink reports it: the cause wrapped by `wrap`, and the message where it
    /// was sent, `destination` unless the refusal recorded another.
    #[must_use]
    pub(crate) fn into_sink<Error>(
        self,
        destination: &str,
        wrap: impl FnOnce(BoxError) -> Error,
    ) -> SinkRefusal<'p, Error> {
        let error = wrap(self.error);
        let unsent = self.unsent.and_then(|mut unsent| {
            if unsent.sent.is_none() {
                unsent.sent = Some(sent_name(destination));
            }
            // A name past the fixed room is not a destination to report: the message would be
            // republished to the wrong place, so the sink hands back the error alone.
            routable(unsent.sent.as_ref()).then_some(unsent)
        });
        SinkRefusal::leasing(error, unsent)
    }

    /// The cause.
    #[must_use]
    pub fn error(&self) -> &BoxError {
        &self.error
    }

    /// The cause, without the message.
    #[must_use]
    pub fn into_error(self) -> BoxError {
        self.error
    }

    /// The cause and the message where it was sent: `destination`, unless the refusal recorded
    /// another.
    ///
    /// `lent` takes the bytes a lending publisher was shown, given where they were: the caller
    /// that owns them moves them out instead of copying. A refusal with no message (the client
    /// consumed the buffer, or every place was taken), or whose lent bytes `lent` could not
    /// return whole, yields a message with no bytes.
    #[must_use]
    pub fn into_message(
        self,
        destination: &str,
        lent: impl FnOnce(usize, usize) -> CopiedPayload,
    ) -> (BoxError, FailedMessage) {
        let message = match self.unsent.map(Lease::into_unsent) {
            Some(Unsent {
                sent,
                payload,
                headers,
            }) => {
                let payload = match payload {
                    KeptPayload::Owned(bytes) => Some(bytes),
                    // Fewer bytes than were lent means the owner could not keep them (a copy
                    // has a fixed room without `alloc`): the message then has no bytes, so a
                    // republish is refused rather than sent short.
                    KeptPayload::Lent { addr, len } => {
                        Some(lent(addr, len)).filter(|kept| kept.len() == len)
                    }
                };
                FailedMessage {
                    destination: sent.unwrap_or_else(|| sent_name(destination)),
                    payload,
                    headers,
                }
            }
            None => FailedMessage::new(destination, None, HeaderSet::new()),
        };
        (self.error, message)
    }
}

/// The message a publisher returned when it refused it, taken out of its answer so the stage that
/// lent it the message can have it back: the cause, and what of the message the stage gives out.
///
/// For a path that returns its error alone and keeps the message it lent: the stages above read it
/// from there once the error reaches them, so it moves back through no place.
pub(crate) struct Returned {
    error: BoxError,
    parts: Option<ReturnedParts>,
}

impl Returned {
    /// What a refused publish in the form `Form` handed back.
    pub(crate) fn of<Form: PayloadForm>(
        failed: PublishMessageError<impl Into<BoxError>, OutgoingFor<'_, Form>>,
    ) -> Self {
        let (error, message) = failed.into_parts();
        Self {
            error: error.into(),
            parts: message.map(returned_parts::<Form>),
        }
    }

    /// Puts the message back into `out`, which then reads as the message that was sent, and gives
    /// the cause. A refusal with no message leaves `out` empty.
    pub(crate) fn restore<Form: PayloadForm>(self, out: &mut Outgoing<'_>) -> BoxError {
        match self.parts {
            Some(parts) => restore_parts::<Form>(parts, out),
            // The client consumed the buffer: nothing of the message is left to read.
            None => out.consume(),
        }
        self.error
    }
}

/// What a returned message hands back to the stage that lent it out: its payload, and its map
/// where the message owned one.
#[cfg(ruststream_alloc)]
type ReturnedParts = (KeptPayload, HeaderSet);
/// What a returned message hands back to the stage that lent it out: its payload alone, since its
/// map was the stage's own, borrowed.
#[cfg(not(ruststream_alloc))]
type ReturnedParts = KeptPayload;

#[cfg(ruststream_alloc)]
fn returned_parts<Form: PayloadForm>(message: OutgoingFor<'_, Form>) -> ReturnedParts {
    let (_, payload, headers) = message.into_parts();
    (Form::kept(payload), headers)
}

#[cfg(not(ruststream_alloc))]
fn returned_parts<Form: PayloadForm>(message: OutgoingFor<'_, Form>) -> ReturnedParts {
    let (_, payload, _) = message.into_parts();
    Form::kept(payload)
}

#[cfg(ruststream_alloc)]
fn restore_parts<Form: PayloadForm>((payload, headers): ReturnedParts, out: &mut Outgoing<'_>) {
    Form::restored(payload, out);
    *out.headers_mut() = headers;
}

#[cfg(not(ruststream_alloc))]
fn restore_parts<Form: PayloadForm>(payload: ReturnedParts, out: &mut Outgoing<'_>) {
    Form::restored(payload, out);
}

/// Puts the map a refused message carried into its place: moved where there is a heap.
#[cfg(ruststream_alloc)]
fn take_headers(place: &mut HeaderSet, headers: HeaderSet) {
    *place = headers;
}

/// Puts the map a refused message carried into its place: the entries and bytes it holds, where
/// there is no heap.
#[cfg(not(ruststream_alloc))]
fn take_headers(place: &mut HeaderSet, headers: &HeaderSet) {
    place.clone_from(headers);
}

/// Moves the map of the message `out` holds into its place where there is a heap.
#[cfg(ruststream_alloc)]
fn take_out_headers(place: &mut HeaderSet, out: &mut Outgoing<'_>) {
    *place = mem::take(out.headers_mut());
}

/// Copies the entries and bytes of the map the message `out` holds into its place, where there is
/// no heap: what `out` keeps is dropped with it.
#[cfg(not(ruststream_alloc))]
fn take_out_headers(place: &mut HeaderSet, out: &Outgoing<'_>) {
    place.clone_from(out.headers());
}

/// Whether a recorded destination is one a message can be republished to.
#[cfg(ruststream_alloc)]
const fn routable(_sent: Option<&SentName>) -> bool {
    true
}

/// Whether a recorded destination is one a message can be republished to: not past the room.
#[cfg(not(ruststream_alloc))]
const fn routable(sent: Option<&SentName>) -> bool {
    matches!(sent, Some(Some(_)))
}

impl fmt::Display for Refusal<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.error, f)
    }
}

impl From<BoxError> for Refusal<'_> {
    fn from(error: BoxError) -> Self {
        Self::new(error)
    }
}

/// One publish the runtime made for a delivery and that failed: its cause and the message that
/// did not go out.
///
/// The message is the one the runtime tried to send, with its bytes and headers as they were;
/// republish it through another `Out`, store it or inspect it. Nothing is decoded again.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "json")]
/// # mod demo {
/// use std::time::Duration;
///
/// use ruststream::prelude::*;
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Order {
///     id: u64,
/// }
///
/// #[subscriber("orders")]
/// async fn accept(order: &Order) -> HandlerOutcome {
///     let id = order.id;
///     HandlerOutcome::retry_after(Duration::from_secs(5)).error(async move |failures: Failures| {
///         for failed in failures {
///             tracing::error!(
///                 order = id,
///                 error = %failed.error(),
///                 destination = failed.message().destination(),
///                 bytes = failed.message().payload().map_or(0, |payload| payload.len()),
///                 "the retry was lost",
///             );
///         }
///     })
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Debug)]
pub struct Failed {
    error: FailureCause,
    message: FailedMessage,
}

/// The error of a [`Failed`]: its own, or with `alloc` the one error of a publish that carried
/// several messages, shared by the failure of each.
#[derive(Debug)]
enum FailureCause {
    Owned(BoxError),
    #[cfg(ruststream_alloc)]
    Shared(Arc<BoxError>),
}

impl Failed {
    /// A failure of `message`, caused by `error`.
    #[doc(hidden)]
    #[must_use]
    pub const fn new(error: BoxError, message: FailedMessage) -> Self {
        Self {
            error: FailureCause::Owned(error),
            message,
        }
    }

    /// A failure of `message` whose error the failures of the rest of its publish share.
    #[cfg(ruststream_alloc)]
    fn shared(error: Arc<BoxError>, message: FailedMessage) -> Self {
        Self {
            error: FailureCause::Shared(error),
            message,
        }
    }

    /// Why the publish failed: the broker publisher's own error, the encode error, or the
    /// "no free record" kind.
    #[must_use]
    pub fn error(&self) -> &(dyn StdError + Send + Sync + 'static) {
        match &self.error {
            #[cfg(ruststream_alloc)]
            FailureCause::Owned(error) => &**error,
            #[cfg(not(ruststream_alloc))]
            FailureCause::Owned(error) => error.as_error(),
            #[cfg(ruststream_alloc)]
            FailureCause::Shared(error) => &***error,
        }
    }

    /// The message that did not go out.
    #[must_use]
    pub const fn message(&self) -> &FailedMessage {
        &self.message
    }

    /// The message that did not go out, taken by value.
    #[must_use]
    pub fn into_message(self) -> FailedMessage {
        self.message
    }

    /// The cause and the message, taken apart.
    #[must_use]
    pub fn into_parts(self) -> (BoxError, FailedMessage) {
        #[cfg(ruststream_alloc)]
        let error = match self.error {
            FailureCause::Owned(error) => error,
            FailureCause::Shared(error) => Box::new(SharedCause(error)),
        };
        #[cfg(not(ruststream_alloc))]
        let FailureCause::Owned(error) = self.error;
        (error, self.message)
    }
}

#[cfg(ruststream_alloc)]
mod boxed;
#[cfg(ruststream_alloc)]
use boxed::SharedCause;
#[cfg(not(ruststream_alloc))]
mod fixed;

#[cfg(ruststream_alloc)]
pub(crate) use boxed::Fan;
#[cfg(ruststream_alloc)]
pub use boxed::{ErrorHook, Failures, Reaction, publish_done, publish_lost};
#[cfg(not(ruststream_alloc))]
pub(crate) use fixed::Unpublished;
#[cfg(not(ruststream_alloc))]
pub use fixed::{BatchFan, FailureSlot, Gathers, NoFan};
#[cfg(not(ruststream_alloc))]
pub use fixed::{ErrorHook, Failures, Reaction, publish_done, publish_lost};

#[cfg(test)]
#[cfg(ruststream_alloc)]
mod tests {

    use super::*;

    #[test]
    fn a_publish_that_went_out_returns_a_result_of_the_cause_alone() {
        assert!(size_of::<Result<(), Refusal>>() <= size_of::<BoxError>() + size_of::<usize>());
    }

    #[test]
    fn a_failed_message_splits_into_its_destination_bytes_and_headers() {
        let message = FailedMessage::new(
            "dest",
            Some(CopiedPayload::from_static(b"body")),
            HeaderSet::new(),
        );

        let (destination, payload, headers) = message.into_parts();

        assert_eq!(
            (destination.as_str(), payload.as_deref(), headers.len()),
            ("dest", Some(&b"body"[..]), 0)
        );
    }

    #[test]
    fn the_no_free_record_error_reports_the_capacity_of_its_store() {
        assert_eq!(NoFreeRecordError::new(3).capacity(), 3);
    }
}

#[cfg(test)]
#[cfg(not(ruststream_alloc))]
mod fixed_tests {
    use super::{FailedMessage, Refusal};
    use crate::HeaderSet;
    use crate::copied::NAME_CAPACITY;
    use crate::lifecycle::BoxError;

    // The refused message lies in a place of its own, so a publish that went out returns the
    // cause and a handle, through every layer of the reply and the copy.
    #[test]
    fn a_publish_that_went_out_returns_a_result_of_the_cause_and_a_handle() {
        let outcome = size_of::<Result<(), Refusal<'_>>>();
        let bound = size_of::<BoxError>() + 2 * size_of::<usize>();
        assert!(outcome <= bound, "{outcome} bytes, above {bound}");
    }

    #[test]
    fn a_destination_that_fits_is_reported() {
        let message = FailedMessage::new("orders", None, HeaderSet::default());
        assert_eq!(message.destination(), Some("orders"));
    }

    #[test]
    fn a_destination_past_the_room_is_reported_as_none_not_as_empty() {
        let long = [b'd'; NAME_CAPACITY + 1];
        let long = core::str::from_utf8(&long).expect("ascii");
        let message = FailedMessage::new(long, None, HeaderSet::default());
        assert_eq!(message.destination(), None);
    }
}
