//! A refused publish as the runtime's sinks report it, and the places its message lies in.
//!
//! A sink that did not send a message reports a [`SinkRefusal`]: the cause, and a [`Lease`] on the
//! message it handed back. With `alloc` the lease is a box, filled only when a publish is refused.
//! Without it the message moves into a free [`Place`] of fixed size that the publishing side keeps
//! (a subscription, an `Out` slot, a publisher used on its own), and the lease is a handle to that
//! place. Either way a publish that went out returns the cause and a pointer-sized handle, whatever
//! the size of the message's rooms.

#[cfg(feature = "alloc")]
use alloc::boxed::Box;
#[cfg(feature = "alloc")]
use alloc::string::String;
#[cfg(not(feature = "alloc"))]
use core::cell::{RefCell, RefMut};
use core::fmt;
#[cfg(feature = "alloc")]
use core::marker::PhantomData;
#[cfg(not(feature = "alloc"))]
use core::mem;
use core::ops::{Deref, DerefMut};

use crate::Headers;
use crate::publisher::{KeptPayload, OutgoingFor, PayloadForm, PublishMessageError};

/// The room a refused publish gives the destination it returns, in bytes, in a build without
/// `alloc`.
#[cfg(not(feature = "alloc"))]
pub(crate) const DESTINATION_BYTES: usize = 64;

/// The destination a refused publish returns with its message: a `String` with `alloc`, a string
/// of [`DESTINATION_BYTES`] bytes without.
#[cfg(feature = "alloc")]
#[doc(hidden)]
pub type Destination = String;
/// The destination a refused publish returns with its message: a `String` with `alloc`, a string
/// of [`DESTINATION_BYTES`] bytes without.
#[cfg(not(feature = "alloc"))]
#[doc(hidden)]
pub type Destination = heapless::String<DESTINATION_BYTES>;

/// The destination a refused message recorded: the name with `alloc`, and without it `None` where
/// the name was longer than [`DESTINATION_BYTES`].
#[cfg(feature = "alloc")]
#[doc(hidden)]
pub type SentName = Destination;
/// The destination a refused message recorded: the name with `alloc`, and without it `None` where
/// the name was longer than [`DESTINATION_BYTES`].
#[cfg(not(feature = "alloc"))]
#[doc(hidden)]
pub type SentName = Option<Destination>;

/// The destination `name` as a refused message records it.
#[cfg(feature = "alloc")]
#[doc(hidden)]
#[must_use]
pub fn sent_name(name: &str) -> SentName {
    String::from(name)
}

/// The destination `name` as a refused message records it: `None` past the fixed room, so a copy
/// is never republished under a shortened name.
#[cfg(not(feature = "alloc"))]
#[doc(hidden)]
#[must_use]
pub fn sent_name(name: &str) -> SentName {
    Destination::try_from(name).ok()
}

/// The recorded destination, when it is one a message can be republished to.
#[cfg(feature = "alloc")]
#[expect(clippy::unnecessary_wraps, reason = "one signature for both variants")]
fn usable(sent: SentName) -> Option<Destination> {
    Some(sent)
}

/// The recorded destination, when it is one a message can be republished to: not past the room.
#[cfg(not(feature = "alloc"))]
const fn usable(sent: SentName) -> Option<Destination> {
    sent
}

/// The message a refused publish handed back, as its place holds it: where it went, its payload as
/// the sink kept it, and its headers. Machinery; the runtime's refusals read and write it.
#[doc(hidden)]
#[derive(Debug)]
pub struct Unsent {
    /// Where the message went: `None` where it went to the destination its caller named, which the
    /// caller still knows.
    pub sent: Option<SentName>,
    /// The payload, owned or still where it was lent from.
    pub payload: KeptPayload,
    /// The headers it carried.
    pub headers: Headers,
}

impl Unsent {
    /// An empty place's content: no name, no bytes, an empty map. Two lengths and two tags, so a
    /// place is cheap to make and to empty.
    pub const EMPTY: Self = Self {
        sent: None,
        payload: KeptPayload::Lent { addr: 0, len: 0 },
        headers: Headers::new(),
    };
}

/// One place a refused message lies in until its refusal is read or dropped. Machinery; a
/// [`PlaceRoom`] holds them.
///
/// With `alloc` a refusal boxes its message, and a place is a value of no size.
#[doc(hidden)]
pub struct Place {
    // Borrowed for as long as a refusal holds the place: the borrow is the lease, and its end
    // gives the place back. The storage belongs to one task, so a `RefCell` is the whole lock.
    #[cfg(not(feature = "alloc"))]
    unsent: RefCell<Unsent>,
}

impl Place {
    /// An empty place.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            #[cfg(not(feature = "alloc"))]
            unsent: RefCell::new(Unsent::EMPTY),
        }
    }
}

impl Default for Place {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for Place {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Place").finish_non_exhaustive()
    }
}

/// `N` places of fixed size, as the side that publishes keeps them: a subscription, an `Out`
/// slot, a publisher used on its own. Machinery; a mount's step or a publisher's sets the count.
#[doc(hidden)]
pub struct PlaceRoom<const N: usize>([Place; N]);

impl<const N: usize> PlaceRoom<N> {
    /// `N` empty places.
    #[must_use]
    pub const fn new() -> Self {
        Self([const { Place::new() }; N])
    }

    /// The places, for a refusal to take one of.
    #[must_use]
    pub const fn places(&self) -> Places<'_> {
        Places::new(&self.0)
    }
}

impl<const N: usize> Default for PlaceRoom<N> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const N: usize> fmt::Debug for PlaceRoom<N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PlaceRoom").field("places", &N).finish()
    }
}

/// The places a refusal may take one of. Machinery; passed down the publish path from the side
/// that keeps them.
#[doc(hidden)]
#[derive(Clone, Copy)]
pub struct Places<'p> {
    #[cfg(not(feature = "alloc"))]
    places: &'p [Place],
    #[cfg(feature = "alloc")]
    places: PhantomData<&'p [Place]>,
}

impl<'p> Places<'p> {
    /// The places of `places`.
    #[must_use]
    pub const fn new(places: &'p [Place]) -> Self {
        #[cfg(feature = "alloc")]
        let _ = places;
        Self {
            #[cfg(not(feature = "alloc"))]
            places,
            #[cfg(feature = "alloc")]
            places: PhantomData,
        }
    }

    /// No place at all: without `alloc`, a refusal through these comes back without its message.
    #[must_use]
    pub const fn none() -> Self {
        Self::new(&[])
    }

    /// Moves a refused message into a free place, which `fill` writes it into, and leases the
    /// place to the refusal. With `alloc` the message is boxed instead.
    ///
    /// `None` where every place is taken: the refusal then comes back without its message.
    #[cfg(feature = "alloc")]
    pub fn hold(self, fill: impl FnOnce(&mut Unsent)) -> Option<Lease<'p>> {
        let mut unsent = Unsent::EMPTY;
        fill(&mut unsent);
        Some(Lease {
            unsent: Box::new(unsent),
            places: PhantomData,
        })
    }

    /// Moves a refused message into a free place, which `fill` writes it into, and leases the
    /// place to the refusal. With `alloc` the message is boxed instead.
    ///
    /// `None` where every place is taken: the refusal then comes back without its message.
    #[cfg(not(feature = "alloc"))]
    pub fn hold(self, fill: impl FnOnce(&mut Unsent)) -> Option<Lease<'p>> {
        // A place a refusal holds is borrowed, so the first one that lends itself is free.
        let mut unsent = self
            .places
            .iter()
            .find_map(|place| place.unsent.try_borrow_mut().ok())?;
        fill(&mut unsent);
        Some(Lease { unsent })
    }
}

impl fmt::Debug for Places<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct("Places");
        #[cfg(not(feature = "alloc"))]
        debug.field("places", &self.places.len());
        debug.finish_non_exhaustive()
    }
}

/// A refusal's hold on the message it handed back: the box with `alloc`, the place without, given
/// back when the lease is dropped. Machinery; read through `Deref` and taken with
/// [`into_unsent`](Self::into_unsent).
#[doc(hidden)]
pub struct Lease<'p> {
    #[cfg(feature = "alloc")]
    unsent: Box<Unsent>,
    #[cfg(feature = "alloc")]
    places: PhantomData<&'p Place>,
    #[cfg(not(feature = "alloc"))]
    unsent: RefMut<'p, Unsent>,
}

impl Lease<'_> {
    /// The message, moved out; the place is empty and free again.
    #[must_use]
    pub fn into_unsent(self) -> Unsent {
        #[cfg(feature = "alloc")]
        {
            *self.unsent
        }
        #[cfg(not(feature = "alloc"))]
        {
            let mut lease = self;
            mem::replace(&mut *lease.unsent, Unsent::EMPTY)
        }
    }
}

impl Deref for Lease<'_> {
    type Target = Unsent;

    fn deref(&self) -> &Unsent {
        &self.unsent
    }
}

impl DerefMut for Lease<'_> {
    fn deref_mut(&mut self) -> &mut Unsent {
        &mut self.unsent
    }
}

impl fmt::Debug for Lease<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

/// How a sink reports a message it did not send: the cause, and a lease on the message as it was
/// to leave.
///
/// The message is owned, so it outlives the stages of the publish path that rewrote it: a payload
/// a transform wrote into a buffer of the call moves out of that buffer, and a payload nothing
/// rewrote stays with the caller that lent it. It is `None` where the client consumed a
/// [`Take`](crate::Take) buffer and did not return it. In a build without `alloc` the message
/// lies in a place of the side that published, and it is `None` as well where every place is
/// taken or the destination is longer than its room of 64 bytes: a shortened name would send a
/// republished copy to the wrong place. Machinery; a broker's publisher reports a [`PublishMessageError`] and
/// the sinks of the runtime turn it into this.
#[doc(hidden)]
#[derive(Debug)]
pub struct SinkRefusal<'p, Error> {
    error: Error,
    unsent: Option<Lease<'p>>,
}

/// What a sink returns: nothing on success, and on failure the cause with a lease on the unsent
/// message.
#[doc(hidden)]
pub type SinkOutcome<'p, Error> = Result<(), SinkRefusal<'p, Error>>;

impl<Error> From<Error> for SinkRefusal<'_, Error> {
    fn from(error: Error) -> Self {
        Self::new(error)
    }
}

impl<'p, Error> SinkRefusal<'p, Error> {
    /// A refusal that has no message to hand back.
    #[must_use]
    pub const fn new(error: Error) -> Self {
        Self {
            error,
            unsent: None,
        }
    }

    /// A refusal holding `unsent`, a message whose recorded destination is the one it was sent
    /// to.
    #[must_use]
    pub const fn leasing(error: Error, unsent: Option<Lease<'p>>) -> Self {
        Self { error, unsent }
    }

    /// The refusal of a publisher in the form `Form`, with the message it returned moved into one
    /// of `places`.
    #[must_use]
    pub fn returned<Form: PayloadForm>(
        failed: PublishMessageError<Error, OutgoingFor<'_, Form>>,
        places: Places<'p>,
    ) -> Self {
        let (error, message) = failed.into_parts();
        let Some(message) = message else {
            return Self::new(error);
        };
        let (name, payload, headers) = message.into_parts();
        // A name past the room is not kept, and the refusal then reports its error without the
        // message: a returned message under a shortened destination would be republished to the
        // wrong place.
        let Some(destination) = usable(sent_name(name)) else {
            return Self::new(error);
        };
        let unsent = places.hold(|place| {
            place.sent = Some(sent(destination));
            place.payload = Form::kept(payload);
            take_headers(&mut place.headers, headers);
        });
        Self { error, unsent }
    }

    /// The same refusal over another cause, the message kept.
    #[must_use]
    pub fn map_error<Other>(self, map: impl FnOnce(Error) -> Other) -> SinkRefusal<'p, Other> {
        SinkRefusal {
            error: map(self.error),
            unsent: self.unsent,
        }
    }

    /// The cause.
    #[must_use]
    pub const fn error(&self) -> &Error {
        &self.error
    }

    /// The cause, without the message.
    #[must_use]
    pub fn into_error(self) -> Error {
        self.error
    }

    /// The refusal once the stage that spent the message takes back what it kept for the send:
    /// the buffer a stage wrote moves into the message, and nothing is copied.
    #[must_use]
    pub fn reclaiming<Form: PayloadForm>(mut self, spent: &mut Form::Spent) -> Self {
        if let Some(unsent) = &mut self.unsent {
            Form::reclaimed(spent, &mut unsent.payload);
        }
        self
    }

    /// The cause and the lease on the message, taken apart.
    #[must_use]
    pub fn into_lease(self) -> (Error, Option<Lease<'p>>) {
        (self.error, self.unsent)
    }

    /// The cause and the message, taken apart: its destination, its payload as the sink kept it,
    /// and its headers, moved out of the place. `None` where there is no message.
    #[must_use]
    pub fn into_parts(self) -> (Error, Option<(Destination, KeptPayload, Headers)>) {
        let unsent = self.unsent.map(Lease::into_unsent).and_then(|unsent| {
            let destination = usable(unsent.sent?)?;
            Some((destination, unsent.payload, unsent.headers))
        });
        (self.error, unsent)
    }
}

/// The recorded destination of a name known to fit.
#[cfg(feature = "alloc")]
fn sent(destination: Destination) -> SentName {
    destination
}

/// The recorded destination of a name known to fit.
// One signature for both variants: a recorded name of the fixed build is an `Option`, and this one
// is known to fit.
#[cfg(not(feature = "alloc"))]
#[allow(clippy::unnecessary_wraps)]
const fn sent(destination: Destination) -> SentName {
    Some(destination)
}

/// Puts the map a refused message carried into its place: moved where there is a heap.
#[cfg(feature = "alloc")]
fn take_headers(place: &mut Headers, headers: Headers) {
    *place = headers;
}

/// Puts the map a refused message carried into its place: the entries and bytes it holds, written
/// over what the place held, where there is none.
#[cfg(not(feature = "alloc"))]
fn take_headers(place: &mut Headers, headers: &Headers) {
    place.clone_from(headers);
}

#[cfg(test)]
mod tests {
    use std::io;

    use super::*;
    use crate::{Lend, OutgoingMessage};

    /// A refusal of a lending publisher that hands `name` back with its payload.
    fn refused<'p>(name: &str, places: Places<'p>) -> SinkRefusal<'p, &'static str> {
        let payload = b"order".as_slice();
        let failed = PublishMessageError::with_message(
            "refused",
            OutgoingMessage::with_payload(name, payload),
        );
        SinkRefusal::returned::<Lend>(failed, places)
    }

    #[test]
    fn a_refusal_keeps_the_message_its_publisher_returned() {
        let room = PlaceRoom::<1>::new();
        let (error, unsent) = refused("orders", room.places()).into_parts();
        let (destination, payload, headers) = unsent.expect("the message is returned");
        assert_eq!(
            (error, destination.as_str(), headers.len()),
            ("refused", "orders", 0)
        );
        assert!(matches!(payload, KeptPayload::Lent { len: 5, .. }));
    }

    #[test]
    fn a_refusal_whose_publisher_kept_nothing_has_no_message() {
        let room = PlaceRoom::<1>::new();
        let failed = PublishMessageError::<_, OutgoingFor<'_, Lend>>::new(io::Error::other("no"));
        let refusal = SinkRefusal::returned::<Lend>(failed, room.places());
        assert!(refusal.into_parts().1.is_none());
    }
}

#[cfg(all(test, feature = "alloc"))]
mod boxed_tests {
    use super::*;

    // Every refusal boxes its message where there is a heap, whatever the places.
    #[test]
    fn a_refusal_without_places_still_keeps_its_message() {
        let failed = PublishMessageError::with_message(
            "refused",
            crate::OutgoingMessage::with_payload("orders", b"order".as_slice()),
        );
        let refusal = SinkRefusal::returned::<crate::Lend>(failed, Places::none());
        assert!(refusal.into_parts().1.is_some());
    }
}

#[cfg(all(test, not(feature = "alloc")))]
mod fixed_tests {
    use std::io;

    use super::*;
    use crate::{Lend, OutgoingMessage};

    /// A refusal of a lending publisher that hands `name` back with its payload.
    fn refused<'p>(name: &str, places: Places<'p>) -> SinkRefusal<'p, &'static str> {
        let failed = PublishMessageError::with_message(
            "refused",
            OutgoingMessage::with_payload(name, b"order".as_slice()),
        );
        SinkRefusal::returned::<Lend>(failed, places)
    }

    // A refusal holds a handle to the place its message lies in, so a sink's outcome is the
    // cause and that handle whatever the message's rooms hold.
    #[test]
    fn a_sink_that_sent_returns_an_outcome_of_the_cause_and_a_handle() {
        let outcome = size_of::<SinkOutcome<'_, io::Error>>();
        let bound = size_of::<io::Error>() + 2 * size_of::<usize>();
        assert!(outcome <= bound, "{outcome} bytes, above {bound}");
    }

    #[test]
    fn a_refusal_comes_back_without_its_message_once_every_place_is_taken() {
        let room = PlaceRoom::<2>::new();
        let first = refused("orders", room.places());
        let second = refused("orders", room.places());
        let third = refused("orders", room.places());
        let (error, unsent) = third.into_parts();
        assert_eq!(error, "refused");
        assert!(unsent.is_none(), "both places are held");
        assert!(first.into_parts().1.is_some() && second.into_parts().1.is_some());
    }

    #[test]
    fn a_place_given_back_is_taken_again() {
        let room = PlaceRoom::<1>::new();
        let held = refused("orders", room.places());
        drop(held);
        let (_, unsent) = refused("payments", room.places()).into_parts();
        let (destination, _, _) = unsent.expect("the place is free again");
        assert_eq!(destination.as_str(), "payments");
    }

    #[test]
    fn a_refusal_without_places_comes_back_without_its_message() {
        assert!(refused("orders", Places::none()).into_parts().1.is_none());
    }

    #[test]
    fn a_refusal_reports_its_error_without_a_destination_past_the_room() {
        let long = "d".repeat(DESTINATION_BYTES + 1);
        let room = PlaceRoom::<1>::new();
        let (error, unsent) = refused(&long, room.places()).into_parts();
        assert_eq!(error, "refused");
        assert!(
            unsent.is_none(),
            "a shortened destination would misroute a copy"
        );
    }
}
