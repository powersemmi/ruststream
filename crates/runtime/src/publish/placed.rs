//! A publisher used outside a subscription, with places of its own for the messages its refused
//! publishes hand back.

use core::fmt;
use core::ops::Deref;

use ruststream_core::PlaceRoom;

use super::builder::{HeadersUnset, MessageBody, PublishBuilder, message_of};
use super::sink::{PublishSink, SinkOutcome, UnnamedCodec};
use crate::{HeaderSet, MaybeSend, OutgoingDestination, OutgoingFor, Publisher};

/// A publisher with `N` places of its own for the messages its refused publishes hand back, made
/// by [`refusal_capacity`](super::PublishExt::refusal_capacity).
///
/// Without `alloc`, a refused publish moves its message into a free place, and the publishes that
/// run through this publisher at once share the places; when every place is taken, a refused
/// publish comes back without its message. A publisher used without the step keeps one place in
/// each of its publishes instead. With `alloc` a refused message is boxed, and the places are
/// values of no size.
///
/// It dereferences to the publisher it wraps, so the publisher's own surface stays reachable, and
/// a clone is another handle on that publisher with places of its own. Its typed publishes go
/// through its own [`message`](Self::message), which takes the shared places; a helper generic
/// over a [`Publisher`] is handed the wrapped publisher, `&*publisher`, whose publishes keep a
/// place each.
///
/// # Examples
///
/// See [`refusal_capacity`](super::PublishExt::refusal_capacity).
///
/// A helper generic over a publisher would publish past the shared places, so it does not take
/// one with places of its own:
///
/// ```compile_fail,E0277
/// use ruststream::Publisher;
/// use ruststream::memory::MemoryBroker;
/// use ruststream::runtime::PublishExt;
///
/// /// Any publisher.
/// fn through<P: Publisher>(publisher: &P) -> &P {
///     publisher
/// }
///
/// let readings = MemoryBroker::new().publisher().refusal_capacity::<2>();
/// through(&readings);
/// ```
pub struct WithPlaces<P, const N: usize> {
    publisher: P,
    // The storage a refusal of this publisher takes a place in; the borrow a refusal holds is its
    // lease, so the places belong to this one task.
    places: PlaceRoom<N>,
}

impl<P, const N: usize> WithPlaces<P, N> {
    /// `publisher`, with `N` places of its own.
    pub(crate) fn new(publisher: P) -> Self {
        const {
            assert!(
                N > 0,
                "a publisher keeps at least one place for refused messages: give \
                 `refusal_capacity` a count above zero"
            );
        }
        Self {
            publisher,
            places: PlaceRoom::new(),
        }
    }
}

impl<P: Publisher, const N: usize> WithPlaces<P, N> {
    /// Starts a typed publish of a `#[derive(Outgoing)]` value, as
    /// [`PublishExt::message`](super::PublishExt::message) does: a refused publish keeps its
    /// message in one of this publisher's places.
    ///
    /// # Examples
    ///
    /// See [`refusal_capacity`](super::PublishExt::refusal_capacity).
    pub fn message<'a, T>(
        &'a self,
        value: &'a T,
    ) -> PublishBuilder<
        SharedPlaces<'a, P, N>,
        MessageBody<'a, T>,
        UnnamedCodec,
        HeadersUnset,
        T::Form,
    >
    where
        T: OutgoingDestination,
    {
        message_of(SharedPlaces(self), value, UnnamedCodec::new())
    }
}

impl<P: Clone, const N: usize> Clone for WithPlaces<P, N> {
    fn clone(&self) -> Self {
        Self {
            publisher: self.publisher.clone(),
            places: PlaceRoom::new(),
        }
    }
}

impl<P, const N: usize> Deref for WithPlaces<P, N> {
    type Target = P;

    fn deref(&self) -> &P {
        &self.publisher
    }
}

impl<P, const N: usize> fmt::Debug for WithPlaces<P, N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WithPlaces")
            .field("places", &N)
            .finish_non_exhaustive()
    }
}

/// The sink of a publisher with places of its own: a refused message takes one of the publisher's
/// places, so the publish keeps none. Machinery; [`WithPlaces::message`] builds on it.
#[doc(hidden)]
#[derive(Debug)]
pub struct SharedPlaces<'a, P, const N: usize>(&'a WithPlaces<P, N>);

impl<P: Publisher, const N: usize> PublishSink for SharedPlaces<'_, P, N> {
    type Payload = P::Payload;
    type Error = P::Error;
    type Options = P::Options;
    type OwnPlaces = PlaceRoom<0>;

    fn send<'p>(
        &'p mut self,
        msg: OutgoingFor<'_, Self::Payload>,
        options: Option<&Self::Options>,
        _own: &'p PlaceRoom<0>,
    ) -> impl Future<Output = SinkOutcome<'p, Self::Error>> + MaybeSend {
        let placed: &WithPlaces<P, N> = self.0;
        placed
            .publisher
            .publish_kept(msg, options, placed.places.places())
    }

    fn base_headers(&self) -> Option<&HeaderSet> {
        self.0.publisher.base_headers()
    }
}
