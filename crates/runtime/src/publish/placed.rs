//! A publisher used outside a subscription, with places of its own for the messages its refused
//! publishes hand back.

use core::fmt;
use core::ops::Deref;

use ruststream_core::{PlaceRoom, Places};

use super::builder::{HeadersUnset, MessageBody, PublishBuilder, message_of};
use super::sink::{KeepsPlaces, KeptPlaces, SinkOutcome, UnnamedCodec};
use crate::{HeaderSet, MaybeSend, OutgoingDestination, OutgoingFor, PublishOutcome, Publisher};

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
/// a clone is another handle on that publisher with places of its own.
///
/// # Examples
///
/// See [`refusal_capacity`](super::PublishExt::refusal_capacity).
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

    /// The publisher this one wraps, without its places.
    #[must_use]
    pub fn into_inner(self) -> P {
        self.publisher
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
    ) -> PublishBuilder<KeptPlaces<'a, Self>, MessageBody<'a, T>, UnnamedCodec, HeadersUnset, T::Form>
    where
        T: OutgoingDestination,
    {
        message_of(KeptPlaces(self), value, UnnamedCodec::new())
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

impl<P: Publisher, const N: usize> Publisher for WithPlaces<P, N> {
    type Payload = P::Payload;
    type Error = P::Error;
    type Options = P::Options;

    fn publish<'msg>(
        &self,
        msg: OutgoingFor<'msg, Self::Payload>,
        options: Option<&Self::Options>,
    ) -> impl Future<Output = PublishOutcome<'msg, Self::Error, Self::Payload>> + MaybeSend {
        self.publisher.publish(msg, options)
    }

    fn publish_kept<'p>(
        &self,
        msg: OutgoingFor<'_, Self::Payload>,
        options: Option<&Self::Options>,
        places: Places<'p>,
    ) -> impl Future<Output = SinkOutcome<'p, Self::Error>> + MaybeSend {
        self.publisher.publish_kept(msg, options, places)
    }

    fn base_headers(&self) -> Option<&HeaderSet> {
        self.publisher.base_headers()
    }
}

impl<P: Publisher, const N: usize> KeepsPlaces for WithPlaces<P, N> {
    fn places(&self) -> Places<'_> {
        self.places.places()
    }
}
