//! One arena entry of the local family: the wired live value of a marker on a chain started with
//! [`Router::local`](crate::Router::local), and the [`LocalOutEntry`] bound a
//! [`LocalHandle`](crate::LocalHandle) body states.

use alloc::boxed::Box;
use alloc::format;
use alloc::vec::Vec;
use core::fmt;
use core::marker::PhantomData;
use core::ops::Deref;

#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
use crate::codec::Codec;
use crate::inject::LocalFromStartup;
use crate::metadata::OutgoingMessageMetadata;
use crate::publish::{
    HeadersUnset, LocalOutPipeline, LocalPublishSink, MessageBody, PublishBuilder, message_of,
};
use crate::router::LocalPipeline;
use crate::slot::{ContainsMessage, OutSlot, PublishedThrough};
#[cfg(feature = "testing")]
use crate::testing::coordinator::{
    Origin, PipelinePublish, paired, publishing_to, record_slot_publish,
};
use crate::{
    HeaderMap, LocalBroker, LocalConnected, LocalPublishPolicy, LocalPublisher,
    OutgoingDestination, OutgoingFor, PairError,
};

use super::arena::SlotDictionary;

/// The phantom carrying a local slot's marker and declared message set.
type LocalSlotShape<M, Body> = PhantomData<fn() -> (M, Body)>;

/// One arena entry of the local family: the wired live value of the marker `M` and the publish
/// path the include site gave it, as [`Slot`](super::Slot) holds them on the multi-thread runner.
///
/// Neither the entry nor its wired value has to be `Send` or `Sync`: the arena lives in the one
/// task that polls the app. `Deref` reaches the live value's own surface, and
/// [`message`](Self::message) starts a typed publish over the include site's codec and the
/// marker's dictionary.
///
/// A body does not spell this type out: it names one parameter per marker and bounds it with
/// [`LocalOutEntry`].
pub struct LocalSlot<M, W, E, Pipe = LocalPipeline, Body = ()> {
    wired: W,
    codec: E,
    pipeline: Pipe,
    _declared: LocalSlotShape<M, Body>,
    /// The broker the slot's publisher was paired against, which its publishes go to.
    #[cfg(feature = "testing")]
    origin: Origin,
}

impl<M, W, E, Pipe, Body> fmt::Debug for LocalSlot<M, W, E, Pipe, Body> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalSlot").finish_non_exhaustive()
    }
}

impl<M, W, E, Pipe, Body> Deref for LocalSlot<M, W, E, Pipe, Body> {
    type Target = W;

    fn deref(&self) -> &W {
        &self.wired
    }
}

impl<M: OutSlot, W: LocalPublisher, E, Pipe, Body> LocalSlot<M, W, E, Pipe, Body> {
    /// Starts a typed publish through the slot, on the message type's own wire: the slot's
    /// [`message`](super::Slot::message) of the multi-thread runner, read the same way.
    ///
    /// # Examples
    ///
    /// See [`LocalOutEntry`].
    pub fn message<'a, T, Index>(
        &'a self,
        value: &'a T,
    ) -> PublishBuilder<&'a Self, MessageBody<'a, T>, &'a E, HeadersUnset, T::Form>
    where
        Body: ContainsMessage<T, Index>,
        T: OutgoingDestination + PublishedThrough<M>,
        &'a Self: LocalPublishSink,
    {
        message_of(self, value, &self.codec)
    }
}

// The publish travels the slot's path: its transforms, then the leaf call. A slot with no transform
// lends the leaf the message as the builder made it, with nothing rebuilt or copied. Under the
// harness, the leaf records the publish with its slot, and with the broker it went to, as the
// multi-thread runner's slot records it.
impl<M, W, E, Pipe, Body> LocalPublishSink for &LocalSlot<M, W, E, Pipe, Body>
where
    M: OutSlot,
    W: LocalPublisher,
    Pipe: LocalOutPipeline<W>,
{
    type Payload = W::Payload;
    type Error = W::Error;
    type Options = W::Options;

    // The publisher's own future, which is `Send` exactly where the publisher's is.
    #[allow(clippy::future_not_send)]
    fn send(
        &mut self,
        msg: OutgoingFor<'_, Self::Payload>,
        options: Option<&Self::Options>,
    ) -> impl Future<Output = Result<(), Self::Error>> {
        #[cfg(feature = "testing")]
        let slot: &LocalSlot<M, W, E, Pipe, Body> = self;
        #[cfg(feature = "testing")]
        return publishing_to(slot.origin, async move {
            let leaf = Attributed::<M, W>(&slot.wired, PhantomData);
            slot.pipeline.send(&leaf, msg, options).await
        });
        #[cfg(not(feature = "testing"))]
        self.pipeline.send(&self.wired, msg, options)
    }

    fn base_headers(&self) -> Option<&HeaderMap> {
        self.wired.base_headers()
    }
}

/// The injected slot of the local family: pairs the marker's attached policy against the
/// connected broker and keeps the live value under the include site's codec and pipeline. A
/// failing pair surfaces at startup with the slot's name.
// Polled on the one thread that hosts the app: `Send` exactly where the types it runs are.
#[allow(clippy::future_not_send)]
impl<B, Sub, Policy, E, Pipe, M, Body> LocalFromStartup<B, Sub, (Policy, E, Pipe)>
    for LocalSlot<M, <Policy as LocalPublishPolicy<LocalConnected<B>>>::Live, E, Pipe, Body>
where
    B: LocalBroker,
    Policy: LocalPublishPolicy<LocalConnected<B>>,
    M: OutSlot,
{
    async fn resolve_local(
        (policy, codec, pipeline): (Policy, E, Pipe),
        connected: &LocalConnected<B>,
        _subscriber: &Sub,
    ) -> Result<Self, PairError> {
        let pairing = policy.pair(connected);
        #[cfg(feature = "testing")]
        let (live, origin) = paired(connected, pairing).await;
        #[cfg(not(feature = "testing"))]
        let live = pairing.await;
        let live = live.map_err(|err| {
            PairError::from_boxed(Box::from(format!(
                "pairing the publisher for the `{}` slot failed: {err}",
                M::NAME,
            )))
        })?;
        Ok(Self {
            wired: live,
            codec,
            pipeline,
            _declared: PhantomData,
            #[cfg(feature = "testing")]
            origin,
        })
    }
}

/// Publishes `msg` through `wired`, recorded with the slot `M` for the harness driving the
/// dispatch, if any: the slot's record and the destination's, and the hooks each owes.
#[cfg(feature = "testing")]
#[allow(clippy::future_not_send)] // as the sink's own future
/// The slot's publisher under the harness: records each publish that leaves the slot's path,
/// with the slot's name, then hands it on.
#[cfg(feature = "testing")]
struct Attributed<'a, M, W>(&'a W, PhantomData<fn() -> M>);

#[cfg(feature = "testing")]
impl<M: OutSlot, W: LocalPublisher> LocalPublisher for Attributed<'_, M, W> {
    type Payload = W::Payload;
    type Error = W::Error;
    type Options = W::Options;

    // The publisher's own future, which is `Send` exactly where the publisher's is.
    #[allow(clippy::future_not_send)]
    async fn publish(
        &self,
        msg: OutgoingFor<'_, Self::Payload>,
        options: Option<&Self::Options>,
    ) -> Result<(), Self::Error> {
        let recorded = PipelinePublish::capture(&msg, options);
        let hooks = record_slot_publish(M::NAME, &msg, options);
        let sent = self.0.publish(msg, options).await;
        hooks.run().await;
        if sent.is_ok() {
            recorded.sent().await;
        }
        sent
    }

    fn base_headers(&self) -> Option<&HeaderMap> {
        self.0.base_headers()
    }
}

/// What a local entry's codec position is: a [`Codec`] in a build that has one, so a body
/// generic over its entry encodes through it, and anything in a build without one. Machinery;
/// never named in user code.
#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
#[doc(hidden)]
pub trait SlotEncoding: Codec {}

#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
impl<C: Codec> SlotEncoding for C {}

/// See the codec counterpart: without a codec the position is
/// [`UnnamedCodec`](crate::UnnamedCodec), and an encoded publish names the missing codec.
#[cfg(not(any(feature = "json", feature = "cbor", feature = "msgpack")))]
#[doc(hidden)]
pub trait SlotEncoding {}

#[cfg(not(any(feature = "json", feature = "cbor", feature = "msgpack")))]
impl<C> SlotEncoding for C {}

/// [`OutEntry`](super::OutEntry) for a [`LocalHandle`](crate::LocalHandle) body: what one arena
/// entry is, stated as the mount's choices, with no `Send` or `Sync` demanded of it.
///
/// A body names one type parameter per marker and bounds it here, with the capability of the
/// local family it needs on [`Wire`](Self::Wire); [`Outs::get`](super::Outs::get) picks the
/// entry by marker as on the multi-thread runner.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "json")]
/// # mod demo {
/// use ruststream_core::{
///     CallerName, LocalPublisher, MessageHeaders, NoHeaders, OutgoingDestination,
/// };
/// use ruststream_runtime::{
///     Context, HandlerOutcome, LocalHandle, LocalOutEntry, OutSlot, Outs, PublishedThrough,
///     Reads,
/// };
/// # #[derive(serde::Deserialize)]
/// # struct Order { id: u64 }
///
/// #[derive(serde::Serialize)]
/// struct Audited {
///     id: u64,
/// }
///
/// impl OutgoingDestination for Audited {
///     type Form = CallerName;
/// }
///
/// impl MessageHeaders for Audited {
///     type Contract = NoHeaders;
/// }
///
/// struct Audit;
///
/// impl OutSlot for Audit {
///     const NAME: &'static str = "Audit";
///     type Destination = Reads;
/// }
///
/// impl PublishedThrough<Audit> for Audited {}
///
/// struct Mirror;
///
/// impl<A> LocalHandle<Order, (), Outs<(A,)>> for Mirror
/// where
///     A: LocalOutEntry<Audit, Wire: LocalPublisher>,
/// {
///     async fn handle(
///         &self,
///         order: &Order,
///         outs: &Outs<(A,)>,
///         _ctx: &mut Context<'_>,
///     ) -> Result<(), HandlerOutcome> {
///         outs.get(Audit)
///             .message(&Audited { id: order.id })
///             .to("audit")
///             .publish()
///             .await
///             .map_err(|_| HandlerOutcome::retry())
///     }
/// }
/// # }
/// ```
pub trait LocalOutEntry<M, Body = ()> {
    /// The wired live value: the bound policy's live form.
    type Wire;

    /// The codec the entry's typed publishes encode with: the include site's, or
    /// [`UnnamedCodec`](crate::UnnamedCodec) in a build without one, where the entry publishes
    /// only the types that carry their own bytes.
    type Enc: SlotEncoding;

    /// The publish path every message leaving the entry travels: the slot's transforms above
    /// the leaf call.
    type Pipe: LocalOutPipeline<Self::Wire>;

    /// The entry behind the bound. Machinery: [`Outs::get`](super::Outs::get) resolves the
    /// concrete slot through it.
    #[doc(hidden)]
    fn entry(&self) -> &LocalSlot<M, Self::Wire, Self::Enc, Self::Pipe, Body>;
}

impl<M, W, E, Pipe, Body> LocalOutEntry<M, Body> for LocalSlot<M, W, E, Pipe, Body>
where
    E: SlotEncoding,
    Pipe: LocalOutPipeline<W>,
{
    type Wire = W;
    type Enc = E;
    type Pipe = Pipe;

    fn entry(&self) -> &Self {
        self
    }
}

/// The marker tuple of an arena of the local family: [`EntryMarkers`](super::EntryMarkers) for
/// [`LocalSlot`] entries. A separate trait, so an arena a body left generic resolves its entries
/// to one family by the router it mounts on. Machinery; never named in user code.
#[doc(hidden)]
pub trait LocalEntryMarkers {
    /// The markers, as [`LocalHasSlots::Markers`](crate::LocalHasSlots::Markers) reports them.
    type Markers;
}

macro_rules! impl_local_entry_markers {
    ($(($($m:ident: $w:ident / $e:ident / $p:ident / $b:ident),+))+) => {$(
        impl<$($m: OutSlot, $w, $e, $p, $b),+> LocalEntryMarkers
            for ($(LocalSlot<$m, $w, $e, $p, $b>,)+)
        {
            type Markers = ($($m,)+);
        }

        impl<$($m: OutSlot, $w, $e, $p, $b),+> SlotDictionary
            for ($(LocalSlot<$m, $w, $e, $p, $b>,)+)
        {
            fn outgoing() -> Vec<OutgoingMessageMetadata> {
                let mut declared = Vec::new();
                $(declared.extend($m::outgoing());)+
                declared
            }
        }
    )+};
}

impl_local_entry_markers! {
    (M0: W0 / E0 / P0 / B0)
    (M0: W0 / E0 / P0 / B0, M1: W1 / E1 / P1 / B1)
    (M0: W0 / E0 / P0 / B0, M1: W1 / E1 / P1 / B1, M2: W2 / E2 / P2 / B2)
}

#[cfg(test)]
mod tests {
    use alloc::format;
    use alloc::vec;

    use super::*;
    use crate::Reads;
    use crate::handle::outs::Slot;

    /// A marker whose dictionary declares one message, as `#[derive(OutSlot)]` with
    /// `#[publishes(..)]` writes it.
    struct Alerts;

    impl OutSlot for Alerts {
        const NAME: &'static str = "Alerts";
        type Destination = Reads;

        fn outgoing() -> Vec<OutgoingMessageMetadata> {
            vec![OutgoingMessageMetadata::new("alerts", "Alert")]
        }
    }

    /// The document a subscription contributes reads its arena's dictionaries, so a local
    /// subscription's slots are described with the same entries as the multi-thread runner's.
    #[test]
    fn a_local_arena_describes_its_slots_as_the_multi_thread_arena_does() {
        let local = <(LocalSlot<Alerts, (), (), (), ()>,) as SlotDictionary>::outgoing();
        let threaded = <(Slot<Alerts, (), (), (), ()>,) as SlotDictionary>::outgoing();
        assert_eq!(format!("{local:?}"), format!("{threaded:?}"));
        assert_eq!(local.len(), 1);
        assert_eq!(local[0].channel, "alerts");
    }
}
