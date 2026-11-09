//! The copy path of a registration whose runner keeps its retry copies in place: the broker's
//! default publisher and the subscription's redelivery address, paired once when the
//! registration opens, typed in the route so a copy is published with no dynamic call.

// Written over the contract traits for a runner that polls on one thread.
#![allow(clippy::future_not_send)]

use core::any::type_name;
use core::future::ready;
use core::marker::PhantomData;
use core::mem;
use ruststream_core::{MaybeSend, MaybeSendSync, MaybeSync, Places};
use ruststream_core::{NameText, RedeliveryAddress};

use ruststream_core::PayloadBuf;

use super::{RetryDestinationError, startup_failure};
use crate::copied::{KeptName, NoRoom, diagnostic_name, form_of, lay_over};
use crate::host::KeptDelivery;
use crate::lifecycle::BoxError;
use crate::publish::{
    ForReply, PublishContext, PublishTransform, PublishTransformIdentity, PublishTransformStack,
};
use crate::publish_failure::Refusal;
use crate::{
    AddressedCopies, Broker, BrokerMoves, Connected, DefaultPublish, HeaderSet, NamedCopies,
    Outgoing, OutgoingMessage, PublishPolicy, Publisher, Reads, RedeliveryAddressed, Settle,
    SubscriptionSource,
};

/// Where a kept copy leaves: the publisher and the address that reaches the subscription
/// again. Machinery; never named directly.
#[doc(hidden)]
pub trait CopyPublisher: MaybeSync {
    /// The per-message settings a kept copy carries to its publish: what the mount site's
    /// transforms wrote when the copy was kept.
    type Options: MaybeSendSync;

    /// The address a copy back to the subscription is published under.
    fn address(&self) -> &str;

    /// Publishes one copy to `destination`, as [`PrepareCopy::prepare`] left it. A refused copy
    /// moves into one of `places`.
    ///
    /// # Errors
    ///
    /// What the publisher returned.
    fn publish_copy<'p>(
        &self,
        destination: &str,
        payload: &[u8],
        headers: HeaderSet,
        options: Option<&Self::Options>,
        places: Places<'p>,
    ) -> impl Future<Output = Result<(), Refusal<'p>>> + MaybeSend;
}

/// A copy path that prepares the copy of a delivery whose broker context is `Cx` while the
/// delivery is still at hand. Machinery; never named directly.
#[doc(hidden)]
pub trait PrepareCopy<Cx>: CopyPublisher {
    /// Puts the publisher's own constants under the delivery's headers and runs the mount site's
    /// transforms over the copy bound for `destination`, the way a publish through the
    /// multi-thread runner's retry slot does.
    fn prepare<M: Settle>(
        &self,
        payload: &mut PayloadBuf,
        headers: &mut HeaderSet,
        options: &mut Option<Self::Options>,
        destination: &str,
        delivery: &KeptDelivery<'_, M, Cx>,
    );
}

/// A copy path paired from a publish policy, addressed where the mount site or the
/// subscription's descriptor said, with the mount site's retry transforms. Machinery; never
/// named directly.
#[doc(hidden)]
#[derive(Debug)]
pub struct AddressedCopy<Live, Stack = PublishTransformIdentity> {
    live: Live,
    address: KeptName,
    stack: Stack,
}

impl<Live: Publisher, Stack: MaybeSync> CopyPublisher for AddressedCopy<Live, Stack> {
    type Options = Live::Options;

    fn address(&self) -> &str {
        kept_str(&self.address)
    }

    async fn publish_copy<'p>(
        &self,
        destination: &str,
        payload: &[u8],
        headers: HeaderSet,
        options: Option<&Live::Options>,
        places: Places<'p>,
    ) -> Result<(), Refusal<'p>> {
        // A transport that reads the bytes is lent the record's buffer; one that keeps them is
        // handed a copy of its own.
        let Some(form) = form_of::<Live::Payload>(payload) else {
            return Err(Refusal::new(NoRoom));
        };
        #[cfg(ruststream_alloc)]
        let msg = OutgoingMessage::assembled(destination, form, headers);
        // The copy's map lies in this publish's state, and the message borrows it there.
        #[cfg(not(ruststream_alloc))]
        let msg = OutgoingMessage::assembled(destination, form, &headers);
        self.live
            .publish(msg, options)
            .await
            .map_err(|failed| Refusal::returned::<Live::Payload>(failed, places))
    }
}

impl<Cx, Live, Stack> PrepareCopy<Cx> for AddressedCopy<Live, Stack>
where
    Live: Publisher,
    Stack: KeptTransforms<Cx, Live::Options> + MaybeSync,
{
    fn prepare<M: Settle>(
        &self,
        payload: &mut PayloadBuf,
        headers: &mut HeaderSet,
        options: &mut Option<Live::Options>,
        destination: &str,
        delivery: &KeptDelivery<'_, M, Cx>,
    ) {
        // The publisher's own constants sit under the delivery's headers, as on every publish
        // through it, so the transforms see the copy as it will be sent.
        // Where the fixed map has no room for the constants as well, the copy leaves with the
        // delivery's own headers: a retry that arrives without a constant beats one that is lost.
        if let Some(laid) = self
            .live
            .base_headers()
            .and_then(|base| lay_over(base, headers))
        {
            *headers = laid;
        }
        self.stack
            .apply_kept(payload, headers, options, destination, delivery);
    }
}

/// The transforms a mount site put on `.out_retry(..)` on the embedded runner, run over a copy
/// as it is kept. Machinery; never named directly.
///
/// A copy waits in the store as data, so the transforms run when it is kept, while the delivery
/// it copies is still at hand to read; the copy then leaves as they left it.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not fit a retry copy kept on the embedded runner",
    label = "a transform on this `.out_retry(..)`",
    note = "a kept copy's transforms implement `PublishTransform<ForReply<C>, Options>` for the \
            subscription's context `C` and the retry publisher's own `Options`, with \
            `Destination = Reads`: a copy kept in the mount's store leaves for the destination \
            the mount site or the descriptor named"
)]
pub trait KeptTransforms<Cx, Options> {
    /// Runs the stack over the copy bound for `destination`.
    fn apply_kept<M: Settle>(
        &self,
        payload: &mut PayloadBuf,
        headers: &mut HeaderSet,
        options: &mut Option<Options>,
        destination: &str,
        delivery: &KeptDelivery<'_, M, Cx>,
    );
}

// Nothing to run: the copy is kept as it was filled, and the context is never built.
impl<Cx, Options> KeptTransforms<Cx, Options> for PublishTransformIdentity {
    #[inline]
    fn apply_kept<M: Settle>(
        &self,
        _payload: &mut PayloadBuf,
        _headers: &mut HeaderSet,
        _options: &mut Option<Options>,
        _destination: &str,
        _delivery: &KeptDelivery<'_, M, Cx>,
    ) {
    }
}

impl<Cx, Options, Inner, Outer> KeptTransforms<Cx, Options> for PublishTransformStack<Inner, Outer>
where
    Self: PublishTransform<ForReply<Cx>, Options, Destination = Reads>,
{
    fn apply_kept<M: Settle>(
        &self,
        payload: &mut PayloadBuf,
        headers: &mut HeaderSet,
        options: &mut Option<Options>,
        destination: &str,
        delivery: &KeptDelivery<'_, M, Cx>,
    ) {
        // The record's buffer and map move into the message and back: nothing is copied, and the
        // buffer keeps its capacity for the record's next copy.
        let mut out = Outgoing::new(destination, mem::take(payload));
        *out.headers_mut() = mem::take(headers);
        let context = (delivery.build_cx)(delivery.message);
        let view = PublishContext::new(delivery.subscription, delivery.message.headers(), &context);
        self.apply(&mut out, options, &view);
        #[cfg(ruststream_alloc)]
        {
            *payload = mem::take(out.payload_mut());
        }
        // The message was built over an owned buffer, so reaching it cannot fail: the fixed form
        // returns a `Result` because it may have to copy lent bytes, which this message never lends.
        #[cfg(not(ruststream_alloc))]
        if let Ok(buffer) = out.payload_mut() {
            *payload = mem::take(buffer);
        }
        *headers = mem::take(out.headers_mut());
    }
}

/// The copy path of a plan with nowhere to keep a copy: there is none. Machinery; never named
/// directly.
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub enum NoCopyPath {}

// No value of the type exists, so no method is ever called: each body is the empty match that
// proves it, the idiom `core::convert::Infallible`'s own impls use.
#[allow(clippy::uninhabited_references, clippy::unused_async_trait_impl)]
impl CopyPublisher for NoCopyPath {
    type Options = ();

    fn address(&self) -> &str {
        match *self {}
    }

    async fn publish_copy<'p>(
        &self,
        _destination: &str,
        _payload: &[u8],
        _headers: HeaderSet,
        _options: Option<&()>,
        _places: Places<'p>,
    ) -> Result<(), Refusal<'p>> {
        match *self {}
    }
}

#[allow(clippy::uninhabited_references)]
impl<Cx> PrepareCopy<Cx> for NoCopyPath {
    fn prepare<M: Settle>(
        &self,
        _payload: &mut PayloadBuf,
        _headers: &mut HeaderSet,
        _options: &mut Option<()>,
        _destination: &str,
        _delivery: &KeptDelivery<'_, M, Cx>,
    ) {
        match *self {}
    }
}

/// A plan that keeps no retry copy: its registration pairs nothing for them, and `retry_after`
/// requeues the delivery. Machinery; never named directly.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default)]
pub struct NoCopies;

/// A plan that keeps its retry copies in a store of its own: its registration pairs the
/// broker's default publisher and the subscription's redelivery address when it opens.
/// Machinery; never named directly.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default)]
pub struct KeepCopies;

/// A registration whose mount chain declared nothing about its retries. Machinery; never named
/// directly.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default)]
pub struct Undeclared;

/// A registration whose mount chain declared a cap or a dead-letter destination, on a descriptor
/// whose copies take the path `Copies`. Machinery; never named directly.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default)]
pub struct DeclaredOn<Copies>(PhantomData<fn() -> Copies>);

/// Whether a plan that keeps these copies applies what the registration declared.
///
/// A cap and a dead-letter destination are applied by whoever publishes the copies: the broker,
/// where the descriptor says it moves the delivery itself, and the mount's own store otherwise.
/// A registration whose copies this process publishes declares them only beside the store that
/// holds them. Machinery; never named directly.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "a cap or a dead-letter destination on this registration has no store of copies \
               to apply it",
    label = "declared here without `.retry_capacity::<CAP>()`",
    note = "this process publishes the copies of the registration's descriptor, and on the \
            embedded runner a copy waits in the mount's own store: add `.retry_capacity::<CAP>()` \
            after the declaration, with room for the copies that wait at once"
)]
pub trait AppliesDeclaration<Declaration> {}

impl AppliesDeclaration<Undeclared> for NoCopies {}

impl AppliesDeclaration<Undeclared> for KeepCopies {}

// The broker moves a spent delivery itself and takes the declaration into its own topology.
impl AppliesDeclaration<DeclaredOn<BrokerMoves>> for NoCopies {}

impl<Copies> AppliesDeclaration<DeclaredOn<Copies>> for KeepCopies {}

/// What a plan's copies need when its registration opens on broker `B` through `Source`.
/// Machinery; never named directly.
#[doc(hidden)]
pub trait PairCopies<B: Broker, Source> {
    /// The copy path the registration opens with.
    type Path: CopyPublisher;

    /// Pairs the copy path against the connected broker; `None` where the plan keeps no copy.
    ///
    /// # Errors
    ///
    /// The broker's refusal to pair the publisher or to say the address.
    fn pair(
        source: &Source,
        connected: &Connected<B>,
    ) -> impl Future<Output = Result<Option<Self::Path>, BoxError>>;
}

impl<B: Broker, Source> PairCopies<B, Source> for NoCopies {
    type Path = NoCopyPath;

    fn pair(
        _source: &Source,
        _connected: &Connected<B>,
    ) -> impl Future<Output = Result<Option<NoCopyPath>, BoxError>> {
        ready(Ok(None))
    }
}

impl<B, Source> PairCopies<B, Source> for KeepCopies
where
    B: Broker,
    Source: SubscriptionSource<Connected<B>>,
    Source::Copies: KeptAddress<B, Source>,
{
    type Path = <Source::Copies as KeptAddress<B, Source>>::Path;

    async fn pair(
        source: &Source,
        connected: &Connected<B>,
    ) -> Result<Option<Self::Path>, BoxError> {
        <Source::Copies as KeptAddress<B, Source>>::pair(source, connected)
            .await
            .map(Some)
    }
}

/// A subscription whose kept copies this process publishes to one address. Machinery; never
/// named directly.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "`.retry_capacity` keeps copies of `{Source}`'s deliveries, and this process does \
               not address them",
    label = "`{Source}` declares `Copies = {Self}`",
    note = "a copy kept in the mount's store goes back to the subscription through the address \
            its descriptor reports, so the descriptor declares `Copies = AddressedCopies` and \
            implements `RedeliveryAddressed`",
    note = "a descriptor whose deliveries the broker moves itself (`BrokerMoves`) or that reads \
            many destinations (`NamedCopies`) keeps no copy in place: leave out \
            `.retry_capacity`, and `retry_after` requeues"
)]
pub trait KeptAddress<B: Broker, Source> {
    /// The copy path paired for it.
    type Path: CopyPublisher;

    /// Pairs the broker's default publisher and asks the descriptor for the address.
    ///
    /// # Errors
    ///
    /// The broker's refusal to pair the publisher or to say the address.
    fn pair(
        source: &Source,
        connected: &Connected<B>,
    ) -> impl Future<Output = Result<Self::Path, BoxError>>;
}

/// A kept address, borrowed as text.
#[cfg(ruststream_alloc)]
fn kept_str(address: &KeptName) -> &str {
    address.as_str()
}

/// A kept address, borrowed as text.
#[cfg(not(ruststream_alloc))]
const fn kept_str(address: &KeptName) -> &str {
    address
}

/// The loop's own copy of an address a descriptor reported: whole in either build.
#[cfg(ruststream_alloc)]
fn kept_address(address: RedeliveryAddress) -> KeptName {
    address.into_text().into_owned()
}

/// The address a descriptor reported, kept as the literal it was made from.
#[cfg(not(ruststream_alloc))]
fn kept_address(address: RedeliveryAddress) -> KeptName {
    address.into_text()
}

/// The loop's own copy of a destination the mount site named: whole in either build.
#[cfg(ruststream_alloc)]
fn kept_named(name: NameText) -> KeptName {
    name.into_owned()
}

/// A destination the mount site named, kept as the literal it is.
#[cfg(not(ruststream_alloc))]
const fn kept_named(name: NameText) -> KeptName {
    name
}

type DefaultLive<B> =
    <<Connected<B> as DefaultPublish>::Policy as PublishPolicy<Connected<B>>>::Live;

impl<B, Source> KeptAddress<B, Source> for AddressedCopies
where
    B: Broker,
    Source: RedeliveryAddressed<Connected<B>>,
    Connected<B>: DefaultPublish,
    DefaultLive<B>: Publisher,
{
    type Path = AddressedCopy<DefaultLive<B>>;

    async fn pair(source: &Source, connected: &Connected<B>) -> Result<Self::Path, BoxError> {
        let address = source
            .redelivery_address(connected)
            .await
            .map_err(|err| startup_failure("redelivery address", source.name(), err))?;
        let live = <Connected<B> as DefaultPublish>::Policy::default()
            .pair(connected)
            .await
            .map_err(|err| startup_failure("copy publisher", source.name(), err))?;
        Ok(AddressedCopy {
            live,
            address: kept_address(address),
            stack: PublishTransformIdentity,
        })
    }
}

/// The deferred-retry position a local mount chain bound with `.out_retry(policy)`: the policy a
/// kept copy leaves through, the destination a `.to(name)` after it gave the copies, and the
/// transforms its `.transform(..)` steps put on them. Machinery; never named directly.
#[doc(hidden)]
#[derive(Debug)]
pub struct LocalRetrySlot<Policy, Stack = PublishTransformIdentity> {
    pub(crate) policy: Policy,
    pub(crate) destination: Option<NameText>,
    pub(crate) stack: Stack,
}

/// What a plan's copies need when a registration that bound `.out_retry(policy)` opens on broker
/// `B` through `Source`: the copy path paired from the mount site's policy. Machinery; never named
/// directly.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "`.out_retry(..)` on the embedded runner names the publisher of copies the mount \
               keeps, and this mount keeps none",
    label = "bound here without `.retry_capacity::<CAP>()`",
    note = "on the embedded runner a `retry_after` copy waits in the mount's own store: add \
            `.retry_capacity::<CAP>()` after `.out_retry(..)`, with room for the copies that \
            wait at once"
)]
pub trait PairSlot<B: Broker, Source, Policy, Stack, Cx> {
    /// The copy path the registration opens with, preparing copies of deliveries whose broker
    /// context is `Cx`.
    type Path: PrepareCopy<Cx>;

    /// Pairs the slot's policy against the connected broker, addressed where the slot or the
    /// descriptor says.
    ///
    /// # Errors
    ///
    /// The broker's refusal to pair the policy or to say the address.
    fn pair_slot(
        slot: LocalRetrySlot<Policy, Stack>,
        source: &Source,
        connected: &Connected<B>,
    ) -> impl Future<Output = Result<Option<Self::Path>, BoxError>>;
}

impl<B, Source, Policy, Stack, Cx> PairSlot<B, Source, Policy, Stack, Cx> for KeepCopies
where
    B: Broker,
    Source: SubscriptionSource<Connected<B>>,
    Source::Copies: SlotAddress<B, Source>,
    Policy: PublishPolicy<Connected<B>>,
    Policy::Live: Publisher,
    // The transforms run over each copy as it is kept, reading the delivery it copies.
    Stack: KeptTransforms<Cx, <Policy::Live as Publisher>::Options> + MaybeSync,
{
    type Path = AddressedCopy<Policy::Live, Stack>;

    async fn pair_slot(
        slot: LocalRetrySlot<Policy, Stack>,
        source: &Source,
        connected: &Connected<B>,
    ) -> Result<Option<Self::Path>, BoxError> {
        // A destination the mount site named is where the copies go; the descriptor is asked
        // only otherwise, as on the multi-thread runner.
        let address = match slot.destination {
            Some(named) => kept_named(named),
            None => <Source::Copies as SlotAddress<B, Source>>::address(source, connected).await?,
        };
        let live = slot
            .policy
            .pair(connected)
            .await
            .map_err(|err| startup_failure("copy publisher", source.name(), err))?;
        Ok(Some(AddressedCopy {
            live,
            address,
            stack: slot.stack,
        }))
    }
}

/// Where a copy goes back to the subscription when the mount site named no destination.
/// Machinery; never named directly.
#[doc(hidden)]
pub trait SlotAddress<B: Broker, Source> {
    /// The descriptor's own address for its copies.
    ///
    /// # Errors
    ///
    /// The broker's refusal to say the address, or a descriptor that addresses none.
    fn address(
        source: &Source,
        connected: &Connected<B>,
    ) -> impl Future<Output = Result<KeptName, BoxError>>;
}

impl<B, Source> SlotAddress<B, Source> for AddressedCopies
where
    B: Broker,
    Source: RedeliveryAddressed<Connected<B>>,
{
    async fn address(source: &Source, connected: &Connected<B>) -> Result<KeptName, BoxError> {
        let address = source
            .redelivery_address(connected)
            .await
            .map_err(|err| startup_failure("redelivery address", source.name(), err))?;
        Ok(kept_address(address))
    }
}

// The chain admits a registration on such a descriptor only with a destination named, so this is
// reached by none; it answers the way the multi-thread runner's startup refusal does.
impl<B, Source> SlotAddress<B, Source> for NamedCopies
where
    B: Broker,
    Source: SubscriptionSource<Connected<B>>,
{
    fn address(
        source: &Source,
        _connected: &Connected<B>,
    ) -> impl Future<Output = Result<KeptName, BoxError>> {
        ready(Err(startup_failure(
            "retry destination",
            source.name(),
            RetryDestinationError {
                subscription: diagnostic_name(source.name()),
                source_type: type_name::<Source>(),
            },
        )))
    }
}

#[cfg(test)]
#[cfg(ruststream_alloc)]
mod tests {
    use crate::Settle;
    use alloc::borrow::ToOwned;
    use alloc::string::String;
    use alloc::sync::Arc;
    use alloc::vec::Vec;
    use core::future::ready;
    use ruststream_core::PublishOutcome;
    use std::io;
    use std::sync::Mutex;

    use ruststream_core::{PayloadBuf, Places};

    use super::{AddressedCopy, CopyPublisher, PrepareCopy};
    use crate::dispatch::RETRY_COUNT_HEADER;
    use crate::host::KeptDelivery;
    use crate::publish::{
        ForReply, PublishContext, PublishTransform, PublishTransformIdentity, PublishTransformStack,
    };
    use crate::{AckError, HeaderSet, IncomingMessage, Lend, Outgoing, OutgoingMessage, Publisher};
    use crate::{Reads, Str};

    /// The broker's per-message setting the transform writes.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct Priority(u8);

    /// What one publish handed the broker.
    type Sent = (String, Vec<u8>, HeaderSet, Option<Priority>);

    /// A publisher with a constant header of its own, recording what it is handed.
    #[derive(Clone)]
    struct Recording {
        base: HeaderSet,
        sent: Arc<Mutex<Vec<Sent>>>,
    }

    impl Publisher for Recording {
        type Payload = Lend;
        type Error = io::Error;
        type Options = Priority;

        fn publish<'msg>(
            &self,
            msg: OutgoingMessage<'msg, &'msg [u8]>,
            options: Option<&Priority>,
        ) -> impl Future<Output = PublishOutcome<'msg, io::Error, Lend>> {
            let (name, payload, headers) = msg.into_parts();
            self.sent.lock().expect("one test thread").push((
                name.to_owned(),
                payload.to_vec(),
                headers,
                options.copied(),
            ));
            ready(Ok(()))
        }

        fn base_headers(&self) -> Option<&HeaderSet> {
            Some(&self.base)
        }
    }

    /// The delivery being retried.
    struct Job {
        payload: Vec<u8>,
        headers: HeaderSet,
    }

    impl IncomingMessage for Job {
        fn payload(&self) -> &[u8] {
            &self.payload
        }

        fn headers(&self) -> &HeaderSet {
            &self.headers
        }
    }

    impl Settle for Job {
        fn ack(self) -> impl Future<Output = Result<(), AckError>> {
            ready(Ok(()))
        }

        fn nack(self, _requeue: bool) -> impl Future<Output = Result<(), AckError>> {
            ready(Ok(()))
        }
    }

    /// The broker context of a delivery: what the transform's kind is written for.
    struct JobContext;

    /// Reads the delivery it retries, and raises the copy's priority.
    #[derive(Clone)]
    struct Escalate;

    impl PublishTransform<ForReply<JobContext>, Priority> for Escalate {
        type Destination = Reads;

        fn apply(
            &self,
            out: &mut Outgoing<'_>,
            options: &mut Option<Priority>,
            cx: &PublishContext<'_, JobContext>,
        ) {
            let seen = cx.headers().get_str(RETRY_COUNT_HEADER).unwrap_or("none");
            let from = alloc::format!("{} after {seen}", cx.name());
            out.headers_mut().insert("escalated", from);
            *options = Some(Priority(9));
        }
    }

    #[tokio::test]
    async fn a_kept_copy_leaves_as_its_transforms_left_it() {
        let mut base = HeaderSet::new();
        base.insert("origin", "board");
        base.insert("tier", "base");
        let copy = AddressedCopy {
            live: Recording {
                base,
                sent: Arc::new(Mutex::new(Vec::new())),
            },
            address: "jobs".to_owned(),
            stack: PublishTransformStack {
                inner: PublishTransformIdentity,
                outer: Escalate,
            },
        };
        let mut delivered = HeaderSet::new();
        delivered.insert(Str::from_static(RETRY_COUNT_HEADER), "1");
        delivered.insert("tier", "delivery");
        let job = Job {
            payload: b"job".to_vec(),
            headers: delivered.clone(),
        };
        let mut headers = delivered;
        headers.insert(Str::from_static(RETRY_COUNT_HEADER), "2");
        let mut payload = PayloadBuf::from(&b"job"[..]);
        let mut options = None;

        copy.prepare(
            &mut payload,
            &mut headers,
            &mut options,
            "jobs.later",
            &KeptDelivery {
                subscription: "jobs",
                message: &job,
                build_cx: |_: &Job| JobContext,
            },
        );
        copy.publish_copy(
            "jobs.later",
            &payload,
            headers,
            options.as_ref(),
            Places::none(),
        )
        .await
        .expect("the broker takes the copy");

        let sent = copy.live.sent.lock().expect("one test thread").clone();
        let [(name, payload, headers, options)] = sent.as_slice() else {
            panic!("one publish, got {sent:?}");
        };
        assert_eq!(
            (name.as_str(), payload.as_slice()),
            ("jobs.later", &b"job"[..])
        );
        assert_eq!(
            headers.get_str("origin"),
            Some("board"),
            "the publisher's constant"
        );
        assert_eq!(
            headers.get_str("tier"),
            Some("delivery"),
            "the delivery's header over it"
        );
        assert_eq!(
            headers.get_str(RETRY_COUNT_HEADER),
            Some("2"),
            "the copy's retry count"
        );
        assert_eq!(
            headers.get_str("escalated"),
            Some("jobs after 1"),
            "the transform read the delivery as it arrived"
        );
        assert_eq!(
            *options,
            Some(Priority(9)),
            "the setting the transform wrote"
        );
    }
}

#[cfg(test)]
mod address_tests {
    use ruststream_core::RedeliveryAddress;

    use super::{kept_address, kept_str};

    /// A subject written as one literal, past the room a built name has.
    const LITERAL: &str = "plant/line-line-line-line-line-line-line-line-line-line-line-line-line-line-line-line-line-line-sensors/temperature";

    #[test]
    fn an_address_past_the_room_is_kept_whole() {
        let kept = kept_address(RedeliveryAddress::new(LITERAL));
        assert_eq!(kept_str(&kept), LITERAL);
    }
}
