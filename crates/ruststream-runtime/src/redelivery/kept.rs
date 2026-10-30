//! The copy path of a registration whose runner keeps its retry copies in place: the broker's
//! default publisher and the subscription's redelivery address, paired once when the
//! registration opens, typed in the route so a copy is published with no dynamic call.

// Written over the local twins of the contract for a runner that polls on one thread.
#![allow(clippy::future_not_send)]

use alloc::borrow::{Cow, ToOwned};
use alloc::boxed::Box;
use alloc::string::String;
use core::any::type_name;
use core::future::ready;
use core::marker::PhantomData;

use super::RetryDestinationError;
use crate::lifecycle::BoxError;
use crate::{
    AddressedCopies, BrokerMoves, HeaderMap, LocalBroker, LocalConnected, LocalDefaultPublish,
    LocalPublishPolicy, LocalPublisher, LocalRedeliveryAddressed, LocalSubscriptionSource,
    NamedCopies, OutgoingMessage, PayloadForm,
};

/// Where a kept copy leaves: the publisher and the address that reaches the subscription
/// again. Machinery; never named directly.
#[doc(hidden)]
pub trait CopyPublisher {
    /// The address a copy back to the subscription is published under.
    fn address(&self) -> &str;

    /// Publishes one copy to `destination`.
    ///
    /// # Errors
    ///
    /// What the publisher returned.
    fn publish_copy(
        &self,
        destination: &str,
        payload: &[u8],
        headers: HeaderMap,
    ) -> impl Future<Output = Result<(), BoxError>>;
}

/// A copy path paired from the broker's default publish policy, addressed where the
/// subscription's descriptor said. Machinery; never named directly.
#[doc(hidden)]
#[derive(Debug)]
pub struct AddressedCopy<Live> {
    live: Live,
    address: String,
}

impl<Live: LocalPublisher> CopyPublisher for AddressedCopy<Live> {
    fn address(&self) -> &str {
        &self.address
    }

    async fn publish_copy(
        &self,
        destination: &str,
        payload: &[u8],
        delivered: HeaderMap,
    ) -> Result<(), BoxError> {
        // The publisher's own constants sit under the delivery's headers, as on every publish
        // through it.
        let headers = if let Some(base) = self.live.base_headers() {
            let mut headers = base.clone();
            for (key, value) in delivered.iter() {
                headers.insert(key.to_owned(), value.to_owned());
            }
            headers
        } else {
            delivered
        };
        // A transport that reads the bytes is lent the record's buffer; one that keeps them is
        // handed a copy of its own.
        let form = <Live::Payload as PayloadForm>::Form::from(payload);
        let msg = OutgoingMessage::assembled(destination, form, headers);
        self.live.publish(msg, None).await.map_err(BoxError::from)
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
    fn address(&self) -> &str {
        match *self {}
    }

    async fn publish_copy(
        &self,
        _destination: &str,
        _payload: &[u8],
        _headers: HeaderMap,
    ) -> Result<(), BoxError> {
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
pub trait PairCopies<B: LocalBroker, Source> {
    /// The copy path the registration opens with.
    type Path: CopyPublisher;

    /// Pairs the copy path against the connected broker; `None` where the plan keeps no copy.
    ///
    /// # Errors
    ///
    /// The broker's refusal to pair the publisher or to say the address.
    fn pair(
        source: &Source,
        connected: &LocalConnected<B>,
    ) -> impl Future<Output = Result<Option<Self::Path>, BoxError>>;
}

impl<B: LocalBroker, Source> PairCopies<B, Source> for NoCopies {
    type Path = NoCopyPath;

    fn pair(
        _source: &Source,
        _connected: &LocalConnected<B>,
    ) -> impl Future<Output = Result<Option<NoCopyPath>, BoxError>> {
        ready(Ok(None))
    }
}

impl<B, Source> PairCopies<B, Source> for KeepCopies
where
    B: LocalBroker,
    Source: LocalSubscriptionSource<LocalConnected<B>>,
    Source::Copies: KeptAddress<B, Source>,
{
    type Path = <Source::Copies as KeptAddress<B, Source>>::Path;

    async fn pair(
        source: &Source,
        connected: &LocalConnected<B>,
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
            implements `LocalRedeliveryAddressed`",
    note = "a descriptor whose deliveries the broker moves itself (`BrokerMoves`) or that reads \
            many destinations (`NamedCopies`) keeps no copy in place: leave out \
            `.retry_capacity`, and `retry_after` requeues"
)]
pub trait KeptAddress<B: LocalBroker, Source> {
    /// The copy path paired for it.
    type Path: CopyPublisher;

    /// Pairs the broker's default publisher and asks the descriptor for the address.
    ///
    /// # Errors
    ///
    /// The broker's refusal to pair the publisher or to say the address.
    fn pair(
        source: &Source,
        connected: &LocalConnected<B>,
    ) -> impl Future<Output = Result<Self::Path, BoxError>>;
}

type DefaultLive<B> = <<LocalConnected<B> as LocalDefaultPublish>::Policy as LocalPublishPolicy<
    LocalConnected<B>,
>>::Live;

impl<B, Source> KeptAddress<B, Source> for AddressedCopies
where
    B: LocalBroker,
    Source: LocalRedeliveryAddressed<LocalConnected<B>>,
    LocalConnected<B>: LocalDefaultPublish,
    DefaultLive<B>: LocalPublisher,
{
    type Path = AddressedCopy<DefaultLive<B>>;

    async fn pair(source: &Source, connected: &LocalConnected<B>) -> Result<Self::Path, BoxError> {
        let address = source
            .redelivery_address(connected)
            .await
            .map_err(|err| -> BoxError { Box::new(err) })?;
        let live = <LocalConnected<B> as LocalDefaultPublish>::Policy::default()
            .pair(connected)
            .await
            .map_err(BoxError::from)?;
        Ok(AddressedCopy {
            live,
            address: address.as_str().to_owned(),
        })
    }
}

/// The deferred-retry position a local mount chain bound with `.out_retry(policy)`: the policy a
/// kept copy leaves through, and the destination a `.to(name)` after it gave the copies.
/// Machinery; never named directly.
#[doc(hidden)]
#[derive(Debug)]
pub struct LocalRetrySlot<Policy> {
    pub(crate) policy: Policy,
    pub(crate) destination: Option<Cow<'static, str>>,
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
pub trait PairSlot<B: LocalBroker, Source, Policy> {
    /// The copy path the registration opens with.
    type Path: CopyPublisher;

    /// Pairs the slot's policy against the connected broker, addressed where the slot or the
    /// descriptor says.
    ///
    /// # Errors
    ///
    /// The broker's refusal to pair the policy or to say the address.
    fn pair_slot(
        slot: LocalRetrySlot<Policy>,
        source: &Source,
        connected: &LocalConnected<B>,
    ) -> impl Future<Output = Result<Option<Self::Path>, BoxError>>;
}

impl<B, Source, Policy> PairSlot<B, Source, Policy> for KeepCopies
where
    B: LocalBroker,
    Source: LocalSubscriptionSource<LocalConnected<B>>,
    Source::Copies: SlotAddress<B, Source>,
    Policy: LocalPublishPolicy<LocalConnected<B>>,
    Policy::Live: LocalPublisher,
{
    type Path = AddressedCopy<Policy::Live>;

    async fn pair_slot(
        slot: LocalRetrySlot<Policy>,
        source: &Source,
        connected: &LocalConnected<B>,
    ) -> Result<Option<Self::Path>, BoxError> {
        // A destination the mount site named is where the copies go; the descriptor is asked
        // only otherwise, as on the multi-thread runner.
        let address = match slot.destination {
            Some(named) => named.into_owned(),
            None => <Source::Copies as SlotAddress<B, Source>>::address(source, connected).await?,
        };
        let live = slot.policy.pair(connected).await.map_err(BoxError::from)?;
        Ok(Some(AddressedCopy { live, address }))
    }
}

/// Where a copy goes back to the subscription when the mount site named no destination.
/// Machinery; never named directly.
#[doc(hidden)]
pub trait SlotAddress<B: LocalBroker, Source> {
    /// The descriptor's own address for its copies.
    ///
    /// # Errors
    ///
    /// The broker's refusal to say the address, or a descriptor that addresses none.
    fn address(
        source: &Source,
        connected: &LocalConnected<B>,
    ) -> impl Future<Output = Result<String, BoxError>>;
}

impl<B, Source> SlotAddress<B, Source> for AddressedCopies
where
    B: LocalBroker,
    Source: LocalRedeliveryAddressed<LocalConnected<B>>,
{
    async fn address(source: &Source, connected: &LocalConnected<B>) -> Result<String, BoxError> {
        let address = source
            .redelivery_address(connected)
            .await
            .map_err(|err| -> BoxError { Box::new(err) })?;
        Ok(address.as_str().to_owned())
    }
}

// The chain admits a registration on such a descriptor only with a destination named, so this is
// reached by none; it answers the way the multi-thread runner's startup refusal does.
impl<B, Source> SlotAddress<B, Source> for NamedCopies
where
    B: LocalBroker,
    Source: LocalSubscriptionSource<LocalConnected<B>>,
{
    fn address(
        source: &Source,
        _connected: &LocalConnected<B>,
    ) -> impl Future<Output = Result<String, BoxError>> {
        ready(Err(BoxError::from(RetryDestinationError {
            subscription: source.name().to_owned(),
            source_type: type_name::<Source>(),
        })))
    }
}
