//! The copy path of a registration whose runner keeps its retry copies in place: the broker's
//! default publisher and the subscription's redelivery address, paired once when the
//! registration opens, typed in the route so a copy is published with no dynamic call.

// Written over the local twins of the contract for a runner that polls on one thread.
#![allow(clippy::future_not_send)]

use alloc::borrow::ToOwned;
use alloc::boxed::Box;
use alloc::string::String;
use core::future::ready;

use crate::lifecycle::BoxError;
use crate::{
    AddressedCopies, HeaderMap, LocalBroker, LocalConnected, LocalDefaultPublish,
    LocalPublishPolicy, LocalPublisher, LocalRedeliveryAddressed, LocalSubscriptionSource,
    OutgoingMessage, PayloadForm,
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
