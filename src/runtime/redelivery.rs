//! What one registration owes its retries at startup: the publisher a copy of a delivery leaves
//! through, the address such a copy reaches the subscription at, and the declaration the mount
//! site made.
//!
//! A copy needs a destination, and a subscription's own name is not one: where a subscription and
//! a publish destination are separate resources (a Pub/Sub subscription and its topic), a copy
//! published under the subscription's name reaches nothing. So the address comes from the
//! subscription's [`SubscriptionSource`], and a registration whose descriptor says the runtime
//! publishes its copies ([`RuntimeCopies`]) but reports no address refuses to start - on its own,
//! leaving the other registrations of the scope untouched.

use std::any::type_name;
use std::fmt;
use std::sync::Arc;

use thiserror::Error;
use tokio_util::task::TaskTracker;
use tracing::info;

use crate::runtime::dispatch::Delivery;
use crate::runtime::handle::Slot;
use crate::runtime::lifecycle::{BoxError, BoxFuture};
use crate::runtime::publish::OutPipeline;
use crate::runtime::publisher_registry::ErasedPublisher;
use crate::runtime::retry::Retry;
use crate::{
    Broker, BrokerMoves, Connected, DefaultPublish, PairError, PublishPolicy, Publisher,
    RetryDeclaration, RuntimeCopies, SubscriptionSource,
};

#[cfg(feature = "testing")]
use crate::testing::coordinator::TestHooks;

/// The plain publish policy of a broker's connected form, which is what a retry copy leaves
/// through until a mount site names another.
type DefaultPolicy<B> = <Connected<B> as DefaultPublish>::Policy;

/// The live publisher that policy pairs into.
type DefaultLive<B> = <DefaultPolicy<B> as PublishPolicy<Connected<B>>>::Live;

/// The retry path of one subscription: the publisher a copy of a delivery leaves through, and the
/// address a copy meant for the subscription itself is published to.
pub(crate) struct DeferredRetry {
    /// The registration's retry slot, erased. A publish through it travels the slot's own
    /// transforms and the app's publish pipeline, as a publish through any slot does.
    pub(crate) publisher: Arc<dyn ErasedPublisher>,
    pub(crate) address: Arc<str>,
}

/// Whether a mount site may name the publisher of this descriptor's retry copies.
///
/// Implemented by [`RuntimeCopies`] alone: where the broker moves the delivery itself there is no
/// publisher of this process's to customise. The descriptor rides the trait's parameter so the
/// compile error names it. Machinery; never named directly.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "subscription descriptor `{Source}` publishes no retry copy in this process",
    label = "`out_retry(..)` has no publisher to name on this registration",
    note = "`{Source}` declares `Copies = BrokerMoves`: the broker or its client library moves \
            the delivery itself (a delivery limit with a dead-letter exchange, a subscription's \
            dead-letter policy, a redrive policy), so nothing of this process's is published for \
            it and a publish policy has nothing to shape",
    note = "declare the cap and the destination instead - `.max_attempts(n).dead_letter(\"dlq\")` \
            - and that descriptor maps them onto the broker's own mechanism"
)]
pub trait PublishesCopiesHere<Source> {}

impl<Source> PublishesCopiesHere<Source> for RuntimeCopies {}

/// The retry publisher a descriptor's copy path owes every registration mounted on it.
///
/// [`RuntimeCopies`] owes one, paired from the broker's [`DefaultPublish`] policy the way the
/// default reply publisher is, so a `retry_after` behaves the same whether or not the mount site
/// named a policy; [`BrokerMoves`] owes none. Machinery; never named directly.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "broker `{B}` names no default publish policy, so a retry copy has nothing to \
               leave through",
    label = "this subscription's retry copies are published by the runtime",
    note = "the descriptor declares `Copies = RuntimeCopies`, so every registration on it gets a \
            retry publisher paired from `DefaultPublish`; implement `DefaultPublish` on the \
            broker's connected form, or declare `Copies = BrokerMoves` on a descriptor whose \
            deliveries the broker moves itself"
)]
pub trait CopyPathPairing<B: Broker, Pipeline> {
    /// The pairing, or `None` where this process publishes nothing for the subscription.
    fn pairing(pipeline: Pipeline) -> Option<RetryPairing<B>>;
}

impl<B, Pipeline> CopyPathPairing<B, Pipeline> for RuntimeCopies
where
    B: Broker + 'static,
    Connected<B>: DefaultPublish,
    DefaultLive<B>: Publisher + 'static,
    Pipeline: OutPipeline<DefaultLive<B>> + 'static,
{
    fn pairing(pipeline: Pipeline) -> Option<RetryPairing<B>> {
        // The copy carries the delivery's own bytes, so the codec position stays empty: the unit
        // codec is what the wire resolves for a slot that encodes nothing.
        Some(RetryPairing::new(
            DefaultPolicy::<B>::default(),
            (),
            pipeline,
        ))
    }
}

impl<B: Broker, Pipeline> CopyPathPairing<B, Pipeline> for BrokerMoves {
    fn pairing(_pipeline: Pipeline) -> Option<RetryPairing<B>> {
        None
    }
}

/// What one registration hands the runtime about its retries: the publisher a copy leaves
/// through, and what the mount site declared.
///
/// The publisher starts empty and is filled from one of two places - the policy a
/// `.out_retry(policy)` named, or the descriptor's copy path, which pairs the broker's default -
/// so a registration whose subscription publishes its copies here always has one.
#[doc(hidden)]
pub struct RetrySetup<B: Broker> {
    publisher: Option<RetryPairing<B>>,
    declaration: RetryDeclaration,
}

// The publisher is a closure with nothing to print; the declaration is the part worth reading.
impl<B: Broker> fmt::Debug for RetrySetup<B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RetrySetup")
            .field("declaration", &self.declaration)
            .finish_non_exhaustive()
    }
}

impl<B: Broker> Default for RetrySetup<B> {
    fn default() -> Self {
        Self {
            publisher: None,
            declaration: RetryDeclaration::new(),
        }
    }
}

impl<B: Broker + 'static> RetrySetup<B> {
    /// Records the publisher a `.out(Retry, policy)` named, replacing the broker's default.
    pub(crate) fn with_publisher(mut self, publisher: RetryPairing<B>) -> Self {
        self.publisher = Some(publisher);
        self
    }

    /// Records what `max_attempts(..)` and `dead_letter(..)` declared.
    pub(crate) fn with_declaration(mut self, declaration: RetryDeclaration) -> Self {
        self.declaration = declaration;
        self
    }

    /// Fills in the broker's default publisher where the mount site named none, per the
    /// descriptor's copy path.
    #[must_use]
    pub(crate) fn resolve<Copies, Pipeline>(mut self, pipeline: &Pipeline) -> Self
    where
        Copies: CopyPathPairing<B, Pipeline>,
        Pipeline: Clone,
    {
        if self.publisher.is_none() {
            self.publisher = Copies::pairing(pipeline.clone());
        }
        self
    }
}

/// The pairing one registration's `.out(Retry, policy)` owes: the retry slot, erased against the
/// broker the mount site named, waiting for the connected form.
///
/// A policy is declaration and pairs only against a connected broker, which exists after the
/// synchronous builder has run. Erasing the whole slot here - the policy, the codec and the
/// pipeline the mount composed - is what keeps the retry position out of the route's type: a
/// route carries this one type whatever the mount site named.
#[doc(hidden)]
pub struct RetryPairing<B: Broker>(PairRetry<B>);

// The pairing is a closure over the bound policy, with nothing of its own to print.
impl<B: Broker> fmt::Debug for RetryPairing<B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RetryPairing").finish_non_exhaustive()
    }
}

/// The erased pairing step: pairs the bound policy against the connected broker.
type PairRetry<B> = Box<
    dyn for<'a> FnOnce(
            &'a Connected<B>,
        ) -> BoxFuture<'a, Result<Arc<dyn ErasedPublisher>, PairError>>
        + Send,
>;

impl<B: Broker + 'static> RetryPairing<B> {
    /// The pairing the retry slot owes against this broker's connected form.
    ///
    /// What the pairing yields is the slot entry itself, erased: a publish through it runs the
    /// mount site's transforms and the app's publish pipeline before reaching the broker, which
    /// is what makes the deferred copy travel the slot it was bound on rather than a path of its
    /// own.
    pub(crate) fn new<Policy, Enc, Pipe>(policy: Policy, codec: Enc, pipeline: Pipe) -> Self
    where
        Policy: PublishPolicy<Connected<B>> + Send + 'static,
        Policy::Live: Publisher + 'static,
        Enc: Send + Sync + 'static,
        Pipe: OutPipeline<Policy::Live> + 'static,
    {
        Self(Box::new(move |connected| {
            Box::pin(async move {
                let live = policy.pair(connected).await?;
                Ok(
                    Arc::new(Slot::<Retry, _, _, _>::wired(live, codec, pipeline))
                        as Arc<dyn ErasedPublisher>,
                )
            })
        }))
    }

    /// Takes the pairing, producing the publisher the deferred copy leaves through.
    ///
    /// # Errors
    ///
    /// Returns the policy's own [`PairError`].
    async fn pair(self, connected: &Connected<B>) -> Result<Arc<dyn ErasedPublisher>, PairError> {
        (self.0)(connected).await
    }
}

/// What a broker scope hands every subscription it starts.
///
/// The deferred retry is a registration's own, so it is not here: this is what the whole scope
/// shares with each of its subscriptions.
pub(crate) struct ScopeDelivery {
    /// App-wide tracker for post-settle continuations, so a graceful shutdown drains them.
    tasks: TaskTracker,
    /// The harness's recording-and-quiescence hooks for this scope.
    #[cfg(feature = "testing")]
    hooks: Arc<TestHooks>,
    /// This broker's registration index, scoping recorded deliveries per broker.
    #[cfg(feature = "testing")]
    scope_id: usize,
}

impl ScopeDelivery {
    /// The scope context every subscription of one broker dispatches under.
    pub(crate) fn new(
        tasks: TaskTracker,
        #[cfg(feature = "testing")] hooks: Arc<TestHooks>,
        #[cfg(feature = "testing")] scope_id: usize,
    ) -> Self {
        Self {
            tasks,
            #[cfg(feature = "testing")]
            hooks,
            #[cfg(feature = "testing")]
            scope_id,
        }
    }

    /// The tracker post-settle continuations are spawned onto.
    pub(crate) fn tasks(&self) -> &TaskTracker {
        &self.tasks
    }

    /// The harness hooks this scope dispatches under.
    #[cfg(feature = "testing")]
    pub(crate) fn hooks(&self) -> &Arc<TestHooks> {
        &self.hooks
    }

    /// This broker's registration index.
    #[cfg(feature = "testing")]
    pub(crate) fn scope_id(&self) -> usize {
        self.scope_id
    }
}

/// A registration whose subscription publishes its retry copies in this process, over a
/// descriptor that cannot say where such a copy reaches it.
///
/// Raised at startup, before that subscription opens, and only for that registration. The
/// alternative is a `retry_after` that publishes its copy into nothing under load, which is a
/// lost message.
// The field is named away from `source` so `thiserror` does not read it as the error's cause.
#[derive(Debug, Error)]
#[error(
    "subscription `{subscription}`: descriptor `{source_type}` declares `Copies = RuntimeCopies`, \
     so the runtime publishes this subscription's retry copies, but it reports no redelivery \
     address on broker `{broker}`. Implement `SubscriptionSource::redelivery_address` (or \
     `Subscribe::redelivery_address` for the by-name form) with the destination a publish reaches \
     this subscription at, or declare `Copies = BrokerMoves` where the broker moves the delivery \
     itself"
)]
pub(crate) struct RetryAddressError {
    /// The subscription as the registration names it.
    subscription: String,
    /// The subscription descriptor that declined to answer.
    source_type: &'static str,
    /// The connected broker the descriptor was asked against.
    broker: &'static str,
}

/// The policy bound with `.out(Retry, policy)` could not be paired with the connected broker.
///
/// Raised at startup, before the subscription opens, the way a reply policy that fails to pair is.
#[derive(Debug, Error)]
#[error(
    "subscription `{subscription}`: the deferred-retry policy bound with `out_retry` failed to \
     pair: {source}"
)]
pub(crate) struct RetryPairError {
    /// The subscription as the registration names it.
    subscription: String,
    #[source]
    source: PairError,
}

/// Opens `source`'s subscription and builds the delivery context it dispatches under.
///
/// The retry publisher is paired and the redelivery address resolved first, against the live
/// connection, because both may have to talk to the broker (a Pub/Sub subscription is looked up
/// to learn its topic) and because a registration that cannot answer must fail before it holds an
/// open subscription. The declaration reaches the descriptor before it subscribes, so a broker
/// that applies the cap and the destination itself declares its topology with them.
///
/// # Errors
///
/// Returns the broker's error when the address lookup or the subscription fails, [`RetryPairError`]
/// when the bound retry policy fails to pair, and [`RetryAddressError`] when the descriptor
/// publishes its copies here and reports no address.
pub(crate) async fn open_subscription<B, Source>(
    source: Source,
    connected: &Connected<B>,
    scope: &ScopeDelivery,
    subscription: &str,
    setup: RetrySetup<B>,
) -> Result<(Source::Subscriber, Arc<Delivery>), BoxError>
where
    B: Broker + 'static,
    Source: SubscriptionSource<Connected<B>>,
{
    let RetrySetup {
        publisher,
        declaration,
    } = setup;
    // The publisher and the address are read into the pair together, so a retry path that holds
    // one without the other is never built.
    let retry = match publisher {
        Some(pairing) => {
            let publisher = pairing.pair(connected).await.map_err(|err| {
                Box::new(RetryPairError {
                    subscription: subscription.to_owned(),
                    source: err,
                }) as BoxError
            })?;
            let reported = source
                .redelivery_address(connected)
                .await
                .map_err(|err| Box::new(err) as BoxError)?
                .ok_or_else(|| RetryAddressError {
                    subscription: subscription.to_owned(),
                    source_type: type_name::<Source>(),
                    broker: type_name::<Connected<B>>(),
                })?;
            Some(DeferredRetry {
                publisher,
                address: Arc::from(reported.as_str()),
            })
        }
        None => None,
    };
    announce(subscription, &declaration, retry.as_ref());
    let subscriber = source
        .declare_retry(&declaration)
        .subscribe(connected)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;
    Ok((
        subscriber,
        Arc::new(Delivery::for_subscription(scope, retry, declaration)),
    ))
}

/// Says once, at startup, what a registration declared and who applies it, so an operator reading
/// the log knows whether the cap is this process's business or the broker's.
fn announce(subscription: &str, declaration: &RetryDeclaration, retry: Option<&DeferredRetry>) {
    if declaration.declares_nothing() {
        return;
    }
    info!(
        target: "ruststream::retry",
        subscription = %subscription,
        max_attempts = declaration.max_attempts().map(std::num::NonZeroU32::get),
        dead_letter = declaration.dead_letter(),
        applied_by = if retry.is_some() { "runtime" } else { "broker" },
        "retry declaration",
    );
}

/// The delivery context for a subscriber mounted without a source: nothing describes where a
/// redelivery of it would be published, and no mount chain can bind the retry position on one, so
/// there is no retry path to build.
pub(crate) fn open_mounted_subscriber(scope: &ScopeDelivery) -> Arc<Delivery> {
    Arc::new(Delivery::for_subscription(
        scope,
        None,
        RetryDeclaration::new(),
    ))
}

#[cfg(all(test, feature = "memory"))]
mod tests {
    use std::pin::pin;

    use futures::StreamExt as _;

    use super::*;
    use crate::memory::{MemoryBroker, MemoryPublish};
    use crate::runtime::publish::PublishIdentity;
    use crate::{IncomingMessage, OutgoingMessage, Subscriber};

    /// The retry slot of a mount that named neither a codec nor a transform: the surface's codec
    /// position and the app's bare pipeline, which is what the wire produces there.
    fn bare_slot() -> RetryPairing<MemoryBroker> {
        RetryPairing::<MemoryBroker>::new(MemoryPublish, (), PublishIdentity)
    }

    /// A scope with the harness pieces it holds under the `testing` feature.
    fn scope() -> ScopeDelivery {
        ScopeDelivery::new(
            TaskTracker::new(),
            #[cfg(feature = "testing")]
            Arc::new(TestHooks::detached()),
            #[cfg(feature = "testing")]
            0,
        )
    }

    /// A subscriber mounted without a source describes no subscription, so nothing reports where
    /// a copy of one of its deliveries would go: the mount carries no retry path at all, and the
    /// chain offers no way to bind one.
    #[test]
    fn a_sourceless_mount_carries_no_retry_path() {
        let delivery = open_mounted_subscriber(&scope());
        assert!(delivery.retry.is_none());
        assert!(delivery.declaration.declares_nothing());
    }

    /// The bound slot pairs against the connected broker, and the entry it yields reaches that
    /// broker: what the deferred copy travels through.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_bound_slot_pairs_against_the_connected_broker() {
        let broker = MemoryBroker::new();
        let mut subscriber = broker.subscribe("retry.fallback");
        let connected = broker.connect().await.expect("connect");

        let publisher = bare_slot()
            .pair(&connected)
            .await
            .expect("the bound slot pairs");
        publisher
            .publish_erased(OutgoingMessage::new("retry.fallback", b"deferred"))
            .await
            .expect("the erased publish failed");

        let mut stream = pin!(subscriber.stream());
        let msg = stream
            .next()
            .await
            .expect("the publish must reach the broker")
            .expect("delivery");
        assert_eq!(msg.payload(), b"deferred");
    }
}
