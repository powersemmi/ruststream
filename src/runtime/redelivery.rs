//! Where a deferred `retry_after` copy is published, resolved once per registration at startup.
//!
//! A broker without native delayed redelivery gets the delay honoured by a copy the runtime
//! publishes after it. The copy needs a destination, and a subscription's own name is not one:
//! where a subscription and a publish destination are separate resources (a Pub/Sub subscription
//! and its topic), a copy published under the subscription's name reaches nothing. So the address
//! comes from the subscription's [`SubscriptionSource`], and a registration that bound the
//! deferred-retry position over a source that cannot report one refuses to start - on its own,
//! leaving the other registrations of the scope untouched.

use std::any::type_name;
use std::fmt;
use std::sync::Arc;

use thiserror::Error;
use tokio_util::task::TaskTracker;

use crate::runtime::dispatch::Delivery;
use crate::runtime::handle::Slot;
use crate::runtime::lifecycle::{BoxError, BoxFuture};
use crate::runtime::publish::OutPipeline;
use crate::runtime::publisher_registry::ErasedPublisher;
use crate::runtime::retry::Retry;
use crate::{Broker, Connected, PairError, PublishPolicy, Publisher, SubscriptionSource};

#[cfg(feature = "testing")]
use crate::testing::coordinator::TestHooks;

/// The deferred `retry_after` fallback of one subscription: the retry slot its registration
/// bound, paired with the address that subscription's source reported at startup.
pub(crate) struct DeferredRetry {
    /// The bound slot's entry, erased. A publish through it travels the slot's own transforms and
    /// the app's publish pipeline, as a publish through any slot does.
    pub(crate) publisher: Arc<dyn ErasedPublisher>,
    pub(crate) address: Arc<str>,
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
        Pipe: OutPipeline + 'static,
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

/// A registration bound the deferred-retry position over a subscription that cannot say where a
/// redelivery of it is published.
///
/// Raised at startup, before that subscription opens, and only for the registration that bound
/// the position. The alternative is a `retry_after` that publishes its copy into nothing under
/// load, which is a lost message.
#[derive(Debug, Error)]
pub(crate) enum RetryAddressError {
    /// The subscription has a source, and the source reports no address.
    // The field is named away from `source` so `thiserror` does not read it as the error's cause.
    #[error(
        "subscription `{subscription}`: the registration binds the deferred-retry position, but \
         source `{source_type}` reports no redelivery address on broker `{broker}`. Mount it on a \
         source that reports one, or drop `out_retry` from this registration and let `retry_after` \
         requeue immediately"
    )]
    Unaddressed {
        /// The subscription as the registration names it.
        subscription: String,
        /// The subscription source that declined to answer.
        source_type: &'static str,
        /// The connected broker the source was asked against.
        broker: &'static str,
    },
    /// The subscription was mounted from an already-open subscriber, so there is no source to ask.
    #[error(
        "subscription `{subscription}`: the registration binds the deferred-retry position, but \
         the subscriber was mounted directly, so nothing reports a redelivery address for it. \
         Mount it on a subscription source, or drop `out_retry` from this registration and let \
         `retry_after` requeue immediately"
    )]
    Sourceless {
        /// The subscription as the registration names it.
        subscription: String,
    },
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
/// The retry policy is paired and the redelivery address resolved first, against the live
/// connection, because both may have to talk to the broker (a Pub/Sub subscription is looked up
/// to learn its topic) and because a registration that cannot answer must fail before it holds an
/// open subscription. A registration with no deferred-retry position asks nothing: the address
/// would have no use.
///
/// # Errors
///
/// Returns the broker's error when the address lookup or the subscription fails, [`RetryPairError`]
/// when the bound retry policy fails to pair, and [`RetryAddressError`] when the registration
/// defers retries over a source that reports no address.
pub(crate) async fn open_subscription<B, Source>(
    source: Source,
    connected: &Connected<B>,
    scope: &ScopeDelivery,
    subscription: &str,
    retry: Option<RetryPairing<B>>,
) -> Result<(Source::Subscriber, Arc<Delivery>), BoxError>
where
    B: Broker + 'static,
    Source: SubscriptionSource<Connected<B>>,
{
    // The publisher and the address are read into the pair together, so a fallback that holds one
    // without the other is never built.
    let retry = match retry {
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
                .ok_or_else(|| RetryAddressError::Unaddressed {
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
    let subscriber = source
        .subscribe(connected)
        .await
        .map_err(|err| Box::new(err) as BoxError)?;
    Ok((
        subscriber,
        Arc::new(Delivery::for_subscription(scope, retry)),
    ))
}

/// The delivery context for a subscriber mounted without a source, which is the one mount that
/// cannot report a redelivery address.
///
/// # Errors
///
/// Returns [`RetryAddressError::Sourceless`] when the registration binds the deferred-retry
/// position.
// The pairing travels by value like it does to every other mount: this one cannot address it, so
// this is where it is dropped. A borrow would leave the caller holding a pairing with no use.
#[allow(clippy::needless_pass_by_value)]
pub(crate) fn open_mounted_subscriber<B: Broker>(
    scope: &ScopeDelivery,
    subscription: &str,
    retry: Option<RetryPairing<B>>,
) -> Result<Arc<Delivery>, BoxError> {
    if retry.is_some() {
        return Err(Box::new(RetryAddressError::Sourceless {
            subscription: subscription.to_owned(),
        }));
    }
    Ok(Arc::new(Delivery::for_subscription(scope, None)))
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

    /// A subscriber mounted without a source has nothing to ask, so a registration that defers
    /// retries has no address for it. The startup error names the subscription and the way out,
    /// rather than letting the subscription run and publish its deferred copies into nothing.
    #[test]
    fn a_sourceless_mount_under_a_retry_position_is_a_startup_error() {
        let refused = open_mounted_subscriber(&scope(), "orders", Some(bare_slot()))
            .expect_err("a registration that defers retries cannot address a sourceless mount");
        let message = refused.to_string();
        assert!(message.contains("orders"), "{message}");
        assert!(message.contains("out_retry"), "{message}");
    }

    /// Without the position there is nothing to address, so the same mount starts.
    #[test]
    fn a_sourceless_mount_starts_when_the_registration_defers_nothing() {
        let delivery = open_mounted_subscriber::<MemoryBroker>(&scope(), "orders", None)
            .expect("a registration with no retry position asks for no address");
        assert!(delivery.retry.is_none());
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
