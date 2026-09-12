//! Where a deferred `retry_after` copy is published, resolved once per subscription at startup.
//!
//! A broker without native delayed redelivery gets the delay honoured by a copy the runtime
//! publishes after it. The copy needs a destination, and a subscription's own name is not one:
//! where a subscription and a publish destination are separate resources (a Pub/Sub subscription
//! and its topic), a copy published under the subscription's name reaches nothing. So the address
//! comes from the subscription's [`SubscriptionSource`], and a scope that wired a publisher with
//! [`retry_via`](crate::runtime::BrokerScope::retry_via) over a source that cannot report one
//! refuses to start.

use std::any::type_name;
use std::sync::{Arc, Mutex};

use thiserror::Error;
use tokio::sync::OnceCell;
use tokio_util::task::TaskTracker;

use crate::runtime::dispatch::Delivery;
use crate::runtime::lifecycle::{BoxError, BoxFuture, ConnectedSlot};
use crate::runtime::publish_source::pair_bound;
use crate::runtime::publisher_registry::ErasedPublisher;
use crate::{Broker, Connected, PairError, PublishPolicy, Publisher, SubscriptionSource};

#[cfg(feature = "testing")]
use crate::testing::coordinator::TestHooks;

/// The deferred `retry_after` fallback of one subscription: the publisher its scope wired,
/// paired with the address that subscription's source reported at startup.
pub(crate) struct DeferredRetry {
    pub(crate) publisher: Arc<dyn ErasedPublisher>,
    pub(crate) address: Arc<str>,
}

/// The pairing a scope's deferred-retry policy still owes, and the live publisher once it ran.
///
/// A policy is declaration and pairs only against the connected broker, which exists after the
/// synchronous builder has run; so the scope keeps the pairing as a step to take, and takes it
/// once, when its first subscription opens. Every subscription of the scope then shares the one
/// live publisher, erased like the reply publishers are.
pub(crate) struct RetryPairing {
    // The step is consumed by the first pairing; the mutex is the hand-over, not contention.
    pair: Mutex<Option<PairRetry>>,
    live: OnceCell<Arc<dyn ErasedPublisher>>,
}

/// The erased pairing step: reads the scope's connected broker and pairs the policy against it.
type PairRetry =
    Box<dyn FnOnce() -> BoxFuture<'static, Result<Arc<dyn ErasedPublisher>, PairError>> + Send>;

impl RetryPairing {
    /// The pairing of `policy` against the broker `slot` will hold once startup connected it.
    pub(crate) fn new<B, Policy>(slot: ConnectedSlot<B>, policy: Policy) -> Self
    where
        B: Broker + 'static,
        Policy: PublishPolicy<Connected<B>> + Send + 'static,
        Policy::Live: Publisher + 'static,
    {
        let pair: PairRetry = Box::new(move || {
            Box::pin(async move {
                let live = pair_bound::<B, Policy>(&slot, policy).await?;
                Ok(Arc::new(live) as Arc<dyn ErasedPublisher>)
            })
        });
        Self {
            pair: Mutex::new(Some(pair)),
            live: OnceCell::new(),
        }
    }

    /// The live deferred-retry publisher, paired on first use.
    ///
    /// # Errors
    ///
    /// Returns [`RetryPairError`] when the policy fails to pair. A failed pairing aborts startup,
    /// so a second call after one is not reached in practice; it reports the same failure again
    /// rather than pairing twice.
    pub(crate) async fn publisher(&self) -> Result<&Arc<dyn ErasedPublisher>, RetryPairError> {
        self.live
            .get_or_try_init(|| async {
                let pair = self
                    .pair
                    .lock()
                    .expect("deferred-retry pairing mutex poisoned")
                    .take()
                    .ok_or_else(|| {
                        RetryPairError(PairError::from_boxed(Box::from(
                            "the deferred-retry publisher failed to pair earlier in this startup",
                        )))
                    })?;
                pair().await.map_err(RetryPairError)
            })
            .await
    }
}

/// The policy wired with [`retry_via`](crate::runtime::BrokerScope::retry_via) could not be
/// paired with the connected broker.
///
/// Raised at startup, before the scope's first subscription opens, the way a reply policy that
/// fails to pair is.
#[derive(Debug, Error)]
#[error("the deferred-retry publisher wired with `retry_via` failed to pair: {0}")]
pub(crate) struct RetryPairError(#[source] PairError);

/// What a broker scope hands every subscription it starts.
///
/// The publisher is scope-wide, the address is not, so this is the half of a [`Delivery`] that is
/// known before a subscription resolves: pairing the two is what
/// [`open_subscription`] does.
pub(crate) struct ScopeDelivery {
    /// The policy wired with [`retry_via`](crate::runtime::BrokerScope::retry_via), as the
    /// pairing it owes, or `None` when the scope did not opt in - in which case a `retry_after`
    /// on a broker without native delayed redelivery degrades to an immediate requeue.
    retry: Option<RetryPairing>,
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
    /// The scope context, with `retry` as its deferred-retry fallback still to pair.
    pub(crate) fn new(
        retry: Option<RetryPairing>,
        tasks: TaskTracker,
        #[cfg(feature = "testing")] hooks: Arc<TestHooks>,
        #[cfg(feature = "testing")] scope_id: usize,
    ) -> Self {
        Self {
            retry,
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

    /// The deferred-retry pairing this scope wired, if any.
    fn retry(&self) -> Option<&RetryPairing> {
        self.retry.as_ref()
    }

    /// Whether this scope defers retries at all: the question a mount that cannot report an
    /// address asks, which needs no pairing to answer.
    fn defers_retries(&self) -> bool {
        self.retry.is_some()
    }
}

/// A scope wired a deferred-retry publisher over a subscription that cannot say where a
/// redelivery of it is published.
///
/// Raised at startup, before the subscription opens. The alternative is a `retry_after` that
/// publishes its copy into nothing under load, which is a lost message.
#[derive(Debug, Error)]
pub(crate) enum RetryAddressError {
    /// The subscription has a source, and the source reports no address.
    // The field is named away from `source` so `thiserror` does not read it as the error's cause.
    #[error(
        "subscription `{subscription}`: the scope wires a deferred-retry publisher, but source \
         `{source_type}` reports no redelivery address on broker `{broker}`. Mount it on a source \
         that reports one, or drop `retry_via` from the scope and let `retry_after` requeue \
         immediately"
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
        "subscription `{subscription}`: the scope wires a deferred-retry publisher, but the \
         subscriber was mounted directly, so nothing reports a redelivery address for it. Mount \
         it on a subscription source, or drop `retry_via` from the scope and let `retry_after` \
         requeue immediately"
    )]
    Sourceless {
        /// The subscription as the registration names it.
        subscription: String,
    },
}

/// Opens `source`'s subscription and builds the delivery context it dispatches under.
///
/// The deferred-retry publisher is paired and the redelivery address resolved first, against the
/// live connection, because both may have to talk to the broker (a Pub/Sub subscription is looked
/// up to learn its topic) and because a scope that cannot answer must fail before it holds an
/// open subscription. A scope with no deferred-retry policy asks nothing: the address would have
/// no use.
///
/// # Errors
///
/// Returns the broker's error when the address lookup or the subscription fails,
/// [`RetryPairError`] when the scope's retry policy fails to pair, and [`RetryAddressError`]
/// when the scope defers retries over a source that reports no address.
pub(crate) async fn open_subscription<B, Source>(
    source: Source,
    connected: &Connected<B>,
    scope: &ScopeDelivery,
    subscription: &str,
) -> Result<(Source::Subscriber, Arc<Delivery>), BoxError>
where
    B: Broker,
    Source: SubscriptionSource<Connected<B>>,
{
    // The publisher and the address are read into the pair together, so a fallback that holds one
    // without the other is never built.
    let retry = match scope.retry() {
        Some(pairing) => {
            let publisher = pairing
                .publisher()
                .await
                .map_err(|err| Box::new(err) as BoxError)?;
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
                publisher: Arc::clone(publisher),
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
/// Returns [`RetryAddressError::Sourceless`] when the scope defers retries.
pub(crate) fn open_mounted_subscriber(
    scope: &ScopeDelivery,
    subscription: &str,
) -> Result<Arc<Delivery>, BoxError> {
    if scope.defers_retries() {
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
    use crate::{IncomingMessage, OutgoingMessage, Subscriber};

    /// A scope carrying `retry`, with the harness pieces a scope holds under the `testing`
    /// feature.
    fn scope(retry: Option<RetryPairing>) -> ScopeDelivery {
        ScopeDelivery::new(
            retry,
            TaskTracker::new(),
            #[cfg(feature = "testing")]
            Arc::new(TestHooks::detached()),
            #[cfg(feature = "testing")]
            0,
        )
    }

    /// The pairing a scope wires before startup, against a slot that startup has not filled.
    fn unpaired() -> RetryPairing {
        let slot: ConnectedSlot<MemoryBroker> = Arc::new(Mutex::new(None));
        RetryPairing::new::<MemoryBroker, MemoryPublish>(slot, MemoryPublish)
    }

    /// The policy pairs once, against the broker the slot holds, and the live publisher it yields
    /// reaches that broker. A second call hands back the same publisher instead of pairing again.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_retry_policy_pairs_once_against_the_connected_broker() {
        let broker = MemoryBroker::new();
        let mut subscriber = broker.subscribe("retry.fallback");
        let connected = broker.connect().await.expect("connect");
        let slot: ConnectedSlot<MemoryBroker> = Arc::new(Mutex::new(Some(Arc::new(connected))));
        let pairing = RetryPairing::new::<MemoryBroker, MemoryPublish>(slot, MemoryPublish);

        let first = Arc::clone(pairing.publisher().await.expect("the policy pairs"));
        let second = Arc::clone(
            pairing
                .publisher()
                .await
                .expect("the second call reuses it"),
        );
        assert!(
            Arc::ptr_eq(&first, &second),
            "one scope pairs one publisher"
        );

        first
            .publish_erased(OutgoingMessage::new("retry.fallback", b"deferred"))
            .await
            .expect("the erased fallback publish failed");
        let mut stream = pin!(subscriber.stream());
        let msg = stream
            .next()
            .await
            .expect("the fallback publish must reach the broker")
            .expect("delivery");
        assert_eq!(msg.payload(), b"deferred");
    }

    /// A slot startup never filled cannot pair, and the error says so instead of panicking.
    #[tokio::test]
    async fn a_policy_over_an_unconnected_broker_reports_the_pairing_failure() {
        // Not `expect_err`: the erased publisher has no `Debug` to print on the success path.
        let Err(failed) = unpaired().publisher().await else {
            panic!("nothing to pair against");
        };
        let message = failed.to_string();
        assert!(message.contains("retry_via"), "{message}");
        assert!(message.contains("not connected"), "{message}");
    }

    /// A subscriber mounted without a source has nothing to ask, so a scope that defers retries
    /// has no address for it. The startup error names the subscription and the way out, rather
    /// than letting the subscription run and publish its deferred copies into nothing.
    #[test]
    fn a_sourceless_mount_under_a_retry_publisher_is_a_startup_error() {
        let refused = open_mounted_subscriber(&scope(Some(unpaired())), "orders")
            .expect_err("a scope that defers retries cannot address a sourceless mount");
        let message = refused.to_string();
        assert!(message.contains("orders"), "{message}");
        assert!(message.contains("retry_via"), "{message}");
    }

    /// Without a retry publisher there is nothing to address, so the same mount starts.
    #[test]
    fn a_sourceless_mount_starts_when_the_scope_defers_nothing() {
        let delivery = open_mounted_subscriber(&scope(None), "orders")
            .expect("a scope with no retry publisher asks for no address");
        assert!(delivery.retry.is_none());
    }
}
