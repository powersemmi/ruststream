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
use std::sync::Arc;

use thiserror::Error;
use tokio_util::task::TaskTracker;

use crate::runtime::dispatch::Delivery;
use crate::runtime::lifecycle::BoxError;
use crate::runtime::publisher_registry::ErasedPublisher;
use crate::{Broker, Connected, SubscriptionSource};

#[cfg(feature = "testing")]
use crate::testing::coordinator::TestHooks;

/// The deferred `retry_after` fallback of one subscription: the publisher its scope wired,
/// paired with the address that subscription's source reported at startup.
pub(crate) struct DeferredRetry {
    pub(crate) publisher: Arc<dyn ErasedPublisher>,
    pub(crate) address: Arc<str>,
}

/// What a broker scope hands every subscription it starts.
///
/// The publisher is scope-wide, the address is not, so this is the half of a [`Delivery`] that is
/// known before a subscription resolves: pairing the two is what
/// [`open_subscription`] does.
pub(crate) struct ScopeDelivery {
    /// The publisher wired with [`retry_via`](crate::runtime::BrokerScope::retry_via), or `None`
    /// when the scope did not opt in - in which case a `retry_after` on a broker without native
    /// delayed redelivery degrades to an immediate requeue.
    retry_publisher: Option<Arc<dyn ErasedPublisher>>,
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
    /// The scope context, with `retry_publisher` as its deferred-retry fallback.
    pub(crate) fn new(
        retry_publisher: Option<Arc<dyn ErasedPublisher>>,
        tasks: TaskTracker,
        #[cfg(feature = "testing")] hooks: Arc<TestHooks>,
        #[cfg(feature = "testing")] scope_id: usize,
    ) -> Self {
        Self {
            retry_publisher,
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

    /// The publisher this scope defers retries through, if it wired one.
    fn retry_publisher(&self) -> Option<&Arc<dyn ErasedPublisher>> {
        self.retry_publisher.as_ref()
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
/// The redelivery address is resolved first, against the live connection, because that is what
/// may have to ask the broker (a Pub/Sub subscription is looked up to learn its topic) and
/// because a scope that cannot answer must fail before it holds an open subscription. A scope
/// with no deferred-retry publisher asks nothing: the address would have no use.
///
/// # Errors
///
/// Returns the broker's error when the address lookup or the subscription fails, and
/// [`RetryAddressError`] when the scope defers retries over a source that reports no address.
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
    let retry = match scope.retry_publisher() {
        Some(publisher) => {
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
    if scope.retry_publisher().is_some() {
        return Err(Box::new(RetryAddressError::Sourceless {
            subscription: subscription.to_owned(),
        }));
    }
    Ok(Arc::new(Delivery::for_subscription(scope, None)))
}

#[cfg(all(test, feature = "memory"))]
mod tests {
    use super::*;
    use crate::memory::MemoryBroker;

    /// A scope carrying `retry_publisher`, with the harness pieces a scope holds under the
    /// `testing` feature.
    fn scope(retry_publisher: Option<Arc<dyn ErasedPublisher>>) -> ScopeDelivery {
        ScopeDelivery::new(
            retry_publisher,
            TaskTracker::new(),
            #[cfg(feature = "testing")]
            Arc::new(TestHooks::detached()),
            #[cfg(feature = "testing")]
            0,
        )
    }

    /// A subscriber mounted without a source has nothing to ask, so a scope that defers retries
    /// has no address for it. The startup error names the subscription and the way out, rather
    /// than letting the subscription run and publish its deferred copies into nothing.
    #[test]
    fn a_sourceless_mount_under_a_retry_publisher_is_a_startup_error() {
        let publisher: Arc<dyn ErasedPublisher> = Arc::new(MemoryBroker::new().publisher());
        let refused = open_mounted_subscriber(&scope(Some(publisher)), "orders")
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
