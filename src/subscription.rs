//! Subscription descriptors: how a handler is bound to one broker subscription.
//!
//! A [`SubscriptionSource`] is the value a broker crate exposes as its subscriber configuration:
//! it carries everything needed to open one subscription (subject / name, consumer group,
//! durable name, delivery policy, ...) and knows how to turn that into a live [`Subscriber`]
//! against a connected broker. The default [`Name`] source covers brokers that only need a name
//! string (those implementing [`Subscribe`]); richer brokers ship their own sources.
//!
//! This is the seam the `#[subscriber(..)]` macro and the application object build on: the macro
//! takes a source (a name string or a broker config value), the runtime resolves it once after
//! the broker is connected.

use std::{borrow::Cow, future::Future};

use crate::{Broker, Subscribe, Subscriber};

/// A description of one subscription, resolved against a concrete broker at startup.
///
/// The runtime calls [`subscribe`] once, after [`Broker::connect`], to obtain the live
/// [`Subscriber`]. The associated [`Subscriber`](Self::Subscriber) type lives on the source rather
/// than the broker, so a single broker can offer several subscription kinds with different
/// subscriber types (for example `Redis` pub/sub versus streams).
///
/// [`subscribe`]: Self::subscribe
///
/// # Examples
///
/// ```
/// use ruststream::{Broker, SubscriptionSource};
///
/// async fn open<B, S>(source: S, broker: &B) -> Result<S::Subscriber, B::Error>
/// where
///     B: Broker,
///     S: SubscriptionSource<B>,
/// {
///     source.subscribe(broker).await
/// }
/// ```
pub trait SubscriptionSource<B: Broker> {
    /// The subscriber type this source opens.
    type Subscriber: Subscriber;

    /// The name (subject / channel) this subscription binds to.
    ///
    /// Used for handler metadata and `AsyncAPI` generation; it need not be the only routing
    /// information the source carries.
    fn name(&self) -> &str;

    /// Opens the subscription against `broker`. Called once, after [`Broker::connect`].
    ///
    /// # Errors
    ///
    /// Returns [`Broker::Error`] when the broker rejects the subscription or the transport fails.
    fn subscribe(
        self,
        broker: &B,
    ) -> impl Future<Output = Result<Self::Subscriber, B::Error>> + Send;
}

/// The default [`SubscriptionSource`]: subscribe by name string via the [`Subscribe`] capability.
///
/// Produced by `#[subscriber("name")]` and usable directly with any broker implementing
/// [`Subscribe`].
///
/// # Examples
///
/// ```
/// use ruststream::{Broker, Subscribe, SubscriptionSource, Name};
///
/// async fn open<B: Subscribe>(broker: &B) -> Result<B::Subscriber, B::Error> {
///     Name::new("orders").subscribe(broker).await
/// }
/// ```
#[derive(Debug, Clone)]
pub struct Name(Cow<'static, str>);

impl Name {
    /// Creates a name source bound to `name`.
    #[must_use]
    pub fn new(name: impl Into<Cow<'static, str>>) -> Self {
        Self(name.into())
    }
}

impl<B: Subscribe> SubscriptionSource<B> for Name {
    type Subscriber = B::Subscriber;

    fn name(&self) -> &str {
        &self.0
    }

    async fn subscribe(self, broker: &B) -> Result<Self::Subscriber, B::Error> {
        broker.subscribe(&self.0).await
    }
}
