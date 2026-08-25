//! Cross-broker publisher tokens: a [`PublishPolicy`] bound to a concrete broker instance.
//!
//! A bare policy attached at an include site pairs with that scope's own broker. When the
//! handler must publish to a *different* broker (consume Kafka, publish to Redis), or a sibling
//! task needs a publisher after start, wrap the target broker in [`Bindable`] and mint [`Bound`]
//! tokens from it before registration - tokens carry the instance identity a foreign scope
//! cannot provide, and the registration order stops mattering (a bidirectional bridge binds
//! both directions up front).

use std::sync::Arc;

use std::sync::Mutex;

use crate::runtime::lifecycle::ConnectedSlot;
use crate::{Broker, Connected, ConnectedBroker, PairError, PublishPolicy};

/// A broker wrapped for token minting before registration.
///
/// `Bindable` owns the slot the runtime fills with the broker's connected form, so [`bind`]
/// hands out [`Bound`] tokens *before* `with_broker` runs - registration order stops mattering,
/// which is what a bidirectional bridge needs (each direction's scope takes the other broker's
/// token). Pass the wrapper itself to
/// [`with_broker`](crate::runtime::RustStream::with_broker); a token whose broker never
/// registers fails fast at pairing with a clear error.
///
/// [`bind`]: Self::bind
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "memory", feature = "json"))]
/// # fn demo() {
/// use ruststream::Broker;
/// use ruststream::memory::{MemoryBroker, MemoryPublish};
/// use ruststream::runtime::{AppInfo, RustStream};
///
/// let broker = MemoryBroker::new().bindable(); // sugar for Bindable::new(..)
/// let egress = broker.bind(MemoryPublish);
/// let app = RustStream::new(AppInfo::new("svc", "0.1.0")).with_broker(broker, |_b| {});
/// // `egress` pairs once the app starts: attach it at an include site, or pair it through
/// // the running handle for a sibling task.
/// # let _ = (app, egress);
/// # }
/// ```
pub struct Bindable<B: Broker> {
    pub(crate) broker: B,
    pub(crate) slot: ConnectedSlot<B>,
}

impl<B: Broker + std::fmt::Debug> std::fmt::Debug for Bindable<B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Bindable")
            .field("broker", &self.broker)
            .finish_non_exhaustive()
    }
}

impl<B: Broker + 'static> Bindable<B> {
    /// Wraps `broker` so publisher tokens can be minted before it registers.
    #[must_use]
    pub fn new(broker: B) -> Self {
        Self {
            broker,
            slot: Arc::new(Mutex::new(None)),
        }
    }

    /// Returns the wrapped broker, for its own constructors (publishers, subscribers).
    #[must_use]
    pub fn broker(&self) -> &B {
        &self.broker
    }

    /// Mints a token for `source`: a publisher source carrying this broker's instance identity,
    /// usable at any include site, in a sibling task via
    /// [`RunningApp::publisher`](crate::runtime::RunningApp::publisher), or directly with
    /// [`Bound::live`] once the app started.
    #[must_use]
    pub fn bind<S>(&self, source: S) -> Bound<B, S>
    where
        S: PublishPolicy<Connected<B>>,
    {
        Bound {
            slot: Arc::clone(&self.slot),
            source,
        }
    }
}

/// What the `with_broker` family accepts.
///
/// Either a bare [`Broker`] (registered with a fresh internal slot) or a [`Bindable`] carrying
/// pre-minted tokens. Implemented by those two shapes only; you never implement or call it.
pub trait BrokerRegistration {
    /// The broker being registered.
    type Broker: Broker + 'static;

    #[doc(hidden)]
    fn into_parts(self) -> (Self::Broker, ConnectedSlot<Self::Broker>);
}

impl<B: Broker + 'static> BrokerRegistration for B {
    type Broker = B;

    fn into_parts(self) -> (B, ConnectedSlot<B>) {
        (self, Arc::new(Mutex::new(None)))
    }
}

impl<B: Broker + 'static> BrokerRegistration for Bindable<B> {
    type Broker = B;

    fn into_parts(self) -> (B, ConnectedSlot<B>) {
        (self.broker, self.slot)
    }
}

/// A publisher source bound to a concrete broker instance, minted by [`Bindable::bind`].
///
/// The token shares the slot the runtime fills with that broker's connected form at startup, so
/// pairing needs no lookup and cannot pick a wrong instance. It implements [`PublishPolicy`]
/// against *any* connected broker by ignoring it and pairing against its own; that is what lets
/// a Kafka-scope registration accept a token for the Redis broker.
pub struct Bound<B2: Broker, S> {
    pub(crate) slot: ConnectedSlot<B2>,
    pub(crate) source: S,
}

impl<B2: Broker, S: std::fmt::Debug> std::fmt::Debug for Bound<B2, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Bound")
            .field("source", &self.source)
            .finish_non_exhaustive()
    }
}

impl<B2: Broker + 'static, S: Clone> Clone for Bound<B2, S> {
    fn clone(&self) -> Self {
        Self {
            slot: Arc::clone(&self.slot),
            source: self.source.clone(),
        }
    }
}

impl<B2, S> Bound<B2, S>
where
    B2: Broker + 'static,
    S: PublishPolicy<Connected<B2>>,
{
    /// Pairs the token against its own broker, producing the live form.
    ///
    /// Available anywhere the application startup has already connected the broker: an
    /// `after_startup` hook (the documented home of a first publish), or a sibling task after
    /// [`start`](crate::runtime::RustStream::start) returned (the
    /// [`RunningApp::publisher`](crate::runtime::RunningApp::publisher) sugar calls this).
    ///
    /// # Errors
    ///
    /// Returns [`PairError`] when the broker is not connected yet (pairing before startup
    /// finished), or when the policy's own pairing fails.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(all(feature = "memory", feature = "json"))]
    /// # fn demo() {
    /// use ruststream::Broker;
    /// use ruststream::memory::{MemoryBroker, MemoryPublish};
    /// use ruststream::runtime::{AppInfo, RustStream};
    ///
    /// let broker = MemoryBroker::new().bindable();
    /// let token = broker.bind(MemoryPublish);
    /// let app = RustStream::new(AppInfo::new("svc", "0.1.0"))
    ///     .with_broker(broker, |_b| {})
    ///     .after_startup(async move |_state| {
    ///         let publisher = token.live().await?;
    ///         // ... publish the first message through `publisher` ...
    ///         # let _ = publisher;
    ///         Ok::<_, ruststream::PairError>(())
    ///     });
    /// # let _ = app;
    /// # }
    /// ```
    pub async fn live(self) -> Result<S::Live, PairError> {
        pair_bound::<B2, S>(&self.slot, self.source).await
    }
}

/// Pairs `source` against the broker in `slot`. The runtime's own pairing entry for tokens.
pub(crate) async fn pair_bound<B2, S>(
    slot: &ConnectedSlot<B2>,
    source: S,
) -> Result<S::Live, PairError>
where
    B2: Broker + 'static,
    S: PublishPolicy<Connected<B2>>,
{
    let connected = slot
        .lock()
        .expect("connected slot mutex poisoned")
        .clone()
        .ok_or_else(|| {
            PairError::from_boxed(Box::from(
                "the token's broker is not connected: pairing happens after startup connects \
                 every registered broker",
            ))
        })?;
    source.pair(connected.as_ref()).await
}

// The token is a policy for ANY connected broker: it ignores the scope's broker and pairs
// against its own.
impl<C, B2, S> PublishPolicy<C> for Bound<B2, S>
where
    C: ConnectedBroker,
    B2: Broker + 'static,
    S: PublishPolicy<Connected<B2>> + Send,
{
    type Live = S::Live;

    async fn pair(self, _connected: &C) -> Result<Self::Live, PairError> {
        pair_bound::<B2, S>(&self.slot, self.source).await
    }
}

#[cfg(all(test, feature = "memory", feature = "json"))]
mod tests {
    use futures::StreamExt;

    use crate::memory::{MemoryBroker, MemoryPublish};
    use crate::runtime::{AppInfo, PublishExt, RustStream};
    use crate::{Broker, IncomingMessage, Subscriber};

    /// Cloning a token duplicates the binding, not the broker: both clones pair against the one
    /// instance the wrapper registered, and neither can pair before it is connected.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_cloned_token_pairs_against_the_same_broker() {
        let bindable = MemoryBroker::new().bindable();
        let mut inbox = bindable.broker().subscribe("bound.out");
        let token = bindable.bind(MemoryPublish);
        let spare = token.clone();

        assert!(
            format!("{bindable:?}").contains("MemoryBroker"),
            "the wrapper names the broker it holds",
        );
        assert!(
            format!("{token:?}").contains("MemoryPublish"),
            "the token names the policy it carries",
        );
        let err = spare
            .clone()
            .live()
            .await
            .expect_err("a token cannot pair before startup connects its broker");
        assert!(
            err.to_string().contains("not connected"),
            "the error must name the reason: {err}",
        );

        let app =
            RustStream::new(AppInfo::new("bound", "0.1.0")).with_broker(bindable, |_scope| {});
        let running = app.start().await.expect("startup failed");

        for (token, payload) in [(token, b"one"), (spare, b"two")] {
            let publisher = token.live().await.expect("pairing after startup failed");
            publisher
                .raw(payload.as_slice())
                .to("bound.out")
                .publish()
                .await
                .expect("publish through the paired token failed");
        }

        let mut stream = std::pin::pin!(inbox.stream());
        let mut seen = Vec::new();
        for _ in 0..2 {
            let msg = stream
                .next()
                .await
                .expect("delivery missing")
                .expect("memory subscriber never errors");
            seen.push(msg.payload().to_vec());
            msg.ack().await.expect("ack failed");
        }
        assert_eq!(
            seen,
            vec![b"one".to_vec(), b"two".to_vec()],
            "both clones reach the broker the wrapper registered",
        );

        running.shutdown().await.expect("shutdown failed");
    }
}
