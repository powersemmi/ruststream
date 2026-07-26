//! Cross-broker publisher tokens: a [`PublishPolicy`] bound to a concrete registered broker.
//!
//! A bare policy attached at an include site pairs with that scope's own broker. When the
//! handler must publish to a *different* broker (consume Kafka, publish to Redis), the scope of
//! the target broker mints a [`Bound`] token that carries the instance identity a foreign scope
//! cannot provide.

use std::sync::Arc;

use crate::runtime::lifecycle::ConnectedSlot;
use crate::{Broker, Connected, ConnectedBroker, PairError, PublishPolicy};

/// A publisher source bound to a concrete registered broker, minted by
/// [`BrokerScope::bind`](crate::runtime::BrokerScope::bind).
///
/// Being scope-minted is its proof of registration: the token shares the slot the runtime fills
/// with that broker's connected form at startup, so pairing needs no lookup and cannot pick the
/// wrong instance. It implements [`PublishPolicy`] against *any* connected broker by ignoring it
/// and pairing against its own; that is what lets a Kafka-scope registration accept a token for
/// the Redis broker.
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
// against its own. Covering every `C` here also keeps coherence simple - no downstream impl can
// exist for a `Bound`, so it composes with the blanket source handling at the include sites.
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
