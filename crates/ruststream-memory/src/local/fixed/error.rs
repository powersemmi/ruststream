//! The error of the fixed local bus.

use ruststream_core::{AckError, Cause};
use thiserror::Error;

/// An error of the local bus.
///
/// # Examples
///
/// ```
/// use ruststream_memory::local::prelude::*;
///
/// static BUS: LocalMemoryBus<4> = LocalMemoryBus::new();
///
/// /// A publish after the bus shut down is refused, never lost silently.
/// async fn late_publish() -> Result<(), LocalMemoryError> {
///     let connected = LocalMemoryBroker::new(&BUS).connect().await?;
///     let publisher = connected.publisher();
///     connected.shutdown().await?;
///     let reading = &b"21"[..];
///     publisher
///         .publish(OutgoingMessage::with_payload("sensors/temp", reading), None)
///         .await
///         .map_err(PublishMessageError::into_error)
/// }
///
/// assert_eq!(embassy_futures::block_on(late_publish()), Err(LocalMemoryError::ShutDown));
/// ```
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum LocalMemoryError {
    /// The bus is shut down: it takes no publish, subscription or requeue until it connects again.
    #[error("the local memory broker is shut down")]
    ShutDown,
    /// A subscription of the name holds `capacity` messages, waiting or in hand, so the publish
    /// reached none of the subscriptions.
    #[error(
        "a subscription holds {capacity} messages, its capacity; the publish reached no \
         subscription"
    )]
    Full {
        /// The capacity of each subscription, the bus's `MESSAGES`.
        capacity: usize,
    },
    /// The payload does not fit the room a message has on the bus.
    #[error("a payload of {size} bytes does not fit the {capacity} bytes a message holds")]
    PayloadTooLarge {
        /// The size of the payload published.
        size: usize,
        /// The largest payload the bus holds, its `PAYLOAD`.
        capacity: usize,
    },
    /// Every subscription of the bus is taken.
    #[error("the bus holds {capacity} subscriptions and all of them are open")]
    NoFreeSubscription {
        /// How many subscriptions the bus holds, its `SUBSCRIPTIONS`.
        capacity: usize,
    },
    /// A subscription name built at run time does not fit the room the bus copies it into.
    #[error("a name built at run time holds at most {capacity} bytes")]
    NameTooLong {
        /// The longest name the bus copies, in bytes.
        capacity: usize,
    },
}

impl LocalMemoryError {
    /// The error as the cause a settlement reports, which holds no allocation.
    pub(super) const fn cause(self) -> Cause {
        let (kind, code) = match self {
            Self::ShutDown => ("the local memory broker is shut down", 1),
            Self::Full { .. } => ("a subscription is full", 2),
            Self::PayloadTooLarge { .. } => ("the payload does not fit a message", 3),
            Self::NoFreeSubscription { .. } => ("every subscription is open", 4),
            Self::NameTooLong { .. } => ("the subscription name is too long", 5),
        };
        Cause::new(kind, code)
    }
}

impl From<LocalMemoryError> for AckError {
    fn from(error: LocalMemoryError) -> Self {
        Self::Broker(error.cause())
    }
}
