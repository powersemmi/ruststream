//! Error types shared across the core contract.

use std::error::Error as StdError;

use thiserror::Error;

/// Errors returned by [`IncomingMessage::ack`] and [`IncomingMessage::nack`].
///
/// Implementations should map broker-specific failure modes to one of these variants.
///
/// [`IncomingMessage::ack`]: crate::IncomingMessage::ack
/// [`IncomingMessage::nack`]: crate::IncomingMessage::nack
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum AckError {
    /// The broker does not support explicit acknowledgement (for example a fire-and-forget
    /// transport, or `NATS` core subjects outside `JetStream`).
    ///
    /// It is also the answer for one settlement a transport cannot honour while honouring the
    /// others, per delivery rather than per broker: a requeue on a log read whose commits are
    /// advisory has nothing to bring the record back with, and a transport that acknowledges at
    /// one quality of service may not at another. Saying so is what keeps the runtime honest -
    /// `Ok(())` from `nack(requeue = true)` is a promise the message comes back, which the retry
    /// path and [`conformance`](crate::conformance) both read that way.
    #[error("acknowledgement is not supported by this broker")]
    Unsupported,

    /// The broker rejected the acknowledgement, typically due to network failure or a stale
    /// delivery token.
    #[error("broker rejected the acknowledgement")]
    Broker(#[source] Box<dyn StdError + Send + Sync>),

    /// The acknowledgement was not confirmed by the broker within the configured timeout.
    #[error("acknowledgement timed out")]
    Timeout,
}
