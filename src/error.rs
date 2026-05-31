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
