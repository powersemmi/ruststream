//! Error types shared across the core contract.

#[cfg(feature = "alloc")]
use alloc::boxed::Box;
#[cfg(feature = "alloc")]
use core::error::Error as StdError;

use thiserror::Error;

/// The cause of a broker failure in the build without `alloc`: a kind and a code, both `Copy`.
///
/// There is no heap to box the client's own error on, so a broker crate of that build reports
/// what it can name: a short kind (`"link down"`) and the client's numeric code (zero when it
/// has none).
///
/// # Examples
///
/// ```
/// use ruststream_core::{AckError, Cause};
///
/// // A broker maps its client's failure onto a cause when a settlement is refused.
/// let refused = AckError::Broker(Cause::new("link down", 7));
/// assert_eq!(refused.to_string(), "broker rejected the acknowledgement");
/// ```
#[cfg(not(feature = "alloc"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Error)]
#[error("{kind} (code {code})")]
pub struct Cause {
    kind: &'static str,
    code: i32,
}

#[cfg(not(feature = "alloc"))]
impl Cause {
    /// A cause named `kind`, with the client's `code`.
    #[must_use]
    pub const fn new(kind: &'static str, code: i32) -> Self {
        Self { kind, code }
    }

    /// The short name of the failure.
    #[must_use]
    pub const fn kind(&self) -> &'static str {
        self.kind
    }

    /// The client's numeric code.
    #[must_use]
    pub const fn code(&self) -> i32 {
        self.code
    }
}

/// Errors returned by [`Settle::ack`] and [`Settle::nack`].
///
/// Implementations should map broker-specific failure modes to one of these variants.
///
/// [`Settle::ack`]: crate::Settle::ack
/// [`Settle::nack`]: crate::Settle::nack
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
    /// path and [`conformance`](https://docs.rs/ruststream/latest/ruststream/conformance/index.html) both read that way.
    #[error("acknowledgement is not supported by this broker")]
    Unsupported,

    /// The broker rejected the acknowledgement, typically due to network failure or a stale
    /// delivery token.
    #[cfg(feature = "alloc")]
    #[error("broker rejected the acknowledgement")]
    Broker(#[source] Box<dyn StdError + Send + Sync>),

    /// The broker rejected the acknowledgement, typically due to network failure or a stale
    /// delivery token.
    #[cfg(not(feature = "alloc"))]
    #[error("broker rejected the acknowledgement")]
    Broker(#[source] Cause),

    /// The acknowledgement was not confirmed by the broker within the configured timeout.
    #[error("acknowledgement timed out")]
    Timeout,
}

/// Errors returned by [`Subscribe::declare_retry`], where a registration mounted by a bare
/// subscription name declares how its deliveries retry.
///
/// [`Subscribe::declare_retry`]: crate::Subscribe::declare_retry
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum DeclareRetryError {
    /// The broker moves a spent delivery itself and maps no declaration made over a bare name,
    /// so the cap and the destination would reach nobody.
    ///
    /// What the default answers on a [`BrokerMoves`](crate::BrokerMoves) broker. A broker with a
    /// native mechanism implements the method and answers `Ok` for the subscription it maps the
    /// declaration onto.
    #[error(
        "broker `{broker}` moves a spent delivery itself and maps no retry declaration made over \
         a bare subscription name, so the cap and the destination declared here reach nobody: \
         declare the cap and the destination on this broker's own descriptor, which maps them \
         onto the subscription"
    )]
    Unsupported {
        /// The connected broker that maps no declaration for a bare name.
        broker: &'static str,
    },

    /// The broker rejected the declaration: an unknown subscription, a destination its topology
    /// has no room for, half a declaration where its mechanism needs both halves.
    ///
    /// The reason rides the message rather than the cause alone, because a startup refusal is
    /// read as one line.
    #[cfg(feature = "alloc")]
    #[error("broker rejected the retry declaration: {0}")]
    Broker(#[source] Box<dyn StdError + Send + Sync>),

    /// The broker rejected the declaration, in the build without `alloc`.
    #[cfg(not(feature = "alloc"))]
    #[error("broker rejected the retry declaration: {0}")]
    Broker(#[source] Cause),
}

#[cfg(all(test, not(feature = "alloc")))]
mod tests {
    use core::fmt::Write as _;

    use super::*;
    use crate::FixedBuf;

    #[test]
    fn a_broker_failure_carries_a_copy_cause() {
        let cause = Cause::new("link down", 7);
        let error = AckError::Broker(cause);
        // The cause is `Copy`: the error can be read after it was passed on.
        let again = cause;
        assert_eq!(again, Cause::new("link down", 7));
        let source = error.source().expect("the cause is the source");
        assert!(source.is::<Cause>());
    }

    #[test]
    fn the_cause_names_its_kind_and_code() {
        let mut text = FixedBuf::<64>::new();
        write!(text, "{}", Cause::new("link down", 7)).unwrap();
        assert_eq!(&*text, b"link down (code 7)");
    }

    #[test]
    fn a_retry_declaration_failure_carries_a_copy_cause() {
        let error = DeclareRetryError::Broker(Cause::new("no queue", -2));
        assert!(error.source().unwrap().is::<Cause>());
    }
}
