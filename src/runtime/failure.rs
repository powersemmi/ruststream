//! One failure policy for the two unhappy dispatch paths: a handler that panics and a payload
//! that fails to decode. Both funnel into the same [`FailurePolicy`] vocabulary, but carry
//! separate per-subscriber defaults: a panic is an internal bug, so it
//! [fails fast](FailurePolicy::FailFast); a decode failure is usually bad external input, so it
//! [drops](FailurePolicy::Drop) the one bad message rather than letting any producer take the
//! consumer down.

use std::any::Any;
use std::time::Duration;

use super::handler::HandlerResult;
use super::shutdown::Shutdown;

/// What a subscriber does when it cannot process a message.
///
/// Set per subscriber, separately for a handler panic and for a decode failure, with the
/// `#[subscriber(.., on_failure(panic = .., decode = ..))]` clause. Each maps onto a settlement
/// of the offending message, except [`FailFast`](Self::FailFast), which tears the service down so
/// an orchestrator restarts it.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
///
/// use ruststream::runtime::FailurePolicy;
///
/// let policy = FailurePolicy::RetryAfter(Duration::from_secs(5));
/// assert!(matches!(policy, FailurePolicy::RetryAfter(_)));
/// # Ok::<(), std::convert::Infallible>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum FailurePolicy {
    /// Log a loud error naming the subscription, then initiate a graceful app shutdown and make
    /// [`run`](super::RustStream::run) return `Err`, so an orchestrator restarts the service and
    /// the failure lands in the logs. The default for a handler panic. Only takes effect under an
    /// unwinding panic profile; with `panic = "abort"` the process is already gone and there is
    /// nothing to catch.
    FailFast,
    /// Drop the message: `nack(false)`. The broker discards or dead-letters it. The default for a
    /// decode failure - one malformed payload should not stop the consumer.
    Drop,
    /// Requeue the message: `nack(true)`. Useful when the failure is transient (a dependency not
    /// yet available, a schema not yet propagated).
    Retry,
    /// Requeue the message, but ask the broker to redeliver no sooner than the given delay:
    /// `nack_after(..)`. A broker without native delayed redelivery takes the copy the runtime
    /// publishes instead - see
    /// [`HandlerOutcome::retry_after`](super::HandlerOutcome::retry_after).
    RetryAfter(Duration),
    /// Acknowledge the failed message to move past it: `ack`. A deliberate poison-message escape
    /// hatch - it advances past a message that could not be processed. This is not success; the
    /// message is gone and was never handled.
    Skip,
}

impl FailurePolicy {
    /// Maps every non-fail-fast policy onto the [`HandlerResult`] that settles the message.
    ///
    /// [`FailFast`](Self::FailFast) has no settlement of its own (it tears the service down), so it
    /// is excluded here; callers handle it before reaching this point.
    pub(crate) const fn settlement(self) -> Option<HandlerResult> {
        match self {
            Self::FailFast => None,
            Self::Drop => Some(HandlerResult::drop()),
            Self::Retry => Some(HandlerResult::retry()),
            Self::RetryAfter(delay) => Some(HandlerResult::retry_after(delay)),
            Self::Skip => Some(HandlerResult::Ack),
        }
    }
}

/// The pair of failure policies a subscriber carries: one for a handler panic, one for a decode
/// failure. Threaded from the def to the dispatch loop alongside the
/// [`Workers`](super::Workers) policy.
///
/// # Examples
///
/// ```
/// use ruststream::runtime::{FailurePolicies, FailurePolicy};
///
/// // The defaults: a panic fails fast, a decode failure drops.
/// let policies = FailurePolicies::default();
/// assert_eq!(policies.panic, FailurePolicy::FailFast);
/// assert_eq!(policies.decode, FailurePolicy::Drop);
/// # Ok::<(), std::convert::Infallible>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct FailurePolicies {
    /// What to do when the handler panics. Defaults to [`FailurePolicy::FailFast`].
    pub panic: FailurePolicy,
    /// What to do when a delivery's typed view cannot be materialized: the codec fails to decode
    /// the payload, or a [`Headers`](super::Headers) contract fails to parse. One policy
    /// covers both - a header contract violation is the same class of bad external input as a
    /// payload that does not decode. Defaults to [`FailurePolicy::Drop`].
    pub decode: FailurePolicy,
}

impl FailurePolicies {
    /// Overrides the handler-panic policy, leaving the decode policy unchanged. The builder the
    /// `#[subscriber]` macro uses to fill in the keys named in `on_failure(..)`.
    #[must_use]
    pub fn with_panic(mut self, policy: FailurePolicy) -> Self {
        self.panic = policy;
        self
    }

    /// Overrides the decode-failure policy, leaving the panic policy unchanged.
    #[must_use]
    pub fn with_decode(mut self, policy: FailurePolicy) -> Self {
        self.decode = policy;
        self
    }
}

impl Default for FailurePolicies {
    fn default() -> Self {
        Self {
            panic: FailurePolicy::FailFast,
            decode: FailurePolicy::Drop,
        }
    }
}

/// What one dispatch loop needs to apply a [`FailurePolicy`]: the per-subscriber [policies] and the
/// app-level [shutdown signal](Shutdown). The policies are captured when the subscriber
/// is mounted (like [`Workers`](super::Workers)); the handle is supplied once at run time, shared
/// by every loop so any of them can tear the service down.
///
/// [policies]: FailurePolicies
#[derive(Debug, Clone)]
pub(crate) struct DispatchFailure {
    pub(crate) policies: FailurePolicies,
    pub(crate) shutdown: Shutdown,
}

impl DispatchFailure {
    /// Bundles the per-subscriber `policies` with the app-level error-shutdown `shutdown` handle.
    pub(crate) fn new(policies: FailurePolicies, shutdown: Shutdown) -> Self {
        Self { policies, shutdown }
    }
}

/// Renders the payload of a caught panic as a human-readable reason, recovering the message a
/// `panic!` / `unwrap` / `expect` carries (`&str` or `String`) and falling back to a placeholder
/// for anything else.
pub(crate) fn panic_reason(payload: &(dyn Any + Send)) -> String {
    payload.downcast_ref::<&'static str>().map_or_else(
        || {
            payload
                .downcast_ref::<String>()
                .cloned()
                .unwrap_or_else(|| "handler panicked".to_owned())
        },
        |s| (*s).to_owned(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn settlement_maps_every_non_fail_fast_policy() {
        assert_eq!(FailurePolicy::FailFast.settlement(), None);
        assert_eq!(
            FailurePolicy::Drop.settlement(),
            Some(HandlerResult::drop())
        );
        assert_eq!(
            FailurePolicy::Retry.settlement(),
            Some(HandlerResult::retry())
        );
        assert_eq!(
            FailurePolicy::RetryAfter(Duration::from_secs(2)).settlement(),
            Some(HandlerResult::retry_after(Duration::from_secs(2)))
        );
        assert_eq!(FailurePolicy::Skip.settlement(), Some(HandlerResult::Ack));
    }

    #[test]
    fn defaults_are_fail_fast_panic_and_drop_decode() {
        let policies = FailurePolicies::default();
        assert_eq!(policies.panic, FailurePolicy::FailFast);
        assert_eq!(policies.decode, FailurePolicy::Drop);
    }

    #[test]
    fn builders_override_one_key_each() {
        let policies = FailurePolicies::default()
            .with_panic(FailurePolicy::Drop)
            .with_decode(FailurePolicy::Skip);
        assert_eq!(policies.panic, FailurePolicy::Drop);
        assert_eq!(policies.decode, FailurePolicy::Skip);
    }

    #[test]
    fn panic_reason_recovers_the_message() {
        let as_str: &(dyn Any + Send) = &"boom";
        assert_eq!(panic_reason(as_str), "boom");

        let owned = String::from("owned boom");
        let as_string: &(dyn Any + Send) = &owned;
        assert_eq!(panic_reason(as_string), "owned boom");

        let other: &(dyn Any + Send) = &42_u8;
        assert_eq!(panic_reason(other), "handler panicked");
    }
}
