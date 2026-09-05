//! Assertion and subject helpers for end-user broker integration tests.
//!
//! These are intentionally broker-agnostic and operate on the `ruststream-core` traits, so
//! they can be reused by application tests against any broker.

use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::time::{SystemTime, UNIX_EPOCH};
use std::{future::Future, time::Duration};

use crate::Subscriber;
use futures::StreamExt;
use tokio::time::{sleep, timeout};

/// A subject built from `prefix` that no other run reuses.
///
/// A suite publishing under a fixed subject passes against a fresh server and fails the second
/// time on any broker that keeps what the first run left behind: a retained log replays both
/// runs into one subscription, a durable queue still holds the earlier messages, a key namespace
/// still holds the earlier type. The suites in [`capabilities`](super::capabilities) and
/// [`harness::lifecycle`](super::harness::lifecycle) name their subjects through this, so they
/// are re-runnable against one server and two of them can run in one process at once; a broker's
/// own end-to-end suite has the same problem and the same answer.
///
/// The suffix mixes the process id, the wall clock and a per-process counter, so two runs in one
/// process and two processes against one server all get their own subject. Everything it adds is
/// `[0-9a-f-]` after a `.`, which every broker's subject, topic, queue and key grammar admits.
///
/// # Examples
///
/// ```
/// use ruststream::conformance::helpers::unique_subject;
///
/// let first = unique_subject("orders.e2e");
/// let second = unique_subject("orders.e2e");
/// assert!(first.starts_with("orders.e2e."));
/// assert_ne!(first, second);
/// ```
#[must_use]
pub fn unique_subject(prefix: &str) -> String {
    // Per process, so two suites started in the same nanosecond still differ.
    static NEXT: AtomicU64 = AtomicU64::new(0);

    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_nanos());
    let seq = NEXT.fetch_add(1, AtomicOrdering::Relaxed);
    let pid = std::process::id();
    format!("{prefix}.{pid:x}-{nanos:x}-{seq:x}")
}

/// Polls `cond` until it returns `true` or `deadline` elapses.
///
/// Returns `true` if the condition was met within the deadline, `false` on timeout.
pub async fn wait_until<F>(mut cond: F, deadline: Duration) -> bool
where
    F: FnMut() -> bool,
{
    timeout(deadline, async {
        while !cond() {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .is_ok()
}

/// Awaits an async `cond` repeatedly until it returns `true` or the deadline elapses.
pub async fn wait_until_async<F, Fut>(mut cond: F, deadline: Duration) -> bool
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    timeout(deadline, async {
        while !cond().await {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .is_ok()
}

/// Asserts that `subscriber` yields no further messages within `quiet_for`.
///
/// Drains the next pending item (if any) via the subscriber's stream. Returns `Ok(())` when
/// the subscriber stayed quiet, or `Err(msg)` carrying the unexpected delivery.
///
/// # Errors
///
/// Returns the unexpected `IncomingMessage` if one arrives before the timeout elapses.
pub async fn wait_for_no_messages<S>(
    subscriber: &mut S,
    quiet_for: Duration,
) -> Result<(), S::Message>
where
    S: Subscriber,
{
    let mut stream = std::pin::pin!(subscriber.stream());
    match timeout(quiet_for, stream.next()).await {
        Err(_) | Ok(None | Some(Err(_))) => Ok(()),
        Ok(Some(Ok(msg))) => Err(msg),
    }
}

/// Awaits the next delivery from `subscriber`, panicking on timeout, stream end, or error.
///
/// Convenience helper for the common test pattern of "expect one message".
///
/// # Panics
///
/// Panics if the stream times out, ends, or yields an error.
pub async fn next_message<S>(subscriber: &mut S, within: Duration) -> S::Message
where
    S: Subscriber,
    S::Error: std::fmt::Debug,
{
    let mut stream = std::pin::pin!(subscriber.stream());
    let item = timeout(within, stream.next())
        .await
        .expect("subscriber stream timed out");
    let item = item.expect("subscriber stream ended unexpectedly");
    item.expect("subscriber stream yielded error")
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    use super::*;

    /// Two properties the suites depend on: no two runs share a subject, and what the suffix adds
    /// stays inside the grammar every broker's subjects, topics, queues and keys admit.
    #[test]
    fn unique_subjects_differ_and_stay_addressable() {
        let subjects: Vec<String> = (0..64)
            .map(|_| unique_subject("conformance.check"))
            .collect();
        let distinct: HashSet<&String> = subjects.iter().collect();
        assert_eq!(
            distinct.len(),
            subjects.len(),
            "every run must get a subject of its own",
        );
        for subject in &subjects {
            let suffix = subject
                .strip_prefix("conformance.check.")
                .unwrap_or_else(|| panic!("the prefix must lead the subject: {subject}"));
            assert!(
                suffix.chars().all(|c| c.is_ascii_hexdigit() || c == '-'),
                "the suffix must stay addressable on every broker: {subject}",
            );
        }
    }

    #[tokio::test]
    async fn wait_until_returns_true_when_condition_eventually_holds() {
        let flag = Arc::new(AtomicBool::new(false));
        let flag_clone = Arc::clone(&flag);
        tokio::spawn(async move {
            sleep(Duration::from_millis(20)).await;
            flag_clone.store(true, Ordering::SeqCst);
        });
        assert!(wait_until(|| flag.load(Ordering::SeqCst), Duration::from_millis(500)).await);
    }

    #[tokio::test]
    async fn wait_until_returns_false_on_timeout() {
        let outcome = wait_until(|| false, Duration::from_millis(50)).await;
        assert!(!outcome);
    }

    #[tokio::test]
    async fn wait_until_async_resolves_and_times_out() {
        let flag = Arc::new(AtomicBool::new(false));
        let flag_clone = Arc::clone(&flag);
        tokio::spawn(async move {
            sleep(Duration::from_millis(20)).await;
            flag_clone.store(true, Ordering::SeqCst);
        });
        assert!(
            wait_until_async(
                || {
                    let flag = Arc::clone(&flag);
                    async move { flag.load(Ordering::SeqCst) }
                },
                Duration::from_millis(500),
            )
            .await
        );
        assert!(
            !wait_until_async(|| async { false }, Duration::from_millis(50)).await,
            "a never-true condition must time out"
        );
    }

    #[cfg(feature = "memory")]
    #[tokio::test]
    async fn next_message_returns_the_next_delivery() {
        use crate::{IncomingMessage, OutgoingMessage, Publisher, memory::MemoryBroker};

        let broker = MemoryBroker::new();
        let mut sub = broker.subscribe("conf-next");
        broker
            .publisher()
            .publish(OutgoingMessage::new("conf-next", b"hi".as_slice()))
            .await
            .unwrap();

        let msg = next_message(&mut sub, Duration::from_secs(1)).await;
        assert_eq!(msg.payload(), b"hi");
        msg.ack().await.unwrap();
    }

    #[cfg(feature = "memory")]
    #[tokio::test]
    async fn wait_for_no_messages_distinguishes_quiet_from_delivery() {
        use crate::{OutgoingMessage, Publisher, memory::MemoryBroker};

        let broker = MemoryBroker::new();
        let mut sub = broker.subscribe("conf-quiet");

        // No publish yet: the subscriber stays quiet within the window.
        assert!(
            wait_for_no_messages(&mut sub, Duration::from_millis(50))
                .await
                .is_ok()
        );

        broker
            .publisher()
            .publish(OutgoingMessage::new("conf-quiet", b"surprise".as_slice()))
            .await
            .unwrap();
        // Now a delivery arrives, so the helper hands it back as an error.
        let unexpected = wait_for_no_messages(&mut sub, Duration::from_millis(200)).await;
        assert!(unexpected.is_err());
    }
}
