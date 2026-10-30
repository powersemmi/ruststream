//! The copy path of a host that keeps its retry copies in place: a record of the host's store
//! takes the copy before the delivery settles, and the store publishes it.

use alloc::string::ToString;
use core::pin::Pin;
use core::task::{Context, Poll};
use core::time::Duration;

use tracing::warn;

use super::super::{RETRY_COUNT_HEADER, Slot};
use super::{Target, current_retry_count};
use crate::host::KeptCopies;
use crate::{AckError, LocalIncomingMessage, Str};

/// Drops the original delivery and sends a copy of it to `target` through a record of the
/// host's store: published at once without a delay (or with a zero one), by the store's driver
/// once `delay` has passed otherwise.
///
/// The record is taken and filled before the original is dropped, so a store with every record
/// taken has the delivery requeued, and the broker keeps it. The requeue then yields once: a broker
/// that redelivers at once would otherwise keep the subscription busy within one poll of the task,
/// and the store's driver, the other subscriptions and the other tasks would never run. The copy's bytes go into the
/// record's own buffer; its header map is the delivery's, carried on with the retry count in it.
///
/// # Errors
///
/// Returns the [`AckError`] from dropping the original, as [`publish_copy`] does; the record is
/// freed then.
// `Send` exactly where the runner's store is.
#[allow(clippy::future_not_send)]
pub(super) async fn keep_copy<M, Kept>(
    slot: &mut Slot<M>,
    name: &str,
    target: Target<'_>,
    delay: Option<Duration>,
    kept: &Kept,
) -> Result<(), AckError>
where
    M: LocalIncomingMessage,
    Kept: KeptCopies,
{
    let Some(record) = kept.take_record() else {
        warn!(
            target: "ruststream::dispatch",
            subscription = %name,
            "the store of retry copies is full; requeuing the delivery now",
        );
        let requeued = slot.take().nack(true).await;
        YieldOnce(false).await;
        return requeued;
    };
    let msg = slot.get();
    let mut headers = msg.headers().clone();
    let next_count = current_retry_count(&headers) + 1;
    headers.insert(Str::from_static(RETRY_COUNT_HEADER), next_count.to_string());
    kept.fill(record, msg.payload(), headers);
    // As on the erased path: a transport with no settlement has nothing to drop, and any other
    // settle failure leaves the delivery with the broker.
    match slot.take().nack(false).await {
        Ok(()) | Err(AckError::Unsupported) => {}
        Err(err) => {
            kept.free(record);
            return Err(err);
        }
    }
    let destination = match target {
        Target::Subscription => None,
        Target::DeadLetter(destination) => Some(destination),
    };
    // Only a copy back to the subscription carries a delay: a dead letter leaves at once.
    match delay.filter(|delay| !delay.is_zero()) {
        Some(delay) => kept.arm(record, delay),
        None => {
            if let Err(err) = kept.send_now(record, destination).await {
                warn!(
                    target: "ruststream::dispatch",
                    subscription = %name,
                    destination = destination.unwrap_or(name),
                    error = %err,
                    "publishing the retry copy failed; message lost",
                );
            }
        }
    }
    Ok(())
}

/// Pending once, with the task woken, then ready: hands the executor back between two deliveries
/// of a subscription whose broker never makes it wait.
struct YieldOnce(bool);

impl Future for YieldOnce {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.0 {
            return Poll::Ready(());
        }
        self.0 = true;
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

#[cfg(test)]
// The store double is a plain cell, as a one-thread runner's store is.
#[allow(clippy::future_not_send)]
mod tests {
    use alloc::borrow::ToOwned;
    use alloc::string::String;
    use alloc::sync::Arc;
    use alloc::vec::Vec;
    use core::cell::RefCell;
    use core::future::ready;
    use core::sync::atomic::{AtomicU8, Ordering};
    use core::time::Duration;
    use std::io;

    use bytes::Bytes;

    use super::super::super::fixtures::{PlainMessage, Settlement};
    use super::super::settle_outcome;
    use crate::dispatch::{Delivery, RETRY_COUNT_HEADER, Slot};
    use crate::handler::HandlerResult;
    use crate::host::{Keeping, KeptCopies, LocalDetach, OnOneThread};
    use crate::lifecycle::BoxError;
    use crate::testkit::log_capture;
    use crate::{HeaderMap, RetryDeclaration};

    /// What the store was asked to do, in order.
    #[derive(Debug, Clone, PartialEq, Eq)]
    enum Asked {
        Fill {
            payload: Vec<u8>,
            count: Option<String>,
        },
        Arm(Duration),
        SendNow(Option<String>),
        Free,
    }

    /// A store with `room` records that records what it is asked, and fails every publish when
    /// `failing`.
    struct Store {
        room: usize,
        failing: bool,
        asked: RefCell<Vec<Asked>>,
    }

    impl Store {
        const fn with_room(room: usize) -> Self {
            Self {
                room,
                failing: false,
                asked: RefCell::new(Vec::new()),
            }
        }

        fn asked(&self) -> Vec<Asked> {
            self.asked.borrow().clone()
        }
    }

    impl KeptCopies for Store {
        fn take_record(&self) -> Option<usize> {
            (self.room > 0).then_some(0)
        }

        fn fill(&self, _record: usize, payload: &[u8], headers: HeaderMap) {
            self.asked.borrow_mut().push(Asked::Fill {
                payload: payload.to_vec(),
                count: headers.get_str(RETRY_COUNT_HEADER).map(ToOwned::to_owned),
            });
        }

        fn arm(&self, _record: usize, delay: Duration) {
            self.asked.borrow_mut().push(Asked::Arm(delay));
        }

        fn send_now(
            &self,
            _record: usize,
            destination: Option<&str>,
        ) -> impl Future<Output = Result<(), BoxError>> {
            self.asked
                .borrow_mut()
                .push(Asked::SendNow(destination.map(ToOwned::to_owned)));
            let sent: Result<(), BoxError> = if self.failing {
                Err(io::Error::other("connection closed").into())
            } else {
                Ok(())
            };
            ready(sent)
        }

        fn free(&self, _record: usize) {
            self.asked.borrow_mut().push(Asked::Free);
        }
    }

    /// The app's host beside the store: the copies never reach it.
    struct NoWork;

    impl LocalDetach for NoWork {
        fn detach<Work>(&self, _work: Work)
        where
            Work: Future<Output = ()> + 'static,
        {
        }

        fn detach_after<Work>(&self, _delay: Duration, _work: Work)
        where
            Work: Future<Output = ()> + 'static,
        {
        }
    }

    fn unit_cx<M>(_msg: &M) {}

    /// Settles one delivery carrying `headers` by `outcome` on a host keeping its copies in
    /// `store`; returns how the delivery was settled (1 dropped, 2 requeued).
    async fn settle_on(
        store: &Store,
        outcome: HandlerResult,
        delivery: &Delivery,
        settlement: Settlement,
    ) -> u8 {
        let settled = Arc::new(AtomicU8::new(0));
        let msg = PlainMessage {
            payload: Bytes::from_static(b"body"),
            headers: HeaderMap::new(),
            settled: Arc::clone(&settled),
            settlement,
        };
        settle_outcome(
            &mut Slot::new(msg),
            outcome,
            "orders",
            delivery,
            &Keeping(&NoWork, store),
            unit_cx,
        )
        .await;
        settled.load(Ordering::SeqCst)
    }

    const LATER: Duration = Duration::from_secs(5);

    fn filled() -> Asked {
        Asked::Fill {
            payload: b"body".to_vec(),
            count: Some("1".to_owned()),
        }
    }

    #[tokio::test]
    async fn a_delayed_copy_is_kept_counted_and_armed_once_the_original_is_dropped() {
        let store = Store::with_room(1);
        let settled = settle_on(
            &store,
            HandlerResult::retry_after(LATER),
            &Delivery::empty(),
            Settlement::Accepted,
        )
        .await;
        assert_eq!(settled, 1, "the original is dropped");
        assert_eq!(store.asked(), [filled(), Asked::Arm(LATER)]);
    }

    #[tokio::test]
    async fn a_full_store_requeues_the_delivery_and_keeps_nothing() {
        let store = Store::with_room(0);
        let settled = settle_on(
            &store,
            HandlerResult::retry_after(LATER),
            &Delivery::empty(),
            Settlement::Accepted,
        )
        .await;
        assert_eq!(settled, 2, "the delivery is requeued");
        assert_eq!(store.asked(), []);
    }

    #[tokio::test]
    async fn a_zero_delay_publishes_the_copy_at_once() {
        let store = Store::with_room(1);
        settle_on(
            &store,
            HandlerResult::retry_after(Duration::ZERO),
            &Delivery::empty(),
            Settlement::Accepted,
        )
        .await;
        assert_eq!(store.asked(), [filled(), Asked::SendNow(None)]);
    }

    #[tokio::test]
    async fn a_failed_settle_frees_the_record_and_publishes_nothing() {
        let store = Store::with_room(1);
        settle_on(
            &store,
            HandlerResult::retry_after(LATER),
            &Delivery::empty(),
            Settlement::Rejected,
        )
        .await;
        assert_eq!(store.asked(), [filled(), Asked::Free]);
    }

    #[tokio::test]
    async fn a_transport_with_no_settlement_still_keeps_the_copy() {
        let store = Store::with_room(1);
        settle_on(
            &store,
            HandlerResult::retry_after(LATER),
            &Delivery::empty(),
            Settlement::Unsupported,
        )
        .await;
        assert_eq!(store.asked(), [filled(), Asked::Arm(LATER)]);
    }

    #[tokio::test]
    async fn a_dead_letter_leaves_at_once_for_its_destination() {
        let store = Store::with_room(1);
        let delivery =
            Delivery::empty().declaring(RetryDeclaration::new().with_dead_letter("orders.dead"));
        settle_on(
            &store,
            HandlerResult::retry_after(LATER),
            &delivery,
            Settlement::Accepted,
        )
        .await;
        assert_eq!(
            store.asked(),
            [filled(), Asked::SendNow(Some("orders.dead".to_owned()))]
        );
    }

    #[tokio::test]
    async fn an_immediate_retry_under_a_cap_is_a_copy_through_the_store() {
        let store = Store::with_room(1);
        let delivery = Delivery::empty()
            .declaring(RetryDeclaration::new().with_max_attempts(crate::nonzero!(3u32)));
        let settled = settle_on(
            &store,
            HandlerResult::Nack { requeue: true },
            &delivery,
            Settlement::Accepted,
        )
        .await;
        assert_eq!(settled, 1, "the original is dropped");
        assert_eq!(store.asked(), [filled(), Asked::SendNow(None)]);
    }

    /// A one-thread runner whose copies wait in a store of the mount's own, run with none.
    struct StoreRunner;

    impl LocalDetach for StoreRunner {
        const STORES_COPIES: bool = true;

        fn detach<Work>(&self, _work: Work)
        where
            Work: Future<Output = ()> + 'static,
        {
        }

        fn detach_after<Work>(&self, _delay: Duration, _work: Work)
        where
            Work: Future<Output = ()> + 'static,
        {
        }
    }

    #[tokio::test]
    async fn a_delay_with_no_store_names_the_step_that_keeps_copies() {
        let (events, _guard) = log_capture::start();
        let settled = Arc::new(AtomicU8::new(0));
        let msg = PlainMessage {
            payload: Bytes::from_static(b"body"),
            headers: HeaderMap::new(),
            settled: Arc::clone(&settled),
            settlement: Settlement::Accepted,
        };
        settle_outcome(
            &mut Slot::new(msg),
            HandlerResult::retry_after(LATER),
            "orders",
            &Delivery::empty(),
            &OnOneThread(&StoreRunner),
            unit_cx,
        )
        .await;
        assert_eq!(
            settled.load(Ordering::SeqCst),
            2,
            "the delivery is requeued"
        );
        let warned = log_capture::find(
            &events,
            "retry_after on a mount with no store of copies; requeuing immediately (the delay \
             is dropped). A mount whose descriptor addresses its copies keeps them with \
             `.retry_capacity::<CAP>()`",
        );
        assert_eq!(
            warned.get("subscription").map(String::as_str),
            Some("orders")
        );
    }

    #[tokio::test]
    async fn a_failed_copy_is_logged_rather_than_propagated() {
        let store = Store {
            failing: true,
            ..Store::with_room(1)
        };
        let settled = settle_on(
            &store,
            HandlerResult::retry_after(Duration::ZERO),
            &Delivery::empty(),
            Settlement::Accepted,
        )
        .await;
        assert_eq!(settled, 1, "the original is dropped; the copy is lost");
        assert_eq!(store.asked(), [filled(), Asked::SendNow(None)]);
    }
}
