//! The copy path of a host that keeps its retry copies in place: a record of the host's store
//! takes the copy before the delivery settles, and the store publishes it.

use alloc::string::ToString;
use core::time::Duration;

use tracing::warn;

use super::super::{RETRY_COUNT_HEADER, Slot};
use super::{Target, current_retry_count};
use crate::host::{KeptDelivery, PrepareKept, WhenFull};
use crate::lifecycle::BoxError;
use crate::{AckError, HeaderMap, LocalSettle, Str};

/// Drops the original delivery and sends a copy of it to `target` through a record of the
/// host's store: published at once without a delay (or with a zero one), by the store's driver
/// once `delay` has passed otherwise.
///
/// The record is taken and filled before the original is dropped. The copy's bytes go into the
/// record's own buffer; its header map is the delivery's, carried on with the retry count in it.
/// The store prepares the copy there too, while the delivery is at hand: the mount site's
/// transforms read it, and the copy leaves as they left it.
///
/// A store with every record taken answers a copy that would wait for its delay as the mount
/// chose ([`WhenFull`]): `declared` is the destination `.dead_letter(..)` named, which the
/// `DeadLetter` choice sends the delivery to, and the `SkipDelay` choice publishes the copy to
/// the subscription at once. Either way the copy takes no record: it is prepared in a buffer of
/// its own, which allocates. The copy leaves first and the original is dropped once the broker
/// took it; a copy the broker refuses requeues the original, so nothing is lost. A copy that never
/// waits (a dead letter, a zero delay) takes the same way out, whatever the choice.
///
/// # Errors
///
/// Returns the [`AckError`] from dropping the original, as [`publish_copy`] does; the record is
/// freed then.
///
/// # Panics
///
/// When the store is full and the mount chose to panic (the default): a device resets rather than
/// losing a retry or spinning. The message names the subscription, the message type and the
/// capacity.
// `Send` exactly where the runner's store is.
#[allow(clippy::future_not_send)]
pub(super) async fn keep_copy<M, C, Kept>(
    slot: &mut Slot<M>,
    name: &str,
    target: Target<'_>,
    delay: Option<Duration>,
    declared: Option<&str>,
    kept: &Kept,
    build_cx: fn(&M) -> C,
) -> Result<(), AckError>
where
    M: LocalSettle,
    Kept: PrepareKept<C>,
{
    let delay = delay.filter(|delay| !delay.is_zero());
    let Some(record) = kept.take_record() else {
        return spill_copy(slot, name, target, delay, declared, kept, build_cx).await;
    };
    let msg = slot.get();
    let destination = match target {
        Target::Subscription => None,
        Target::DeadLetter(destination) => Some(destination),
    };
    kept.fill(record, msg.payload(), counted(msg));
    kept.prepare(
        record,
        destination,
        &KeptDelivery {
            subscription: name,
            message: msg,
            build_cx,
        },
    );
    if let Err(err) = drop_original(slot).await {
        kept.free(record);
        return Err(err);
    }
    // Only a copy back to the subscription carries a delay: a dead letter leaves at once.
    match delay {
        Some(delay) => kept.arm(record, delay),
        None => {
            if let Err(err) = kept.send_now(record, destination).await {
                warn_lost(name, destination, &err);
            }
        }
    }
    Ok(())
}

/// Sends the copy without a record, because the store has none free.
///
/// # Panics
///
/// When the copy would wait for its delay and the mount chose to panic.
// `Send` exactly where the runner's store is.
#[allow(clippy::future_not_send)]
async fn spill_copy<M, C, Kept>(
    slot: &mut Slot<M>,
    name: &str,
    target: Target<'_>,
    delay: Option<Duration>,
    declared: Option<&str>,
    kept: &Kept,
    build_cx: fn(&M) -> C,
) -> Result<(), AckError>
where
    M: LocalSettle,
    Kept: PrepareKept<C>,
{
    let destination = match (target, delay) {
        (Target::DeadLetter(destination), _) => Some(destination),
        (Target::Subscription, None) => None,
        (Target::Subscription, Some(_)) => match kept.when_full() {
            WhenFull::Panic => panic!(
                "the store of retry copies of subscription `{name}` is full (capacity {}) while \
                 a `{}` waits for a retry: raise it with `.retry_capacity::<N>()`, or choose what \
                 a full store does with `.when_retry_full(DeadLetter)` or \
                 `.when_retry_full(SkipDelay)`",
                kept.capacity(),
                kept.input(),
            ),
            WhenFull::DeadLetter => declared,
            WhenFull::SkipDelay => None,
        },
    };
    let msg = slot.get();
    let spilled = kept.spill(
        msg.payload(),
        counted(msg),
        destination,
        &KeptDelivery {
            subscription: name,
            message: msg,
            build_cx,
        },
    );
    // The copy leaves before the original is dropped, so a refusal loses nothing: the broker
    // takes the original back. Dropping the original can still fail after the copy left, which
    // leaves both with the broker.
    if let Err(err) = kept.send_spilled(spilled, destination).await {
        warn!(
            target: "ruststream::dispatch",
            subscription = %name,
            destination = destination.unwrap_or(name),
            error = %err,
            "the retry copy was refused; requeuing the delivery",
        );
        return slot.take().nack(true).await;
    }
    drop_original(slot).await
}

/// The delivery's headers with the retry count advanced: what the copy carries.
fn counted<M: LocalSettle>(msg: &M) -> HeaderMap {
    let mut headers = msg.headers().clone();
    let next_count = current_retry_count(&headers) + 1;
    headers.insert(Str::from_static(RETRY_COUNT_HEADER), next_count.to_string());
    headers
}

/// Drops the original, as on the erased path: a transport with no settlement has nothing to drop,
/// and any other settle failure leaves the delivery with the broker.
async fn drop_original<M: LocalSettle>(slot: &mut Slot<M>) -> Result<(), AckError> {
    match slot.take().nack(false).await {
        Ok(()) | Err(AckError::Unsupported) => Ok(()),
        Err(err) => Err(err),
    }
}

fn warn_lost(name: &str, destination: Option<&str>, err: &BoxError) {
    warn!(
        target: "ruststream::dispatch",
        subscription = %name,
        destination = destination.unwrap_or(name),
        error = %err,
        "publishing the retry copy failed; message lost",
    );
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
    use crate::host::{
        Keeping, KeptCopies, KeptDelivery, LocalDetach, OnOneThread, PrepareKept, WhenFull,
    };
    use crate::lifecycle::BoxError;
    use crate::testkit::log_capture;
    use crate::{HeaderMap, LocalSettle, RetryDeclaration};

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
        Spill {
            payload: Vec<u8>,
            count: Option<String>,
            destination: Option<String>,
        },
        SendSpilled(Option<String>),
    }

    /// A store with `room` records that records what it is asked, and fails every publish when
    /// `failing`.
    struct Store {
        room: usize,
        failing: bool,
        when_full: WhenFull,
        asked: RefCell<Vec<Asked>>,
    }

    impl Store {
        const fn with_room(room: usize) -> Self {
            Self {
                room,
                failing: false,
                when_full: WhenFull::Panic,
                asked: RefCell::new(Vec::new()),
            }
        }

        fn asked(&self) -> Vec<Asked> {
            self.asked.borrow().clone()
        }
    }

    impl KeptCopies for Store {
        type Spilled = ();

        fn capacity(&self) -> usize {
            self.room
        }

        fn input(&self) -> &'static str {
            "Order"
        }

        fn when_full(&self) -> WhenFull {
            self.when_full
        }

        fn take_record(&self) -> Option<usize> {
            (self.room > 0).then_some(0)
        }

        fn send_spilled(
            &self,
            (): (),
            destination: Option<&str>,
        ) -> impl Future<Output = Result<(), BoxError>> {
            self.asked
                .borrow_mut()
                .push(Asked::SendSpilled(destination.map(ToOwned::to_owned)));
            let sent: Result<(), BoxError> = if self.failing {
                Err(io::Error::other("connection closed").into())
            } else {
                Ok(())
            };
            ready(sent)
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

    // The settlement under test, not the transforms: a record is kept as it was filled.
    impl<Cx> PrepareKept<Cx> for Store {
        fn prepare<M: LocalSettle>(
            &self,
            _record: usize,
            _destination: Option<&str>,
            _delivery: &KeptDelivery<'_, M, Cx>,
        ) {
        }

        fn spill<M: LocalSettle>(
            &self,
            payload: &[u8],
            headers: HeaderMap,
            destination: Option<&str>,
            _delivery: &KeptDelivery<'_, M, Cx>,
        ) {
            self.asked.borrow_mut().push(Asked::Spill {
                payload: payload.to_vec(),
                count: headers.get_str(RETRY_COUNT_HEADER).map(ToOwned::to_owned),
                destination: destination.map(ToOwned::to_owned),
            });
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

    fn spilled(destination: Option<&str>) -> [Asked; 2] {
        [
            Asked::Spill {
                payload: b"body".to_vec(),
                count: Some("1".to_owned()),
                destination: destination.map(ToOwned::to_owned),
            },
            Asked::SendSpilled(destination.map(ToOwned::to_owned)),
        ]
    }

    #[tokio::test]
    #[should_panic(
        expected = "the store of retry copies of subscription `orders` is full (capacity 0) while a `"
    )]
    async fn a_full_store_panics_by_default_naming_the_subscription_the_type_and_the_capacity() {
        let store = Store::with_room(0);
        settle_on(
            &store,
            HandlerResult::retry_after(LATER),
            &Delivery::empty(),
            Settlement::Accepted,
        )
        .await;
    }

    #[tokio::test]
    async fn a_full_store_sends_the_delivery_to_the_dead_letter_when_the_mount_chose_it() {
        let store = Store {
            when_full: WhenFull::DeadLetter,
            ..Store::with_room(0)
        };
        let delivery = Delivery::empty().declaring(
            RetryDeclaration::new()
                .with_max_attempts(crate::nonzero!(3u32))
                .with_dead_letter("orders.dead"),
        );
        let settled = settle_on(
            &store,
            HandlerResult::retry_after(LATER),
            &delivery,
            Settlement::Accepted,
        )
        .await;
        assert_eq!(settled, 1, "the original is dropped");
        assert_eq!(store.asked(), spilled(Some("orders.dead")));
    }

    #[tokio::test]
    async fn a_full_store_publishes_the_copy_at_once_when_the_mount_skips_the_delay() {
        let store = Store {
            when_full: WhenFull::SkipDelay,
            ..Store::with_room(0)
        };
        let settled = settle_on(
            &store,
            HandlerResult::retry_after(LATER),
            &Delivery::empty(),
            Settlement::Accepted,
        )
        .await;
        assert_eq!(settled, 1, "the original is dropped");
        assert_eq!(store.asked(), spilled(None));
    }

    #[tokio::test]
    async fn a_full_store_does_not_hold_a_copy_that_leaves_at_once() {
        // The default choice would panic for a copy that waits; these never wait.
        let store = Store::with_room(0);
        settle_on(
            &store,
            HandlerResult::retry_after(Duration::ZERO),
            &Delivery::empty(),
            Settlement::Accepted,
        )
        .await;
        assert_eq!(store.asked(), spilled(None));

        let store = Store::with_room(0);
        let delivery =
            Delivery::empty().declaring(RetryDeclaration::new().with_dead_letter("orders.dead"));
        settle_on(
            &store,
            HandlerResult::retry_after(LATER),
            &delivery,
            Settlement::Accepted,
        )
        .await;
        assert_eq!(store.asked(), spilled(Some("orders.dead")));
    }

    #[tokio::test]
    async fn a_spilled_copy_is_published_before_the_original_is_dropped() {
        // A refused copy requeues the original, which a dropped original could not be.
        for when_full in [WhenFull::DeadLetter, WhenFull::SkipDelay] {
            let store = Store {
                failing: true,
                when_full,
                ..Store::with_room(0)
            };
            let delivery = Delivery::empty().declaring(
                RetryDeclaration::new()
                    .with_max_attempts(crate::nonzero!(3u32))
                    .with_dead_letter("orders.dead"),
            );
            let settled = settle_on(
                &store,
                HandlerResult::retry_after(LATER),
                &delivery,
                Settlement::Accepted,
            )
            .await;
            assert_eq!(
                settled, 2,
                "{when_full:?}: the original is requeued, not lost"
            );
            assert_eq!(
                store.asked().len(),
                2,
                "{when_full:?}: the copy was tried once"
            );
        }
    }

    #[tokio::test]
    async fn a_failed_settle_after_the_spilled_copy_left_is_reported() {
        let store = Store {
            when_full: WhenFull::SkipDelay,
            ..Store::with_room(0)
        };
        settle_on(
            &store,
            HandlerResult::retry_after(LATER),
            &Delivery::empty(),
            Settlement::Rejected,
        )
        .await;
        assert_eq!(store.asked(), spilled(None), "the copy had left already");
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
