//! The copy path of a host that keeps its retry copies in place: a record of the host's store
//! takes the copy before the delivery settles, and the store publishes it.

use alloc::string::ToString;
use bytes::Bytes;
use core::time::Duration;

use super::super::{RETRY_COUNT_HEADER, Slot};
use super::{Target, current_retry_count};
use crate::host::{HostDetach, KeptDelivery, PrepareKept};
use crate::publish_failure::Reaction;
use crate::{AckError, HeaderMap, Settle, Str};

/// Drops the original delivery and sends a copy of it to `target` through a record of the
/// host's store: published at once without a delay (or with a zero one), by the store's driver
/// once `delay` has passed otherwise.
///
/// The record is taken and filled before the original is dropped. The copy's bytes go into the
/// record's own buffer; its header map is the delivery's, carried on with the retry count in it.
/// The store prepares the copy there too, while the delivery is at hand: the mount site's
/// transforms read it, and the copy leaves as they left it.
///
/// A copy that would wait for its delay in a store with every record taken is a failed publish
/// with its own error, "no free record". A copy that never waits (a dead letter, a zero delay)
/// takes no record then, and is prepared in a buffer of its own, which allocates. The original
/// is dropped before the copy leaves, as on the published path; a copy that fails is lost, and
/// the handler's `error` continuation, when the outcome carried one, is given the failure and
/// runs beside the delivery.
///
/// # Errors
///
/// Returns the [`AckError`] from dropping the original, as [`publish_copy`] does; the record is
/// freed then, and no copy leaves.
// `Send` exactly where the runner's store is.
#[allow(clippy::future_not_send)]
#[allow(clippy::too_many_arguments)]
pub(super) async fn keep_copy<M, C, Kept, Host>(
    slot: &mut Slot<M>,
    name: &str,
    target: Target<'_>,
    delay: Option<Duration>,
    reaction: &mut Option<Reaction>,
    kept: &Kept,
    host: &Host,
    build_cx: fn(&M) -> C,
) -> Result<(), AckError>
where
    M: Settle,
    Kept: PrepareKept<C>,
    Host: HostDetach,
{
    let delay = delay.filter(|delay| !delay.is_zero());
    let destination = match target {
        Target::Subscription => None,
        Target::DeadLetter(destination) => Some(destination),
    };
    let Some(record) = kept.take_record() else {
        return spill_copy(
            slot,
            name,
            destination,
            delay,
            reaction,
            kept,
            host,
            build_cx,
        )
        .await;
    };
    let msg = slot.get();
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
        Some(delay) => kept.arm(record, delay, reaction.take()),
        None => {
            if let Some(follow) = kept.send_now(record, destination, reaction.take()).await {
                host.detach_boxed(follow);
            }
        }
    }
    Ok(())
}

/// Sends the copy without a record, because the store has none free.
///
/// A copy that would wait for its delay has nowhere to wait: it is the "no free record" failure.
// `Send` exactly where the runner's store is.
#[allow(clippy::future_not_send)]
#[allow(clippy::too_many_arguments)]
async fn spill_copy<M, C, Kept, Host>(
    slot: &mut Slot<M>,
    name: &str,
    destination: Option<&str>,
    delay: Option<Duration>,
    reaction: &mut Option<Reaction>,
    kept: &Kept,
    host: &Host,
    build_cx: fn(&M) -> C,
) -> Result<(), AckError>
where
    M: Settle,
    Kept: PrepareKept<C>,
    Host: HostDetach,
{
    let msg = slot.get();
    if destination.is_none() && delay.is_some() {
        // Copied for the continuation only: a failure nobody asked about copies nothing.
        let payload = reaction
            .is_some()
            .then(|| Bytes::copy_from_slice(msg.payload()));
        let headers = counted(msg);
        drop_original(slot).await?;
        if let Some(follow) = kept.full(payload, headers, reaction.take()) {
            host.detach_boxed(follow);
        }
        return Ok(());
    }
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
    // The original is dropped before the copy leaves, so a copy that fails is lost and the
    // delivery never runs again because of it.
    drop_original(slot).await?;
    if let Some(follow) = kept
        .send_spilled(spilled, destination, reaction.take())
        .await
    {
        host.detach_boxed(follow);
    }
    Ok(())
}

/// The delivery's headers with the retry count advanced: what the copy carries.
fn counted<M: Settle>(msg: &M) -> HeaderMap {
    let mut headers = msg.headers().clone();
    let next_count = current_retry_count(&headers) + 1;
    headers.insert(Str::from_static(RETRY_COUNT_HEADER), next_count.to_string());
    headers
}

/// Drops the original, as on the erased path: a transport with no settlement has nothing to drop,
/// and any other settle failure leaves the delivery with the broker.
async fn drop_original<M: Settle>(slot: &mut Slot<M>) -> Result<(), AckError> {
    match slot.take().nack(false).await {
        Ok(()) | Err(AckError::Unsupported) => Ok(()),
        Err(err) => Err(err),
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
    use core::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
    use core::time::Duration;
    use std::io;

    use bytes::Bytes;

    use super::super::super::fixtures::{PlainMessage, Settlement};
    use super::super::{settle_outcome, settle_reacting};
    use crate::dispatch::{Delivery, RETRY_COUNT_HEADER, Slot};
    use crate::handler::Continuation;
    use crate::handler::HandlerResult;
    use crate::host::{Detach, Keeping, KeptCopies, KeptDelivery, PrepareKept};
    use crate::publish_failure::{
        ErrorHook, FailedMessage, Failures, NoFreeRecordError, Reaction, publish_lost,
    };
    use crate::testkit::log_capture;
    use crate::{HeaderMap, RetryDeclaration, Settle};

    /// What the store was asked to do, in order.
    #[derive(Debug, Clone, PartialEq, Eq)]
    enum Asked {
        Fill {
            payload: Vec<u8>,
            count: Option<String>,
        },
        Arm(Duration, bool),
        SendNow(Option<String>, bool),
        Free,
        Spill {
            payload: Vec<u8>,
            count: Option<String>,
            destination: Option<String>,
        },
        SendSpilled(Option<String>, bool),
        Full {
            payload: Option<Vec<u8>>,
        },
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

        /// What a publish of the copy leaves behind: nothing when it succeeds, and when the
        /// store is `failing` what the failure makes of the handler's continuation.
        fn outcome_of_publish(
            &self,
            reaction: Option<Reaction>,
            destination: Option<&str>,
        ) -> Option<Continuation> {
            if !self.failing {
                return None;
            }
            let target = destination.unwrap_or("orders");
            publish_lost(
                reaction,
                "orders",
                target,
                "Order",
                io::Error::other("connection closed").into(),
                || FailedMessage::new(target, Some(Bytes::from_static(b"body")), HeaderMap::new()),
            )
        }
    }

    impl KeptCopies for Store {
        type Spilled = ();

        fn take_record(&self) -> Option<usize> {
            (self.room > 0).then_some(0)
        }

        fn send_spilled(
            &self,
            (): (),
            destination: Option<&str>,
            reaction: Option<Reaction>,
        ) -> impl Future<Output = Option<Continuation>> {
            self.asked.borrow_mut().push(Asked::SendSpilled(
                destination.map(ToOwned::to_owned),
                reaction.is_some(),
            ));
            ready(self.outcome_of_publish(reaction, destination))
        }

        fn full(
            &self,
            payload: Option<Bytes>,
            _headers: HeaderMap,
            reaction: Option<Reaction>,
        ) -> Option<Continuation> {
            self.asked.borrow_mut().push(Asked::Full {
                payload: payload.as_ref().map(|bytes| bytes.to_vec()),
            });
            publish_lost(
                reaction,
                "orders",
                "orders",
                "Order",
                NoFreeRecordError::new(self.room).into(),
                || FailedMessage::new("orders", payload, HeaderMap::new()),
            )
        }

        fn fill(&self, _record: usize, payload: &[u8], headers: HeaderMap) {
            self.asked.borrow_mut().push(Asked::Fill {
                payload: payload.to_vec(),
                count: headers.get_str(RETRY_COUNT_HEADER).map(ToOwned::to_owned),
            });
        }

        fn arm(&self, _record: usize, delay: Duration, reaction: Option<Reaction>) {
            self.asked
                .borrow_mut()
                .push(Asked::Arm(delay, reaction.is_some()));
        }

        fn send_now(
            &self,
            _record: usize,
            destination: Option<&str>,
            reaction: Option<Reaction>,
        ) -> impl Future<Output = Option<Continuation>> {
            self.asked.borrow_mut().push(Asked::SendNow(
                destination.map(ToOwned::to_owned),
                reaction.is_some(),
            ));
            ready(self.outcome_of_publish(reaction, destination))
        }

        fn free(&self, _record: usize) {
            self.asked.borrow_mut().push(Asked::Free);
        }
    }

    // The settlement under test, not the transforms: a record is kept as it was filled.
    impl<Cx> PrepareKept<Cx> for Store {
        fn prepare<M: Settle>(
            &self,
            _record: usize,
            _destination: Option<&str>,
            _delivery: &KeptDelivery<'_, M, Cx>,
        ) {
        }

        fn spill<M: Settle>(
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

    /// The app's host beside the store: the copies never reach it, and it counts the
    /// continuations a failed publish leaves with it.
    struct NoWork {
        boxed: AtomicUsize,
    }

    impl Detach for NoWork {
        fn detach_boxed(&self, _work: Continuation) {
            self.boxed.fetch_add(1, Ordering::SeqCst);
        }

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
    /// `store`; returns how the delivery was settled (1 dropped, 2 requeued) and how many
    /// continuations the failed publishes left with the host.
    async fn settle_on(
        store: &Store,
        outcome: HandlerResult,
        delivery: &Delivery,
        settlement: Settlement,
    ) -> u8 {
        settle_reacting_on(store, outcome, None, delivery, settlement)
            .await
            .0
    }

    async fn settle_reacting_on(
        store: &Store,
        outcome: HandlerResult,
        reaction: Option<Reaction>,
        delivery: &Delivery,
        settlement: Settlement,
    ) -> (u8, usize) {
        let settled = Arc::new(AtomicU8::new(0));
        let msg = PlainMessage {
            payload: Bytes::from_static(b"body"),
            headers: HeaderMap::new(),
            settled: Arc::clone(&settled),
            settlement,
        };
        let work = NoWork {
            boxed: AtomicUsize::new(0),
        };
        settle_reacting(
            &mut Slot::new(msg),
            outcome,
            reaction,
            "orders",
            delivery,
            &Keeping(&work, store),
            unit_cx,
        )
        .await;
        (
            settled.load(Ordering::SeqCst),
            work.boxed.load(Ordering::SeqCst),
        )
    }

    /// A handler's `.error` continuation, for the tests that only watch whether it is asked.
    fn hook() -> Reaction {
        Reaction::new(ErrorHook::new(async move |_failures: Failures| {}), None)
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
        assert_eq!(store.asked(), [filled(), Asked::Arm(LATER, false)]);
    }

    #[tokio::test]
    async fn a_record_holds_the_error_continuation_until_the_copy_is_published() {
        let store = Store::with_room(1);
        settle_reacting_on(
            &store,
            HandlerResult::retry_after(LATER),
            Some(hook()),
            &Delivery::empty(),
            Settlement::Accepted,
        )
        .await;
        assert_eq!(store.asked(), [filled(), Asked::Arm(LATER, true)]);
    }

    fn spilled(destination: Option<&str>, has_error: bool) -> [Asked; 2] {
        [
            Asked::Spill {
                payload: b"body".to_vec(),
                count: Some("1".to_owned()),
                destination: destination.map(ToOwned::to_owned),
            },
            Asked::SendSpilled(destination.map(ToOwned::to_owned), has_error),
        ]
    }

    #[tokio::test]
    async fn a_delayed_copy_finding_no_free_record_is_a_failed_publish_of_its_own() {
        let store = Store::with_room(0);
        let (settled, reactions) = settle_reacting_on(
            &store,
            HandlerResult::retry_after(LATER),
            Some(hook()),
            &Delivery::empty(),
            Settlement::Accepted,
        )
        .await;
        assert_eq!(settled, 1, "the original is dropped, never requeued");
        assert_eq!(
            store.asked(),
            [Asked::Full {
                payload: Some(b"body".to_vec())
            }]
        );
        assert_eq!(reactions, 1, "the continuation runs beside the delivery");
    }

    #[tokio::test]
    async fn a_full_store_copies_the_bytes_only_for_a_continuation() {
        let store = Store::with_room(0);
        settle_on(
            &store,
            HandlerResult::retry_after(LATER),
            &Delivery::empty(),
            Settlement::Accepted,
        )
        .await;
        assert_eq!(store.asked(), [Asked::Full { payload: None }]);
    }

    #[tokio::test]
    async fn a_full_store_warns_naming_the_subscription_the_type_and_the_error() {
        let (events, _guard) = log_capture::start();
        let store = Store::with_room(0);
        settle_on(
            &store,
            HandlerResult::retry_after(LATER),
            &Delivery::empty(),
            Settlement::Accepted,
        )
        .await;
        let fields = log_capture::find(&events, "publishing the message failed; message lost");
        assert_eq!(
            fields.get("subscription").map(String::as_str),
            Some("orders")
        );
        assert_eq!(
            fields.get("message_type").map(String::as_str),
            Some("Order")
        );
        assert_eq!(
            fields.get("error").map(String::as_str),
            Some("no free record in the store of retry copies (capacity 0)")
        );
    }

    #[tokio::test]
    async fn a_full_store_does_not_hold_a_copy_that_leaves_at_once() {
        // A copy that waits has nowhere to wait; these never wait.
        let store = Store::with_room(0);
        settle_on(
            &store,
            HandlerResult::retry_after(Duration::ZERO),
            &Delivery::empty(),
            Settlement::Accepted,
        )
        .await;
        assert_eq!(store.asked(), spilled(None, false));

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
        assert_eq!(store.asked(), spilled(Some("orders.dead"), false));
    }

    #[tokio::test]
    async fn a_refused_spilled_copy_is_lost_and_the_delivery_is_not_requeued() {
        let store = Store {
            failing: true,
            ..Store::with_room(0)
        };
        let (settled, reactions) = settle_reacting_on(
            &store,
            HandlerResult::retry_after(Duration::ZERO),
            Some(hook()),
            &Delivery::empty(),
            Settlement::Accepted,
        )
        .await;
        assert_eq!(settled, 1, "the original is dropped, not requeued");
        assert_eq!(store.asked(), spilled(None, true));
        assert_eq!(reactions, 1, "the continuation runs beside the delivery");
    }

    #[tokio::test]
    async fn a_failed_settle_sends_no_spilled_copy() {
        let store = Store::with_room(0);
        settle_on(
            &store,
            HandlerResult::retry_after(Duration::ZERO),
            &Delivery::empty(),
            Settlement::Rejected,
        )
        .await;
        assert_eq!(
            store.asked(),
            [Asked::Spill {
                payload: b"body".to_vec(),
                count: Some("1".to_owned()),
                destination: None,
            }],
            "a copy on top of a delivery the broker keeps would duplicate it",
        );
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
        assert_eq!(store.asked(), [filled(), Asked::SendNow(None, false)]);
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
        assert_eq!(store.asked(), [filled(), Asked::Arm(LATER, false)]);
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
            [
                filled(),
                Asked::SendNow(Some("orders.dead".to_owned()), false)
            ]
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
        assert_eq!(store.asked(), [filled(), Asked::SendNow(None, false)]);
    }

    /// A one-thread runner whose copies wait in a store of the mount's own, run with none.
    struct StoreRunner;

    impl Detach for StoreRunner {
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
            &StoreRunner,
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
        let (events, _guard) = log_capture::start();
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
        assert_eq!(store.asked(), [filled(), Asked::SendNow(None, false)]);
        let fields = log_capture::find(&events, "publishing the message failed; message lost");
        assert_eq!(
            fields.get("destination").map(String::as_str),
            Some("orders")
        );
        assert_eq!(
            fields.get("error").map(String::as_str),
            Some("connection closed")
        );
    }
}
