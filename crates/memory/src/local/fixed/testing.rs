//! The fixed local bus under the test harness: its in-process transition, the view of the
//! connected form the harness drives, and the bookkeeping that view needs.
//!
//! The bookkeeping (the publish log and the coordinator that counts deliveries in flight) is the
//! harness's own and lives on the heap of the test binary. It runs inside a section the failing
//! allocator of a no-heap test lets through, so what that allocator still fails is an allocation
//! of the framework.

use alloc::vec::Vec;
use core::cell::RefCell;

use assert_no_alloc::permit_alloc;
use embassy_sync::blocking_mutex::Mutex;
use embassy_sync::blocking_mutex::raw::CriticalSectionRawMutex;
use ruststream_core::__alloc::bytes::Bytes;
use ruststream_core::{Broker, Headers, OutgoingMessage, RawMessage};
use ruststream_runtime::testing::{Backlog, Coordinator, InProcess, TestableBroker};

use super::{ConnectedLocalMemoryBroker, LocalMemoryBroker};

/// What the test harness reads off a bus: every accepted publish, and the coordinator that counts
/// the deliveries in flight.
pub(super) struct Harness {
    log: Mutex<CriticalSectionRawMutex, RefCell<Vec<RawMessage>>>,
    coordinator: Mutex<CriticalSectionRawMutex, RefCell<Option<Coordinator>>>,
}

impl Harness {
    pub(super) const fn new() -> Self {
        Self {
            log: Mutex::new(RefCell::new(Vec::new())),
            coordinator: Mutex::new(RefCell::new(None)),
        }
    }

    /// Starts a run: the bus is a `static`, so a test that connects it again must not read the
    /// log or count on the coordinator of the run before.
    fn reset(&self) {
        permit_alloc(|| {
            self.log.lock(|log| log.borrow_mut().clear());
            self.coordinator
                .lock(|coordinator| coordinator.borrow_mut().take());
        });
    }

    fn install(&self, coordinator: Coordinator) {
        permit_alloc(|| {
            self.coordinator
                .lock(|slot| slot.borrow_mut().replace(coordinator));
        });
    }

    pub(super) fn record(&self, name: &str, payload: &[u8], headers: &Headers) {
        permit_alloc(|| {
            let message = RawMessage::new(name, Bytes::copy_from_slice(payload))
                .with_headers(headers.clone());
            self.log.lock(|log| log.borrow_mut().push(message));
        });
    }

    fn published(&self, name: &str) -> Vec<RawMessage> {
        permit_alloc(|| {
            self.log.lock(|log| {
                log.borrow()
                    .iter()
                    .filter(|message| message.name() == name)
                    .cloned()
                    .collect()
            })
        })
    }

    fn coordinator(&self) -> Option<Coordinator> {
        self.coordinator.lock(|slot| slot.borrow().clone())
    }

    pub(super) fn enqueued(&self) {
        if let Some(coordinator) = self.coordinator() {
            coordinator.enqueued();
        }
    }

    pub(super) fn consumed(&self) {
        if let Some(coordinator) = self.coordinator() {
            coordinator.consumed();
        }
    }

    pub(super) fn exhausted(&self) -> bool {
        self.coordinator()
            .is_some_and(|coordinator| coordinator.exhausted())
    }
}

// The bus lives in the process by nature, so the transition the harness connects through is the
// ordinary `connect`, after the bookkeeping of an earlier run is cleared.
impl<const MESSAGES: usize, const PAYLOAD: usize, const SUBSCRIPTIONS: usize> InProcess
    for LocalMemoryBroker<MESSAGES, PAYLOAD, SUBSCRIPTIONS>
{
    fn connect_in_process(self) -> impl Future<Output = Result<Self::Connected, Self::Error>> {
        self.bus.harness().reset();
        self.connect()
    }
}

impl<const MESSAGES: usize, const PAYLOAD: usize, const SUBSCRIPTIONS: usize> TestableBroker
    for ConnectedLocalMemoryBroker<MESSAGES, PAYLOAD, SUBSCRIPTIONS>
{
    fn install_coordinator(&self, coordinator: Coordinator) {
        self.bus.harness().install(coordinator);
    }

    fn inject(&self, message: OutgoingMessage<'_>) {
        let (name, payload, headers) = message.into_parts();
        // The harness injects strictly between connect and shutdown and settles what it
        // receives, so a refusal here is a test that outran the capacity it chose.
        if let Err(refused) = self.bus.fanout(name, payload, headers) {
            panic!("the harness injected into {name:?} and the bus refused it: {refused}");
        }
    }

    fn published(&self, name: &str) -> Vec<RawMessage> {
        self.bus.harness().published(name)
    }

    fn backlog(&self) -> Backlog {
        Backlog::Missed
    }
}

#[cfg(test)]
mod tests {
    use alloc::vec::Vec;
    use core::pin::pin;

    use embassy_futures::block_on;
    use futures::{FutureExt, StreamExt};
    use ruststream_core::{
        ConnectedBroker, Headers, IncomingMessage, OutgoingMessage, PublishMessageError, Publisher,
        Settle, Subscribe, Subscriber,
    };
    use ruststream_runtime::testing::{Backlog, Coordinator, InProcess, TestableBroker};

    use super::*;
    use crate::local::LocalMemoryBus;
    use crate::local::LocalMemoryError;
    use crate::local::drive::waiting;

    fn reading<'a>(name: &'a str, payload: &'a [u8]) -> OutgoingMessage<'a, &'a [u8]> {
        OutgoingMessage::with_payload(name, payload)
    }

    /// A coordinator installed on the bus, counting its deliveries in flight as `TestApp` does.
    fn counted<Conn: TestableBroker>(connected: &Conn) -> Coordinator {
        let coordinator = Coordinator::new(usize::MAX);
        connected.install_coordinator(coordinator.clone());
        coordinator
    }

    fn quiescent(coordinator: &Coordinator) -> bool {
        coordinator.drive().now_or_never().is_some()
    }

    #[test]
    fn an_injected_message_reaches_the_subscription_and_the_publish_log() {
        static BUS: LocalMemoryBus<4> = LocalMemoryBus::new();
        block_on(async {
            let connected = LocalMemoryBroker::new(&BUS)
                .connect_in_process()
                .await
                .unwrap();
            let mut readings = connected.subscribe("temp").await.unwrap();
            let mut headers = Headers::new();
            headers.insert("trace", b"a1").unwrap();
            connected.inject(OutgoingMessage::new("temp", b"21".as_slice()).with_headers(&headers));

            let delivery = waiting(&mut pin!(readings.stream())).unwrap();
            assert_eq!(delivery.payload(), b"21");
            let logged = connected.published("temp");
            assert_eq!(logged.len(), 1);
            assert_eq!(logged[0].payload(), b"21");
            assert_eq!(logged[0].headers().get_str("trace"), Some("a1"));
            delivery.ack().await.unwrap();
        });
    }

    #[test]
    fn the_publish_log_holds_own_publishes_and_injections_in_order_and_no_refused_one() {
        static BUS: LocalMemoryBus<2> = LocalMemoryBus::new();
        block_on(async {
            let connected = LocalMemoryBroker::new(&BUS)
                .connect_in_process()
                .await
                .unwrap();
            let _held = connected.subscribe("temp").await.unwrap();
            let publisher = connected.publisher();
            publisher
                .publish(reading("temp", b"first"), None)
                .await
                .unwrap();
            connected.inject(OutgoingMessage::new("temp", b"second".as_slice()));
            let refused = publisher.publish(reading("temp", b"third"), None).await;
            assert_eq!(
                refused.map_err(PublishMessageError::into_error),
                Err(LocalMemoryError::Full { capacity: 2 })
            );
            publisher
                .publish(reading("humidity", b"40"), None)
                .await
                .unwrap();

            let payloads = |name| -> Vec<Vec<u8>> {
                connected
                    .published(name)
                    .iter()
                    .map(|message| message.payload().to_vec())
                    .collect()
            };
            assert_eq!(payloads("temp"), [b"first".to_vec(), b"second".to_vec()]);
            assert_eq!(payloads("humidity"), [b"40".to_vec()]);
        });
    }

    #[test]
    #[should_panic(expected = "the bus refused it")]
    fn an_injection_the_bus_refuses_fails_the_test() {
        static BUS: LocalMemoryBus<1> = LocalMemoryBus::new();
        block_on(async {
            let connected = LocalMemoryBroker::new(&BUS)
                .connect_in_process()
                .await
                .unwrap();
            let _full = connected.subscribe("temp").await.unwrap();
            connected.inject(OutgoingMessage::new("temp", b"21".as_slice()));
            connected.inject(OutgoingMessage::new("temp", b"22".as_slice()));
        });
    }

    #[test]
    fn a_new_in_process_connect_starts_a_new_run_with_an_empty_log() {
        static BUS: LocalMemoryBus<2> = LocalMemoryBus::new();
        block_on(async {
            let first = LocalMemoryBroker::new(&BUS)
                .connect_in_process()
                .await
                .unwrap();
            let _held = first.subscribe("temp").await.unwrap();
            first.inject(OutgoingMessage::new("temp", b"21".as_slice()));
            assert_eq!(first.published("temp").len(), 1);
            first.shutdown().await.unwrap();

            let second = LocalMemoryBroker::new(&BUS)
                .connect_in_process()
                .await
                .unwrap();
            assert!(second.published("temp").is_empty());
        });
    }

    #[test]
    fn the_bus_declares_the_defaults_of_a_publish_subscribe_transport() {
        static BUS: LocalMemoryBus<2> = LocalMemoryBus::new();
        block_on(async {
            let connected = LocalMemoryBroker::new(&BUS)
                .connect_in_process()
                .await
                .unwrap();
            assert_eq!(connected.backlog(), Backlog::Missed);
            assert_eq!(connected.evicted("temp"), 0);
            assert_eq!(connected.routes("temp", &["humidity", "temp"]), [1]);
        });
    }

    #[test]
    fn a_delivery_is_counted_in_flight_until_it_is_settled() {
        static BUS: LocalMemoryBus<4> = LocalMemoryBus::new();
        block_on(async {
            let connected = LocalMemoryBroker::new(&BUS)
                .connect_in_process()
                .await
                .unwrap();
            let mut readings = connected.subscribe("temp").await.unwrap();
            let coordinator = counted(&connected);
            connected
                .publisher()
                .publish(reading("temp", b"21"), None)
                .await
                .unwrap();
            assert!(
                !quiescent(&coordinator),
                "an enqueued delivery was not counted"
            );

            let delivery = waiting(&mut pin!(readings.stream())).unwrap();
            assert!(
                !quiescent(&coordinator),
                "a delivery in hand was released early"
            );
            delivery.ack().await.unwrap();
            assert!(
                quiescent(&coordinator),
                "a settled delivery stays counted in flight"
            );
        });
    }

    #[test]
    fn a_requeue_counts_the_delivery_again() {
        static BUS: LocalMemoryBus<4> = LocalMemoryBus::new();
        block_on(async {
            let connected = LocalMemoryBroker::new(&BUS)
                .connect_in_process()
                .await
                .unwrap();
            let mut readings = connected.subscribe("temp").await.unwrap();
            let coordinator = counted(&connected);
            connected
                .publisher()
                .publish(reading("temp", b"21"), None)
                .await
                .unwrap();

            let delivery = waiting(&mut pin!(readings.stream())).unwrap();
            delivery.nack(true).await.unwrap();
            assert!(
                !quiescent(&coordinator),
                "a requeued delivery is not in flight again"
            );
            let again = waiting(&mut pin!(readings.stream())).unwrap();
            again.ack().await.unwrap();
            assert!(quiescent(&coordinator));
        });
    }

    #[test]
    fn a_publish_no_subscription_receives_is_not_counted() {
        static BUS: LocalMemoryBus<4> = LocalMemoryBus::new();
        block_on(async {
            let connected = LocalMemoryBroker::new(&BUS)
                .connect_in_process()
                .await
                .unwrap();
            let coordinator = counted(&connected);
            connected
                .publisher()
                .publish(reading("temp", b"21"), None)
                .await
                .unwrap();
            assert!(quiescent(&coordinator));
        });
    }

    #[test]
    fn dropping_the_subscriber_releases_what_waits_in_it() {
        static BUS: LocalMemoryBus<4> = LocalMemoryBus::new();
        block_on(async {
            let connected = LocalMemoryBroker::new(&BUS)
                .connect_in_process()
                .await
                .unwrap();
            let readings = connected.subscribe("temp").await.unwrap();
            let coordinator = counted(&connected);
            connected
                .publisher()
                .publish(reading("temp", b"21"), None)
                .await
                .unwrap();
            assert!(!quiescent(&coordinator));
            drop(readings);
            assert!(
                quiescent(&coordinator),
                "a delivery left waiting in a dropped subscription stays counted in flight"
            );
        });
    }

    #[test]
    fn a_requeue_after_the_subscriber_left_lands_nowhere() {
        static BUS: LocalMemoryBus<4> = LocalMemoryBus::new();
        block_on(async {
            let connected = LocalMemoryBroker::new(&BUS)
                .connect_in_process()
                .await
                .unwrap();
            let mut readings = connected.subscribe("temp").await.unwrap();
            let coordinator = counted(&connected);
            connected
                .publisher()
                .publish(reading("temp", b"21"), None)
                .await
                .unwrap();
            let delivery = waiting(&mut pin!(readings.stream())).unwrap();
            drop(readings);
            delivery.nack(true).await.unwrap();
            assert!(
                quiescent(&coordinator),
                "a requeue into a subscription nobody reads stays counted in flight"
            );
        });
    }

    #[test]
    fn a_reaction_that_never_settles_holds_the_deliveries_back() {
        static BUS: LocalMemoryBus<4> = LocalMemoryBus::new();
        block_on(async {
            let connected = LocalMemoryBroker::new(&BUS)
                .connect_in_process()
                .await
                .unwrap();
            let mut readings = connected.subscribe("temp").await.unwrap();
            let coordinator = Coordinator::new(1);
            connected.install_coordinator(coordinator.clone());
            let publisher = connected.publisher();
            publisher
                .publish(reading("temp", b"1"), None)
                .await
                .unwrap();
            publisher
                .publish(reading("temp", b"2"), None)
                .await
                .unwrap();

            let first = waiting(&mut pin!(readings.stream())).unwrap();
            first.ack().await.unwrap();
            // One delivery was processed and one still waits: the step budget of one is spent.
            assert!(pin!(readings.stream()).next().now_or_never().is_none());

            coordinator.begin_step();
            let second = waiting(&mut pin!(readings.stream())).unwrap();
            assert_eq!(second.payload(), b"2");
        });
    }
}
