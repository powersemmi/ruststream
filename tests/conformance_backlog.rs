//! The routing suite checks a message published before a subscription opens against what the
//! broker declares through `TestableBroker::backlog`, both ways.
//!
//! [`Declaring`] stands for a broker of either kind. Its in-process transport is the memory bus;
//! where it keeps a backlog, a subscription opened by name starts from the beginning of the bus's
//! log, the way a queue keeps what reached it before its consumer came.

#![cfg(all(feature = "testing", feature = "memory"))]

use std::future::Future;

use ruststream::conformance::harness;
use ruststream::memory::{
    ConnectedMemoryBroker, MemoryBroker, MemoryError, MemoryPosition, MemorySubscriber, Retaining,
    Retention,
};
use ruststream::testing::{Backlog, Coordinator, InProcess, TestableBroker};
use ruststream::{
    AddressedCopies, Broker, ConnectedBroker, OutgoingMessage, RawMessage, Seekable, Seeker,
    Subscribe, nonzero,
};

/// Whether the transport keeps what reaches a name before a subscription to it opens.
#[derive(Debug, Clone, Copy)]
enum Transport {
    Keeps,
    Drops,
}

/// A broker whose transport keeps or drops a backlog, declaring `declares` either way.
#[derive(Debug)]
struct Declaring {
    bus: MemoryBroker<Retaining>,
    transport: Transport,
    declares: Backlog,
}

impl Declaring {
    fn new(transport: Transport, declares: Backlog) -> Self {
        Self {
            bus: MemoryBroker::retaining(Retention::Messages(nonzero!(64))),
            transport,
            declares,
        }
    }
}

/// The connected form of [`Declaring`].
#[derive(Debug)]
struct Declared {
    bus: ConnectedMemoryBroker<Retaining>,
    transport: Transport,
    declares: Backlog,
}

impl Broker for Declaring {
    type Error = MemoryError;
    type Connected = Declared;

    async fn connect(self) -> Result<Self::Connected, Self::Error> {
        Ok(Declared {
            bus: self.bus.connect().await?,
            transport: self.transport,
            declares: self.declares,
        })
    }
}

impl InProcess for Declaring {
    fn connect_in_process(
        self,
    ) -> impl Future<Output = Result<Self::Connected, Self::Error>> + Send {
        self.connect()
    }
}

impl ConnectedBroker for Declared {
    type Error = MemoryError;
    type Closed = <ConnectedMemoryBroker<Retaining> as ConnectedBroker>::Closed;

    fn shutdown(self) -> impl Future<Output = Result<Self::Closed, Self::Error>> + Send {
        self.bus.shutdown()
    }
}

impl Subscribe for Declared {
    type Subscriber = MemorySubscriber<Retaining>;
    type Copies = AddressedCopies;

    async fn subscribe(&self, name: &str) -> Result<Self::Subscriber, Self::Error> {
        let subscriber = Subscribe::subscribe(&self.bus, name).await?;
        if matches!(self.transport, Transport::Keeps) {
            subscriber.seeker().seek(MemoryPosition::start()).await?;
        }
        Ok(subscriber)
    }
}

impl TestableBroker for Declared {
    fn install_coordinator(&self, coordinator: Coordinator) {
        self.bus.install_coordinator(coordinator);
    }

    fn inject(&self, message: OutgoingMessage<'_>) {
        self.bus.inject(message);
    }

    fn published(&self, name: &str) -> Vec<RawMessage> {
        self.bus.published(name)
    }

    fn backlog(&self) -> Backlog {
        self.declares
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_transport_that_keeps_a_backlog_passes_declaring_it() {
    harness::run_suite(|| Declaring::new(Transport::Keeps, Backlog::Delivered)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_transport_that_drops_a_backlog_passes_declaring_it() {
    harness::run_suite(|| Declaring::new(Transport::Drops, Backlog::Missed)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "declares `Backlog::Delivered`")]
async fn a_transport_that_drops_what_it_declares_kept_fails() {
    harness::run_suite(|| Declaring::new(Transport::Drops, Backlog::Delivered)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[should_panic(expected = "declares `Backlog::Missed`")]
async fn a_transport_that_keeps_what_it_declares_missed_fails() {
    harness::run_suite(|| Declaring::new(Transport::Keeps, Backlog::Missed)).await;
}
