use std::convert::Infallible;
use std::future::{Future, ready};

use futures::{Stream, stream};
use ruststream::runtime::{AppInfo, HandlerOutcome, RustStream};
use ruststream::{
    AckError, AddressedCopies, Broker, ConnectedBroker, HeaderMap, IncomingMessage, Subscribe,
    Subscriber, subscriber,
};
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u32,
}

/// A broker whose publishers always need explicit options, so it names no default publish policy.
struct Bus;

impl Broker for Bus {
    type Error = Infallible;
    type Connected = ConnectedBus;

    fn connect(self) -> impl Future<Output = Result<Self::Connected, Self::Error>> {
        ready(Ok(ConnectedBus))
    }
}

struct ConnectedBus;

impl ConnectedBroker for ConnectedBus {
    type Error = Infallible;
    type Closed = ();

    fn shutdown(self) -> impl Future<Output = Result<(), Self::Error>> {
        ready(Ok(()))
    }
}

impl Subscribe for ConnectedBus {
    type Copies = AddressedCopies;
    type Subscriber = BusSubscriber;

    fn subscribe(&self, _name: &str) -> impl Future<Output = Result<BusSubscriber, Infallible>> {
        ready(Ok(BusSubscriber))
    }

}

struct BusSubscriber;

impl Subscriber for BusSubscriber {
    type Message = BusMessage;
    type Error = Infallible;

    fn stream(&mut self) -> impl Stream<Item = Result<BusMessage, Infallible>> + Send + '_ {
        stream::empty()
    }
}

struct BusMessage;

impl IncomingMessage for BusMessage {
    fn payload(&self) -> &[u8] {
        &[]
    }

    fn headers(&self) -> &HeaderMap {
        unreachable!("the stream is empty")
    }

    async fn ack(self) -> Result<(), AckError> {
        Ok(())
    }

    async fn nack(self, _requeue: bool) -> Result<(), AckError> {
        Ok(())
    }
}

#[subscriber("orders")]
async fn reconcile(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

// The by-name descriptor publishes its retry copies here, so every registration on it owes a
// retry publisher - and this broker names no policy to pair one from.
fn main() {
    RustStream::new(AppInfo::new("app", "0.1.0")).with_broker(Bus, |b| {
        b.include(reconcile);
    });
}
