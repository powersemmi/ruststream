//! The contract traits implemented by a broker whose types are not `Send`, and a body written
//! once over them running on the test's own thread. Built where the Tokio runner is off, because
//! it asks every broker for `Send`.
#![cfg(not(feature = "tokio"))]

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, VecDeque};
use std::convert::Infallible;
use std::error::Error;
use std::future::ready;
use std::pin::pin;
use std::rc::Rc;
use std::task::Poll;

use futures::{Stream, StreamExt, stream};
use ruststream::PublishOutcome;
#[cfg(ruststream_alloc)]
use ruststream::memory::local::LocalMemoryBroker;
use ruststream::{
    AckError, Broker, BrokerMoves, BytesMut, ConnectedBroker, DefaultPublish, HeaderMap,
    IncomingMessage, OutgoingFor, OutgoingMessage, PairError, PublishPolicy, Publisher, Settle,
    Subscribe, Subscriber, Take,
};
use tokio::task::{self, LocalSet};

type BoxError = Box<dyn Error + Send + Sync>;

const ORDERS: &[&[u8]] = &[b"order-1", b"order-2", b"order-3"];

/// The live form a broker's default policy pairs into, read through the contract traits.
type LocalLive<C> = <<C as DefaultPublish>::Policy as PublishPolicy<C>>::Live;

/// What a runtime over the contract traits runs: connect, subscribe, publish, receive, settle,
/// shut down. Written once, it accepts a `!Send` broker and a Send one alike.
async fn round_trip<B>(broker: B, orders: &[&[u8]]) -> Result<Vec<Vec<u8>>, BoxError>
where
    B: Broker,
    B::Connected: Subscribe + DefaultPublish,
    LocalLive<B::Connected>: Publisher<Payload = Take>,
{
    let connected = broker.connect().await?;
    let mut subscriber = connected.subscribe("orders").await?;
    let publisher = <B::Connected as DefaultPublish>::Policy::default()
        .pair(&connected)
        .await?;
    for order in orders {
        let msg = OutgoingMessage::produced("orders", BytesMut::from(*order));
        publisher.publish(msg, None).await?;
    }

    let mut received = Vec::new();
    {
        let mut deliveries = pin!(subscriber.stream());
        while received.len() < orders.len() {
            let delivery = deliveries.next().await.ok_or("the stream ended early")??;
            received.push(delivery.payload().to_vec());
            delivery.ack().await?;
        }
    }
    connected.shutdown().await?;
    Ok(received)
}

fn expected() -> Vec<Vec<u8>> {
    ORDERS.iter().map(|order| order.to_vec()).collect()
}

#[tokio::test(flavor = "current_thread")]
async fn a_broker_that_is_not_send_runs_the_local_body_on_a_local_set() -> Result<(), BoxError> {
    let state = Rc::new(State::default());
    let broker = RcBroker {
        state: Rc::clone(&state),
    };

    let received = LocalSet::new()
        .run_until(async move { task::spawn_local(round_trip(broker, ORDERS)).await })
        .await??;

    assert_eq!(received, expected());
    assert_eq!(state.acked.get(), ORDERS.len());
    Ok(())
}

#[cfg(ruststream_alloc)]
#[tokio::test(flavor = "current_thread")]
async fn local_bus_runs_the_body_written_for_a_broker_that_is_not_send() -> Result<(), BoxError> {
    let received = round_trip(LocalMemoryBroker::<4>::new(), ORDERS).await?;

    assert_eq!(received, expected());
    Ok(())
}

// A broker over a client that keeps its state in `Rc` and `RefCell`: none of its types is `Send`,
// so it implements the contract traits without asking for `Send`.

#[derive(Default)]
struct State {
    queues: RefCell<HashMap<String, VecDeque<Vec<u8>>>>,
    acked: Cell<usize>,
}

struct RcBroker {
    state: Rc<State>,
}

struct RcConnected {
    state: Rc<State>,
}

struct RcSubscriber {
    name: String,
    state: Rc<State>,
}

struct RcDelivery {
    payload: Vec<u8>,
    headers: HeaderMap,
    state: Rc<State>,
}

#[derive(Default)]
struct RcPublish;

#[derive(Clone)]
struct RcPublisher {
    state: Rc<State>,
}

// The broker is not `Send` on purpose: a future that hands out its types cannot be either.
#[allow(clippy::future_not_send)]
impl Broker for RcBroker {
    type Error = Infallible;
    type Connected = RcConnected;

    fn connect(self) -> impl Future<Output = Result<RcConnected, Infallible>> {
        ready(Ok(RcConnected { state: self.state }))
    }
}

impl ConnectedBroker for RcConnected {
    type Error = Infallible;
    type Closed = ();

    fn shutdown(self) -> impl Future<Output = Result<(), Infallible>> {
        ready(Ok(()))
    }
}

// The broker is not `Send` on purpose: a future that hands out its types cannot be either.
#[allow(clippy::future_not_send)]
impl Subscribe for RcConnected {
    type Subscriber = RcSubscriber;
    type Copies = BrokerMoves;

    fn subscribe(&self, name: &str) -> impl Future<Output = Result<RcSubscriber, Infallible>> {
        self.state
            .queues
            .borrow_mut()
            .entry(name.to_owned())
            .or_default();
        ready(Ok(RcSubscriber {
            name: name.to_owned(),
            state: Rc::clone(&self.state),
        }))
    }
}

impl Subscriber for RcSubscriber {
    type Message = RcDelivery;
    type Error = Infallible;

    fn stream(&mut self) -> impl Stream<Item = Result<RcDelivery, Infallible>> + '_ {
        stream::poll_fn(move |_| {
            let next = self
                .state
                .queues
                .borrow_mut()
                .get_mut(&self.name)
                .and_then(VecDeque::pop_front);
            Poll::Ready(next.map(|payload| {
                Ok(RcDelivery {
                    payload,
                    headers: HeaderMap::new(),
                    state: Rc::clone(&self.state),
                })
            }))
        })
    }
}

impl IncomingMessage for RcDelivery {
    fn payload(&self) -> &[u8] {
        &self.payload
    }

    fn headers(&self) -> &HeaderMap {
        &self.headers
    }
}

impl Settle for RcDelivery {
    fn ack(self) -> impl Future<Output = Result<(), AckError>> {
        self.state.acked.set(self.state.acked.get() + 1);
        ready(Ok(()))
    }

    fn nack(self, _requeue: bool) -> impl Future<Output = Result<(), AckError>> {
        ready(Ok(()))
    }
}

// The broker is not `Send` on purpose: a future that hands out its types cannot be either.
#[allow(clippy::future_not_send)]
impl PublishPolicy<RcConnected> for RcPublish {
    type Live = RcPublisher;

    fn pair(self, connected: &RcConnected) -> impl Future<Output = Result<RcPublisher, PairError>> {
        ready(Ok(RcPublisher {
            state: Rc::clone(&connected.state),
        }))
    }
}

impl DefaultPublish for RcConnected {
    type Policy = RcPublish;
}

impl Publisher for RcPublisher {
    type Payload = Take;
    type Error = Infallible;
    type Options = ();

    fn publish<'msg>(
        &self,
        msg: OutgoingFor<'msg, Take>,
        _options: Option<&()>,
    ) -> impl Future<Output = PublishOutcome<'msg, Infallible, Take>> {
        let (name, payload, _headers) = msg.into_parts();
        if let Some(queue) = self.state.queues.borrow_mut().get_mut(name) {
            queue.push_back(payload.to_vec());
        }
        ready(Ok(()))
    }
}
