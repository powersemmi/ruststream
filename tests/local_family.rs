//! The `Local*` contract family: a broker whose types are not `Send` implements it, every Send
//! broker implements it through the blanket impls, and a body written once over it is spawned on
//! tokio's multi-thread pool when the broker is a Send one.

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, VecDeque};
use std::convert::Infallible;
use std::error::Error;
use std::future::ready;
use std::pin::pin;
use std::rc::Rc;
use std::task::Poll;

use futures::{Stream, StreamExt, stream};
use ruststream::memory::MemoryBroker;
use ruststream::{
    AckError, Broker, BrokerMoves, BytesMut, DefaultPublish, HeaderMap, LocalBroker,
    LocalConnectedBroker, LocalDefaultPublish, LocalIncomingMessage, LocalPublishPolicy,
    LocalPublisher, LocalSubscribe, LocalSubscriber, OutgoingFor, OutgoingMessage, PairError,
    PublishPolicy, Publisher, Subscribe, Take,
};
use tokio::task::{self, LocalSet};

type BoxError = Box<dyn Error + Send + Sync>;

const ORDERS: &[&[u8]] = &[b"order-1", b"order-2", b"order-3"];

/// The live form a broker's default policy pairs into, read through the local family.
type LocalLive<C> = <<C as LocalDefaultPublish>::Policy as LocalPublishPolicy<C>>::Live;

/// What a runtime over the local family runs: connect, subscribe, publish, receive, settle,
/// shut down. Written once, it accepts a `!Send` broker and a Send one alike.
async fn round_trip<B>(broker: B, orders: &[&[u8]]) -> Result<Vec<Vec<u8>>, BoxError>
where
    B: LocalBroker,
    B::Connected: LocalSubscribe + LocalDefaultPublish,
    LocalLive<B::Connected>: LocalPublisher<Payload = Take>,
{
    let connected = broker.connect().await?;
    let mut subscriber = connected.subscribe("orders").await?;
    let publisher = <B::Connected as LocalDefaultPublish>::Policy::default()
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

/// A tokio-only spawn site: generic, and bounded on the Send family alone. The local body it
/// spawns is `Send` because every local method of `B` resolves to the Send trait's method, whose
/// future carries `+ Send`.
fn spawn_on_pool<B>(broker: B) -> task::JoinHandle<Result<Vec<Vec<u8>>, BoxError>>
where
    B: Broker + 'static,
    B::Connected: Subscribe + DefaultPublish,
    <<B::Connected as DefaultPublish>::Policy as PublishPolicy<B::Connected>>::Live:
        Publisher<Payload = Take>,
{
    tokio::spawn(round_trip(broker, ORDERS))
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

#[tokio::test(flavor = "current_thread")]
async fn memory_broker_meets_the_local_bounds_through_the_blanket_impls() -> Result<(), BoxError> {
    let received = round_trip(MemoryBroker::new(), ORDERS).await?;

    assert_eq!(received, expected());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_local_body_of_a_send_broker_spawns_on_the_multi_thread_pool() -> Result<(), BoxError> {
    let received = spawn_on_pool(MemoryBroker::new()).await??;

    assert_eq!(received, expected());
    Ok(())
}

// A broker over a client that keeps its state in `Rc` and `RefCell`: none of its types is `Send`,
// so it implements the local family and nothing else.

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

struct RcPublisher {
    state: Rc<State>,
}

// The broker is not `Send` on purpose: a future that hands out its types cannot be either.
#[allow(clippy::future_not_send)]
impl LocalBroker for RcBroker {
    type Error = Infallible;
    type Connected = RcConnected;

    fn connect(self) -> impl Future<Output = Result<RcConnected, Infallible>> {
        ready(Ok(RcConnected { state: self.state }))
    }
}

impl LocalConnectedBroker for RcConnected {
    type Error = Infallible;
    type Closed = ();

    fn shutdown(self) -> impl Future<Output = Result<(), Infallible>> {
        ready(Ok(()))
    }
}

// The broker is not `Send` on purpose: a future that hands out its types cannot be either.
#[allow(clippy::future_not_send)]
impl LocalSubscribe for RcConnected {
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

impl LocalSubscriber for RcSubscriber {
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

impl LocalIncomingMessage for RcDelivery {
    fn payload(&self) -> &[u8] {
        &self.payload
    }

    fn headers(&self) -> &HeaderMap {
        &self.headers
    }

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
impl LocalPublishPolicy<RcConnected> for RcPublish {
    type Live = RcPublisher;

    fn pair(self, connected: &RcConnected) -> impl Future<Output = Result<RcPublisher, PairError>> {
        ready(Ok(RcPublisher {
            state: Rc::clone(&connected.state),
        }))
    }
}

impl LocalDefaultPublish for RcConnected {
    type Policy = RcPublish;
}

impl LocalPublisher for RcPublisher {
    type Payload = Take;
    type Error = Infallible;
    type Options = ();

    fn publish(
        &self,
        msg: OutgoingFor<'_, Take>,
        _options: Option<&()>,
    ) -> impl Future<Output = Result<(), Infallible>> {
        let (name, payload, _headers) = msg.into_parts();
        if let Some(queue) = self.state.queues.borrow_mut().get_mut(name) {
            queue.push_back(payload.to_vec());
        }
        ready(Ok(()))
    }
}
