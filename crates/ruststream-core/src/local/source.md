The local form of [`SubscriptionSource`]: a description of one subscription, resolved against a
connected broker whose types are not `Send`.

The items are those of [`SubscriptionSource`] without the `Send` bound on the subscribe future,
and [`Subscriber`](Self::Subscriber) is a [`LocalSubscriber`]. Every [`SubscriptionSource`] is a
`LocalSubscriptionSource` through a blanket impl.

# Examples

```
# use std::cell::{Cell, RefCell};
# use std::collections::VecDeque;
# use std::convert::Infallible;
# use std::rc::Rc;
# use std::task::Poll;
#
# use futures::{Stream, stream};
#
use ruststream::{
    AckError, BrokerMoves, HeaderMap, LocalConnectedBroker, LocalIncomingMessage, LocalSubscriber,
    LocalSubscriptionSource,
};
# /// The client's state: shared by `Rc`, so nothing built on it is `Send`.
# #[derive(Default)]
# struct Client {
#     queue: RefCell<VecDeque<Vec<u8>>>,
#     acked: Cell<u64>,
# }
# struct Delivery {
#     payload: Vec<u8>,
#     headers: HeaderMap,
#     client: Rc<Client>,
# }
#
# impl LocalIncomingMessage for Delivery {
#     fn payload(&self) -> &[u8] {
#         &self.payload
#     }
#
#     fn headers(&self) -> &HeaderMap {
#         &self.headers
#     }
#
#     async fn ack(self) -> Result<(), AckError> {
#         self.client.acked.set(self.client.acked.get() + 1);
#         Ok(())
#     }
#
#     async fn nack(self, _requeue: bool) -> Result<(), AckError> {
#         Ok(())
#     }
# }
# struct QueueSubscriber {
#     client: Rc<Client>,
# }
#
# impl LocalSubscriber for QueueSubscriber {
#     type Message = Delivery;
#     type Error = Infallible;
#
#     fn stream(&mut self) -> impl Stream<Item = Result<Delivery, Infallible>> + '_ {
#         stream::poll_fn(move |_| {
#             let next = self.client.queue.borrow_mut().pop_front();
#             Poll::Ready(next.map(|payload| {
#                 Ok(Delivery {
#                     payload,
#                     headers: HeaderMap::new(),
#                     client: Rc::clone(&self.client),
#                 })
#             }))
#         })
#     }
# }
# struct QueueConnected {
#     client: Rc<Client>,
# }
#
# impl LocalConnectedBroker for QueueConnected {
#     type Error = Infallible;
#     type Closed = ();
#
#     async fn shutdown(self) -> Result<(), Infallible> {
#         Ok(())
#     }
# }

/// A queue subscription with a setting of the broker's own.
#[derive(Debug, Clone)]
pub struct Queue {
    name: String,
    prefetch: u16,
}

impl LocalSubscriptionSource<QueueConnected> for Queue {
    type Subscriber = QueueSubscriber;
    // The client redelivers a requeued message itself.
    type Copies = BrokerMoves;

    fn name(&self) -> &str {
        &self.name
    }

    async fn subscribe(self, connected: &QueueConnected) -> Result<QueueSubscriber, Infallible> {
        let _ = self.prefetch;
        Ok(QueueSubscriber {
            client: Rc::clone(&connected.client),
        })
    }
}
```
