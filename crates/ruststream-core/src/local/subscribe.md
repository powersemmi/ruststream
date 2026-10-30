The local form of [`Subscribe`]: a connected broker whose subscriptions a name alone identifies,
for a client whose types are not `Send`.

Every [`Subscribe`] is a `LocalSubscribe` through a blanket impl.

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
    AckError, BrokerMoves, HeaderMap, LocalConnectedBroker, LocalIncomingMessage, LocalSubscribe,
    LocalSubscriber,
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

impl LocalSubscribe for QueueConnected {
    type Subscriber = QueueSubscriber;
    // The client redelivers a requeued message itself.
    type Copies = BrokerMoves;

    async fn subscribe(&self, _name: &str) -> Result<QueueSubscriber, Infallible> {
        Ok(QueueSubscriber {
            client: Rc::clone(&self.client),
        })
    }
}
```
