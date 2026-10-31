The local form of [`Subscriber`]: a stream of deliveries whose types are not `Send`.

The items are those of [`Subscriber`] without the `Send` bounds, and [`Message`](Self::Message)
is a [`LocalSettle`]. Every [`Subscriber`] is a `LocalSubscriber` through a blanket impl.

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
use ruststream::{HeaderMap, LocalSubscriber};
# use ruststream::{AckError, IncomingMessage, LocalSettle};
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
# impl IncomingMessage for Delivery {
#     fn payload(&self) -> &[u8] {
#         &self.payload
#     }
#
#     fn headers(&self) -> &HeaderMap {
#         &self.headers
#     }
# }
#
# impl LocalSettle for Delivery {
#     async fn ack(self) -> Result<(), AckError> {
#         self.client.acked.set(self.client.acked.get() + 1);
#         Ok(())
#     }
#
#     async fn nack(self, _requeue: bool) -> Result<(), AckError> {
#         Ok(())
#     }
# }

/// A subscription over the client's queue.
struct QueueSubscriber {
    client: Rc<Client>,
}

impl LocalSubscriber for QueueSubscriber {
    type Message = Delivery;
    type Error = Infallible;

    fn stream(&mut self) -> impl Stream<Item = Result<Delivery, Infallible>> + '_ {
        stream::poll_fn(move |_| {
            // A real client registers the waker and returns `Pending` while its socket is idle.
            let next = self.client.queue.borrow_mut().pop_front();
            Poll::Ready(next.map(|payload| {
                Ok(Delivery {
                    payload,
                    headers: HeaderMap::new(),
                    client: Rc::clone(&self.client),
                })
            }))
        })
    }
}
```
