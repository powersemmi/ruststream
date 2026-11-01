The local form of [`BatchSubscriber`]: a subscriber that hands out whole batches, for a client
whose types are not `Send`.

Every [`BatchSubscriber`] is a `LocalBatchSubscriber` through a blanket impl.

# Examples

```
# use std::cell::{Cell, RefCell};
# use std::collections::VecDeque;
# use std::convert::Infallible;
# use std::rc::Rc;
# use std::task::Poll;
#
# use futures::{Stream, stream};
# use std::num::NonZeroUsize;
#
use ruststream::{HeaderMap, LocalBatchSubscriber};
# use ruststream::{AckError, IncomingMessage, LocalSettle, LocalSubscriber};
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

impl LocalBatchSubscriber for QueueSubscriber {
    type Batch = Vec<Delivery>;

    fn batches(
        &mut self,
        size: NonZeroUsize,
    ) -> impl Stream<Item = Result<Vec<Delivery>, Infallible>> + '_ {
        stream::poll_fn(move |_| {
            let mut queue = self.client.queue.borrow_mut();
            let take = size.get().min(queue.len());
            if take == 0 {
                return Poll::Ready(None);
            }
            let batch = queue
                .drain(..take)
                .map(|payload| Delivery {
                    payload,
                    headers: HeaderMap::new(),
                    client: Rc::clone(&self.client),
                })
                .collect();
            Poll::Ready(Some(Ok(batch)))
        })
    }
}
```
