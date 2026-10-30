The local form of [`Seekable`]: a subscriber that hands out a handle repositioning it in a
replayable log, for a client whose types are not `Send`.

Every [`Seekable`] is a `LocalSeekable` through a blanket impl.

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
use ruststream::LocalSeekable;
# use ruststream::{AckError, HeaderMap, LocalIncomingMessage, LocalSeeker, LocalSubscriber};
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
#
# #[derive(Clone)]
# pub struct Cursor {
#     next: Rc<Cell<u64>>,
# }
#
# impl LocalSeeker for Cursor {
#     type Position = u64;
#     type Error = Infallible;
#
#     async fn seek(&self, to: u64) -> Result<(), Infallible> {
#         self.next.set(to);
#         Ok(())
#     }
# }
impl LocalSeekable for QueueSubscriber {
    type Seeker = Cursor;

    fn seeker(&self) -> Cursor {
        Cursor {
            next: Rc::new(Cell::new(0)),
        }
    }
}
```
