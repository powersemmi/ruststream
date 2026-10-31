The local form of [`Seekable`]: a subscriber that hands out a handle repositioning it in a
replayable log, for a client whose types are not `Send`.

Every [`Seekable`] is a `LocalSeekable` through a blanket impl.

# Examples

```
# use std::cell::{Cell, RefCell};
# use std::convert::Infallible;
# use std::error::Error;
# use std::rc::Rc;
# use std::task::Poll;
#
# use futures::{Stream, StreamExt, stream};
#
use ruststream::LocalSeekable;
# use ruststream::{AckError, HeaderMap, IncomingMessage, LocalSettle, LocalSeeker, LocalSubscriber};
# /// The client's replayable log: shared by `Rc`, so nothing built on it is `Send`.
# #[derive(Default)]
# struct Client {
#     log: RefCell<Vec<Vec<u8>>>,
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
# /// Reads the log from `next`, the offset its seeker moves.
# struct QueueSubscriber {
#     client: Rc<Client>,
#     next: Rc<Cell<u64>>,
# }
#
# impl LocalSubscriber for QueueSubscriber {
#     type Message = Delivery;
#     type Error = Infallible;
#
#     fn stream(&mut self) -> impl Stream<Item = Result<Delivery, Infallible>> + '_ {
#         stream::poll_fn(move |_| {
#             let offset = self.next.get();
#             let payload = usize::try_from(offset)
#                 .ok()
#                 .and_then(|index| self.client.log.borrow().get(index).cloned());
#             Poll::Ready(payload.map(|payload| {
#                 self.next.set(offset + 1);
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
# /// The subscription's read position, shared with it by `Rc`.
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
        // The cursor moves the position this subscriber reads from.
        Cursor {
            next: Rc::clone(&self.next),
        }
    }
}
# fn main() -> Result<(), Box<dyn Error>> {
# futures::executor::block_on(async {
#     let client = Rc::new(Client::default());
#     client.log.borrow_mut().extend([b"a".to_vec(), b"b".to_vec()]);
#     let mut subscriber = QueueSubscriber { client, next: Rc::default() };
#     let cursor = subscriber.seeker();
#     let first = subscriber.stream().next().await.ok_or("no delivery")??;
#     assert_eq!(first.payload(), b"a");
#     cursor.seek(0).await?;
#     let again = subscriber.stream().next().await.ok_or("no delivery")??;
#     assert_eq!(again.payload(), b"a");
#     Ok(())
# })
# }
```
