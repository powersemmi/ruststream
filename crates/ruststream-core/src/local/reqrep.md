The local form of [`RequestReply`]: native request-reply on the publisher handle, for a client
whose types are not `Send`.

Every [`RequestReply`] is a `LocalRequestReply` through a blanket impl.

# Examples

```
# use std::cell::{Cell, RefCell};
# use std::collections::VecDeque;
# use std::convert::Infallible;
# use std::rc::Rc;
# use std::task::Poll;
#
# use futures::{Stream, stream};
# use std::time::Duration;
#
use ruststream::{HeaderMap, LocalRequestReply, OutgoingFor, Take};
# use ruststream::{AckError, IncomingMessage, LocalSettle, LocalPublisher};
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
# struct QueuePublisher {
#     client: Rc<Client>,
# }
#
# impl LocalPublisher for QueuePublisher {
#     type Payload = Take;
#     type Error = Infallible;
#     type Options = ();
#
#     async fn publish(
#         &self,
#         msg: OutgoingFor<'_, Take>,
#         _options: Option<&()>,
#     ) -> Result<(), Infallible> {
#         self.client.queue.borrow_mut().push_back(msg.into_payload().to_vec());
#         Ok(())
#     }
# }

impl LocalRequestReply for QueuePublisher {
    type Reply = Delivery;

    async fn request(
        &self,
        msg: OutgoingFor<'_, Take>,
        _timeout: Duration,
    ) -> Result<Delivery, Infallible> {
        // The client in this example answers every request with its payload.
        Ok(Delivery {
            payload: msg.into_payload().to_vec(),
            headers: HeaderMap::new(),
            client: Rc::clone(&self.client),
        })
    }
}
```
