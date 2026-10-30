The local form of [`DefaultPublish`]: the policy a reply and a retry copy use when the mount site
names none, on a connected broker whose types are not `Send`.

Every [`DefaultPublish`] is a `LocalDefaultPublish` through a blanket impl.

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
    LocalConnectedBroker, LocalDefaultPublish, LocalPublishPolicy, LocalPublisher, OutgoingFor,
    PairError, Take,
};
# #[derive(Default)]
# struct Client {
#     queue: RefCell<VecDeque<Vec<u8>>>,
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
#
# #[derive(Debug, Clone, Default)]
# pub struct Publish;
#
# impl LocalPublishPolicy<QueueConnected> for Publish {
#     type Live = QueuePublisher;
#
#     async fn pair(self, connected: &QueueConnected) -> Result<QueuePublisher, PairError> {
#         Ok(QueuePublisher { client: Rc::clone(&connected.client) })
#     }
# }
// The plain policy is usable as it is, so it answers a reply and a retry copy.
impl LocalDefaultPublish for QueueConnected {
    type Policy = Publish;
}
```
