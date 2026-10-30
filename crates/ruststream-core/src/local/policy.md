The local form of [`PublishPolicy`]: a publish declaration that pairs into a live publisher on a
connected broker whose types are not `Send`.

The items are those of [`PublishPolicy`] without the `Send` bound on the pairing future. Every
[`PublishPolicy`] is a `LocalPublishPolicy` through a blanket impl.

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
use ruststream::{LocalPublishPolicy, PairError};
# use ruststream::{LocalConnectedBroker, LocalPublisher, OutgoingFor, Take};
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

/// The plain publish declaration: holds no connection, so it is built anywhere.
#[derive(Debug, Clone, Default)]
pub struct Publish;

impl LocalPublishPolicy<QueueConnected> for Publish {
    type Live = QueuePublisher;

    async fn pair(self, connected: &QueueConnected) -> Result<QueuePublisher, PairError> {
        Ok(QueuePublisher {
            client: Rc::clone(&connected.client),
        })
    }
}
```
