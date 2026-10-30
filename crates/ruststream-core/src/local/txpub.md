The local form of [`TransactionalPublisher`]: begin, commit and abort around publishes on the
publisher handle, for a client whose types are not `Send`.

Every [`TransactionalPublisher`] is a `LocalTransactionalPublisher` through a blanket impl.

# Examples

```
use std::cell::RefCell;
use std::collections::VecDeque;
use std::convert::Infallible;
use std::rc::Rc;

use ruststream::LocalTransactionalPublisher;
# use ruststream::{LocalPublisher, OutgoingFor, Take};
# #[derive(Default)]
# struct Client {
#     queue: RefCell<VecDeque<Vec<u8>>>,
#     open: RefCell<Option<Vec<Vec<u8>>>>,
# }
#
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
#         let payload = msg.into_payload().to_vec();
#         match self.client.open.borrow_mut().as_mut() {
#             Some(open) => open.push(payload),
#             None => self.client.queue.borrow_mut().push_back(payload),
#         }
#         Ok(())
#     }
# }

impl LocalTransactionalPublisher for QueuePublisher {
    async fn begin_transaction(&self) -> Result<(), Infallible> {
        *self.client.open.borrow_mut() = Some(Vec::new());
        Ok(())
    }

    async fn commit(&self) -> Result<(), Infallible> {
        let committed = self.client.open.borrow_mut().take().unwrap_or_default();
        self.client.queue.borrow_mut().extend(committed);
        Ok(())
    }

    async fn abort(&self) -> Result<(), Infallible> {
        self.client.open.borrow_mut().take();
        Ok(())
    }
}
```
