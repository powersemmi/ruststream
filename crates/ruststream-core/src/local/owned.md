The local form of [`OwnedTransactions`]: a publisher that opens many transactions at once, each
with its own buffer, for a client whose types are not `Send`.

Every [`OwnedTransactions`] is a `LocalOwnedTransactions` through a blanket impl.

# Examples

```
use std::cell::RefCell;
use std::collections::VecDeque;
use std::convert::Infallible;
use std::rc::Rc;

use ruststream::LocalOwnedTransactions;
# use ruststream::{LocalPublisher, LocalTransaction, OutgoingFor, Take};

# #[derive(Default)]
# struct Client {
#     queue: RefCell<VecDeque<Vec<u8>>>,
# }
#
# pub struct QueuePublisher {
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
# pub struct QueueTransaction {
#     client: Rc<Client>,
#     buffered: Vec<Vec<u8>>,
# }
#
# impl LocalTransaction for QueueTransaction {
#     type Payload = Take;
#     type Error = Infallible;
#     type Options = ();
#
#     async fn publish(
#         &mut self,
#         msg: OutgoingFor<'_, Take>,
#         _options: Option<&()>,
#     ) -> Result<(), Infallible> {
#         self.buffered.push(msg.into_payload().to_vec());
#         Ok(())
#     }
#
#     async fn commit(self) -> Result<(), Infallible> {
#         self.client.queue.borrow_mut().extend(self.buffered);
#         Ok(())
#     }
#
#     async fn abort(self) -> Result<(), Infallible> {
#         Ok(())
#     }
# }
impl LocalOwnedTransactions for QueuePublisher {
    type Transaction = QueueTransaction;

    async fn transaction(&self) -> Result<QueueTransaction, Infallible> {
        Ok(QueueTransaction {
            client: Rc::clone(&self.client),
            buffered: Vec::new(),
        })
    }
}
```
