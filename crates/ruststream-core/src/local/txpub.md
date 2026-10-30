The local form of [`TransactionalPublisher`]: begin, commit and abort around publishes on the
publisher handle, for a client whose types are not `Send`.

Every [`TransactionalPublisher`] is a `LocalTransactionalPublisher` through a blanket impl.

# Examples

```
use std::cell::RefCell;
use std::collections::VecDeque;
use std::error::Error;
use std::fmt;
use std::rc::Rc;

use ruststream::LocalTransactionalPublisher;
# use ruststream::{BytesMut, LocalPublisher, OutgoingFor, OutgoingMessage, Take};
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
#     type Error = TransactionError;
#     type Options = ();
#
#     async fn publish(
#         &self,
#         msg: OutgoingFor<'_, Take>,
#         _options: Option<&()>,
#     ) -> Result<(), TransactionError> {
#         let payload = msg.into_payload().to_vec();
#         match self.client.open.borrow_mut().as_mut() {
#             Some(open) => open.push(payload),
#             None => self.client.queue.borrow_mut().push_back(payload),
#         }
#         Ok(())
#     }
# }

/// A begin while a transaction is open, or a commit or abort with none open.
#[derive(Debug)]
pub enum TransactionError {
    AlreadyOpen,
    NotOpen,
}

impl fmt::Display for TransactionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AlreadyOpen => f.write_str("a transaction is already open"),
            Self::NotOpen => f.write_str("no transaction is open"),
        }
    }
}

impl Error for TransactionError {}

impl LocalTransactionalPublisher for QueuePublisher {
    async fn begin_transaction(&self) -> Result<(), TransactionError> {
        let mut open = self.client.open.borrow_mut();
        if open.is_some() {
            // The open transaction keeps its messages.
            return Err(TransactionError::AlreadyOpen);
        }
        *open = Some(Vec::new());
        Ok(())
    }

    async fn commit(&self) -> Result<(), TransactionError> {
        let committed = self.client.open.borrow_mut().take();
        let committed = committed.ok_or(TransactionError::NotOpen)?;
        self.client.queue.borrow_mut().extend(committed);
        Ok(())
    }

    async fn abort(&self) -> Result<(), TransactionError> {
        let aborted = self.client.open.borrow_mut().take();
        aborted.map(drop).ok_or(TransactionError::NotOpen)
    }
}
# fn main() -> Result<(), Box<dyn Error>> {
# futures::executor::block_on(async {
#     let client = Rc::new(Client::default());
#     let publisher = QueuePublisher { client: Rc::clone(&client) };
#     let order = || OutgoingMessage::produced("orders", BytesMut::from(&b"order"[..]));
#     assert!(publisher.commit().await.is_err());
#     assert!(publisher.abort().await.is_err());
#     publisher.begin_transaction().await?;
#     publisher.publish(order(), None).await?;
#     assert!(publisher.begin_transaction().await.is_err());
#     publisher.commit().await?;
#     assert_eq!(client.queue.borrow().len(), 1);
#     Ok(())
# })
# }
```
