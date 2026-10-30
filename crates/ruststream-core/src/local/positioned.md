The local form of [`Positioned`]: a delivery that reports its own position in the log, for a
client whose types are not `Send`.

Every [`Positioned`] is a `LocalPositioned` through a blanket impl.

# Examples

```
use std::rc::Rc;

use ruststream::{HeaderMap, LocalPositioned};
# use ruststream::{AckError, LocalIncomingMessage};

struct Client;

pub struct Delivery {
    offset: u64,
    payload: Vec<u8>,
    headers: HeaderMap,
    client: Rc<Client>,
}

impl LocalPositioned for Delivery {
    type Position = u64;

    fn position(&self) -> u64 {
        self.offset
    }
}
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
#         drop(self.client);
#         Ok(())
#     }
#
#     async fn nack(self, _requeue: bool) -> Result<(), AckError> {
#         Ok(())
#     }
# }
```
