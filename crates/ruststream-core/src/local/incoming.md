The local form of [`IncomingMessage`]: one delivery and its settlement, with futures that are not
`Send`.

The items are those of [`IncomingMessage`] without the `Send` and `Sync` bounds, the defaulted
methods included. Every [`IncomingMessage`] is a `LocalIncomingMessage` through a blanket impl.

# Examples

```
use std::cell::Cell;
use std::rc::Rc;

use ruststream::{AckError, HeaderMap, LocalIncomingMessage};

/// The client's settlement counters, shared by every delivery through `Rc`.
#[derive(Default)]
struct Client {
    acked: Cell<u64>,
    requeued: Cell<u64>,
}

pub struct Delivery {
    payload: Vec<u8>,
    headers: HeaderMap,
    attempt: u64,
    client: Rc<Client>,
}

impl LocalIncomingMessage for Delivery {
    fn payload(&self) -> &[u8] {
        &self.payload
    }

    fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    fn redelivery_count(&self) -> Option<u64> {
        Some(self.attempt)
    }

    async fn ack(self) -> Result<(), AckError> {
        self.client.acked.set(self.client.acked.get() + 1);
        Ok(())
    }

    async fn nack(self, requeue: bool) -> Result<(), AckError> {
        if requeue {
            self.client.requeued.set(self.client.requeued.get() + 1);
        }
        Ok(())
    }
}
```
