The local form of [`Settle`]: the settlement of a delivery whose futures are not `Send`.

The items are those of [`Settle`], the defaulted methods included, without `Send` on the futures.
Every [`Settle`] is a `LocalSettle` through a blanket impl. What the delivery carries is
[`IncomingMessage`], which both forms share.

# Examples

```
use std::cell::Cell;
use std::rc::Rc;

use ruststream::{AckError, HeaderMap, IncomingMessage, LocalSettle};

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

impl IncomingMessage for Delivery {
    fn payload(&self) -> &[u8] {
        &self.payload
    }

    fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    fn redelivery_count(&self) -> Option<u64> {
        Some(self.attempt)
    }
}

impl LocalSettle for Delivery {
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
