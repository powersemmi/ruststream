The local form of [`ConnectedBroker`]: the typed witness of a live connection, for a client whose
types are not `Send`.

The items are those of [`ConnectedBroker`] without the `Send` and `Sync` bounds, and
[`Closed`](Self::Closed) need not be `Send`. Every [`ConnectedBroker`] is a
`LocalConnectedBroker` through a blanket impl.

# Examples

```
use std::cell::Cell;
use std::convert::Infallible;
use std::rc::Rc;

use ruststream::LocalConnectedBroker;

/// The client's state: the number of subscriptions it still holds open.
#[derive(Default)]
struct Client {
    open: Cell<usize>,
}

pub struct QueueConnected {
    client: Rc<Client>,
}

/// What the shutdown dropped, reported as plain data.
pub struct QueueClosed {
    pub dropped_subscriptions: usize,
}

impl LocalConnectedBroker for QueueConnected {
    type Error = Infallible;
    type Closed = QueueClosed;

    async fn shutdown(self) -> Result<QueueClosed, Infallible> {
        Ok(QueueClosed {
            dropped_subscriptions: self.client.open.replace(0),
        })
    }
}
```
