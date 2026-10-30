The local form of [`Broker`]: the unconnected broker, for a client whose types are not `Send`.

A broker built on a thread-per-core client, on an embedded network stack, or on any client that
holds `Rc` or `RefCell` implements this trait and the rest of the `Local*` family. The items are
those of [`Broker`] without the `Send` and `Sync` bounds; the lazy startup contract and the
lifecycle ladder are the same. Every [`Broker`] is a `LocalBroker` through a blanket impl.

# Examples

```
use std::convert::Infallible;
use std::rc::Rc;

use ruststream::{LocalBroker, LocalConnectedBroker};

/// The client's state, shared by `Rc`: the broker and its connected form are not `Send`.
#[derive(Default)]
struct Client;

pub struct QueueBroker {
    client: Rc<Client>,
}

impl QueueBroker {
    /// Records the configuration and performs no I/O.
    #[must_use]
    pub fn new() -> Self {
        Self {
            client: Rc::new(Client),
        }
    }
}

pub struct QueueConnected {
    client: Rc<Client>,
}

impl LocalBroker for QueueBroker {
    type Error = Infallible;
    type Connected = QueueConnected;

    async fn connect(self) -> Result<QueueConnected, Infallible> {
        Ok(QueueConnected {
            client: self.client,
        })
    }
}

impl LocalConnectedBroker for QueueConnected {
    type Error = Infallible;
    type Closed = ();

    async fn shutdown(self) -> Result<(), Infallible> {
        drop(self.client);
        Ok(())
    }
}
```
