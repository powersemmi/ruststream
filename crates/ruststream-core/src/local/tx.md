The local form of [`Transaction`]: one caller-owned transaction with a buffer of its own, for a
client whose types are not `Send`.

Every [`Transaction`] is a `LocalTransaction` through a blanket impl.

# Examples

```
use std::cell::RefCell;
use std::collections::VecDeque;
use std::convert::Infallible;
use std::rc::Rc;

use ruststream::{LocalTransaction, OutgoingFor, Take};

#[derive(Default)]
struct Client {
    queue: RefCell<VecDeque<Vec<u8>>>,
}

pub struct QueueTransaction {
    client: Rc<Client>,
    buffered: Vec<Vec<u8>>,
}

impl LocalTransaction for QueueTransaction {
    type Payload = Take;
    type Error = Infallible;
    type Options = ();

    async fn publish(
        &mut self,
        msg: OutgoingFor<'_, Take>,
        _options: Option<&()>,
    ) -> Result<(), Infallible> {
        self.buffered.push(msg.into_payload().to_vec());
        Ok(())
    }

    async fn commit(self) -> Result<(), Infallible> {
        self.client.queue.borrow_mut().extend(self.buffered);
        Ok(())
    }

    async fn abort(self) -> Result<(), Infallible> {
        Ok(())
    }
}
```
