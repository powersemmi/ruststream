The local form of [`Publisher`]: the live publish handle, for a client whose types are not
`Send`.

The items are those of [`Publisher`] without the `Send` and `Sync` bounds on the handle and its
future. [`Payload`](Self::Payload), [`Error`](Self::Error) and [`Options`](Self::Options) keep
their bounds. The handle is `Clone`, as a `Publisher` is: an `Rc`-held client clones by
refcount. Every [`Publisher`] is a `LocalPublisher` through a blanket impl.

# Examples

```
use std::cell::RefCell;
use std::collections::VecDeque;
use std::convert::Infallible;
use std::rc::Rc;

use ruststream::{LocalPublisher, OutgoingFor, Take};

/// The client's outgoing queue, shared by `Rc`.
#[derive(Default)]
struct Client {
    outgoing: RefCell<VecDeque<(String, Vec<u8>)>>,
}

#[derive(Clone)]
pub struct QueuePublisher {
    client: Rc<Client>,
}

impl LocalPublisher for QueuePublisher {
    // The client keeps the bytes past the call.
    type Payload = Take;
    type Error = Infallible;
    type Options = ();

    async fn publish<'msg>(
        &self,
        msg: OutgoingFor<'msg, Take>,
        _options: Option<&()>,
    ) -> PublishOutcome<'msg, Infallible, Take> {
        let (name, payload, _headers) = msg.into_parts();
        self.client
            .outgoing
            .borrow_mut()
            .push_back((name.to_owned(), payload.to_vec()));
        Ok(())
    }
}
```
