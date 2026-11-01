// A publisher that keeps its state in an `Rc` is refused where the Tokio runner is on: the error
// names the publisher and says it must be `Send + Sync`.
use std::convert::Infallible;
use std::rc::Rc;

use ruststream::{Lend, OutgoingMessage, PublishOutcome, Publisher};

#[derive(Clone)]
struct RcPublisher {
    sent: Rc<()>,
}

impl Publisher for RcPublisher {
    type Payload = Lend;
    type Error = Infallible;
    type Options = ();

    async fn publish<'msg>(
        &self,
        _msg: OutgoingMessage<'msg, &'msg [u8]>,
        _options: Option<&()>,
    ) -> PublishOutcome<'msg, Infallible, Lend> {
        let _ = &self.sent;
        Ok(())
    }
}

fn main() {}
