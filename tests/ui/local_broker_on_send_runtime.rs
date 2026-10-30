// A broker whose client keeps its state in `Rc` implements the local family only: today's runtime
// moves brokers across threads and does not take it.
use std::convert::Infallible;
use std::rc::Rc;

use ruststream::prelude::*;
use ruststream::{LocalBroker, LocalConnectedBroker};

struct RcBroker {
    client: Rc<()>,
}

struct RcConnected {
    client: Rc<()>,
}

impl LocalBroker for RcBroker {
    type Error = Infallible;
    type Connected = RcConnected;

    async fn connect(self) -> Result<RcConnected, Infallible> {
        Ok(RcConnected { client: self.client })
    }
}

impl LocalConnectedBroker for RcConnected {
    type Error = Infallible;
    type Closed = ();

    async fn shutdown(self) -> Result<(), Infallible> {
        drop(self.client);
        Ok(())
    }
}

fn app() -> RustStream {
    let broker = RcBroker {
        client: Rc::new(()),
    };
    RustStream::new(AppInfo::new("local", "0.1.0")).with_broker(broker, |_b| {})
}

fn main() {
    let _ = app;
}
