// A broker whose client keeps its state in `Rc` implements the lifecycle traits as they are: the
// compio runner moves brokers across threads and does not take it.
use std::convert::Infallible;
use std::rc::Rc;

use ruststream_core::{Broker, ConnectedBroker};
use ruststream_runtime::AppInfo;
use ruststream_runtime_compio::RustStream;

struct RcBroker {
    client: Rc<()>,
}

struct RcConnected {
    client: Rc<()>,
}

impl Broker for RcBroker {
    type Error = Infallible;
    type Connected = RcConnected;

    async fn connect(self) -> Result<RcConnected, Infallible> {
        Ok(RcConnected { client: self.client })
    }
}

impl ConnectedBroker for RcConnected {
    type Error = Infallible;
    type Closed = ();

    async fn shutdown(self) -> Result<(), Infallible> {
        drop(self.client);
        Ok(())
    }
}

fn main() {
    let broker = RcBroker {
        client: Rc::new(()),
    };
    let _app = RustStream::new(AppInfo::new("local", "0.1.0")).register_broker(broker);
}
