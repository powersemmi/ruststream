use std::future::{Future, ready};
use std::io;

use ruststream::{Broker, ConnectedBroker};

// Registering a broker with the harness names its in-process transition, so a broker without one
// cannot be registered.
struct Networked;

struct ConnectedNetworked;

impl Broker for Networked {
    type Error = io::Error;
    type Connected = ConnectedNetworked;

    fn connect(self) -> impl Future<Output = Result<ConnectedNetworked, io::Error>> + Send {
        ready(Ok(ConnectedNetworked))
    }
}

impl ConnectedBroker for ConnectedNetworked {
    type Error = io::Error;
    type Closed = ();

    fn shutdown(self) -> impl Future<Output = Result<(), io::Error>> + Send {
        ready(Ok(()))
    }
}

ruststream::register_testable_broker!(Networked);

fn main() {}
