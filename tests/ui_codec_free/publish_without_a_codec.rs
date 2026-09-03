//! A `Serialize` value published in a build with no codec feature: nothing in the chain names a
//! codec, and the encoded wire needs one.
use ruststream::memory::MemoryBroker;
use ruststream::runtime::PublishExt;
use ruststream::{Broker, Outgoing};

#[derive(Outgoing, serde::Serialize)]
#[outgoing(name = "orders.done")]
struct OrderDone {
    id: u64,
}

fn main() {
    let publisher = MemoryBroker::new().publisher();
    let _ = async {
        publisher.message(&OrderDone { id: 7 }).publish().await
    };
}
