use ruststream::memory::{MemoryPublisher, MemorySeeker};
use ruststream::runtime::{HandlerResult, Out, Seek};
use ruststream::subscriber;
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u32,
}

// The two injections do not compose yet; repositioning next to an injected publisher goes
// through a WithSeeker token.
#[subscriber("orders")]
async fn handle(
    order: &Order,
    Out(_out): Out<MemoryPublisher>,
    Seek(_seeker): Seek<MemorySeeker>,
) -> HandlerResult {
    let _ = order.id;
    HandlerResult::Ack
}

fn main() {}
