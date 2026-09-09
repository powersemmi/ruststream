use ruststream::runtime::HandlerOutcome;
use ruststream::subscriber;
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u32,
}

struct Stream {
    name: String,
}

struct GroupedStream {
    name: String,
}

impl Stream {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_owned(),
        }
    }

    fn group(self, _group: &str) -> GroupedStream {
        GroupedStream { name: self.name }
    }
}

// The attribute reads `Stream` off the constructor, but the chain ends on `GroupedStream`. The
// fix is to name what the expression produces: `... .group("workers") as GroupedStream`.
#[subscriber(Stream::new("orders").group("workers"))]
async fn handle(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

fn main() {}
