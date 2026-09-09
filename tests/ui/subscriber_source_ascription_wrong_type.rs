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

// The ascription is checked, not trusted: this chain produces a `GroupedStream`.
#[subscriber(Stream::new("orders").group("workers") as Stream)]
async fn handle(order: &Order) -> HandlerOutcome {
    let _ = order.id;
    HandlerOutcome::ack()
}

fn main() {}
