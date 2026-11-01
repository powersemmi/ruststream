// An `.error` continuation that holds an `Rc` is refused where the Tokio runner is on: the error
// names the closure and says it must be `Send`, at the call that registers it.
use std::rc::Rc;

use ruststream::runtime::{Failures, HandlerOutcome};
use ruststream::subscriber;
use serde::Deserialize;

#[derive(Deserialize)]
struct Order {
    id: u32,
}

#[subscriber("orders")]
async fn on_order(order: &Order) -> HandlerOutcome {
    let held = Rc::new(order.id);
    HandlerOutcome::ack().error(async move |failures: Failures| {
        for failed in failures {
            let (_err, _lost) = failed.into_parts();
        drop(held);
        }
    })
}

fn main() {}
