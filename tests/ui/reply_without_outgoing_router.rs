//! The shape a scaffolded service writes: a `Router` registration that carries a publish position
//! and closes with `.build()`. The reply type is missing its `Outgoing` derive, and the error a
//! service reads has to name that derive rather than the bounds of the mount chain.

use ruststream::memory::{MemoryBroker, MemoryPublish};
use ruststream::runtime::{Reply, Router, RouterDef};
use ruststream::subscriber;
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct Order {
    id: u32,
}

#[derive(Serialize)]
struct Receipt {
    id: u32,
}

#[subscriber("orders", publish("receipts"))]
async fn confirm(order: &Order) -> Receipt {
    Receipt { id: order.id }
}

fn orders() -> impl RouterDef<MemoryBroker> {
    Router::new()
        .include(confirm)
        .out(Reply, MemoryPublish)
        .build()
}

fn main() {
    let _ = orders();
}
