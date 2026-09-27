//! The `Debug` forms of the public types, which logs and panic messages print: each names its type,
//! shows the fields that identify the value (a count, a name, a timeout) and elides what it holds
//! but must not print (a borrowed scope, a closure, a connection, a payload). One test per module.
#![cfg(all(feature = "macros", feature = "memory", feature = "json"))]

mod common;

use std::time::Duration;

use common::{Order, Receipt};
use ruststream::memory::prelude::*;

#[subscriber("debug.reply", publish("debug.reply.out"))]
async fn answer(order: &Order) -> Receipt {
    Receipt { id: order.id }
}

#[subscriber("debug.slot")]
async fn slot(_order: &Order, Out(out): Out<impl Publisher>) -> HandlerOutcome {
    let _ = out;
    HandlerOutcome::ack()
}

/// The app module: a mount guard names the terminal it commits through and elides the scope it
/// borrows, and a running app shows its subscriber and broker counts and its shutdown timeout.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_app_types_render_their_debug_forms() {
    let app = RustStream::new(AppInfo::new("debug", "0.1.0"))
        .shutdown_timeout(Duration::from_secs(5))
        .with_broker(MemoryBroker::new(), |b| {
            // A reply-only registration is complete as it stands, so dropping the guard commits.
            let mounting = b.include(answer);
            assert_eq!(format!("{mounting:?}"), "Mounting { .. }");
            drop(mounting);

            let slots = b.include(slot);
            assert_eq!(format!("{slots:?}"), "MountingSlots { .. }");
            slots.out(DefaultSlot, Publish).build();
        });

    let running = app.start().await.expect("startup failed");
    assert_eq!(
        format!("{running:?}"),
        "RunningApp { subscribers: 2, brokers: 1, shutdown_timeout: Some(5s), .. }",
    );
    running.shutdown().await.expect("shutdown failed");
}

/// The router module: a router and a half-built registration chain name themselves and elide the
/// routes, codec and layers they carry.
#[test]
fn the_router_types_render_their_debug_forms() {
    let router = Router::<MemoryBroker>::new();
    assert_eq!(format!("{router:?}"), "Router { .. }");

    let chain = router.include(slot);
    assert_eq!(format!("{chain:?}"), "RouterWith { .. }");
    let _ = chain.out(DefaultSlot, Publish).build();
}
