//! Shared state and lifecycle hooks from the Lifespan guide: a resource opened in `on_startup`,
//! shared with handlers as the typed application state, and closed in `after_shutdown`.
//!
//! The `Database` here is a stand-in for any async resource (a `sqlx::PgPool`, an HTTP client);
//! only its `connect` / `close` calls would differ.
//!
//! ```text
//! cargo run --example lifespan --features macros,memory,json -- run
//! ```

use std::time::Duration;

use ruststream::memory::MemoryBroker;
use ruststream::runtime::{AppInfo, HandlerResult, Identity, RustStream};
use ruststream::subscriber;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Order {
    id: u64,
}

/// A stand-in for a connection pool: cheap to clone, shared by every handler.
#[derive(Debug, Clone)]
struct Database;

#[derive(Debug, thiserror::Error)]
#[error("database error")]
struct DbError;

impl Database {
    async fn connect(url: &str) -> Result<Self, DbError> {
        println!("connecting to {url}");
        tokio::task::yield_now().await; // stands in for the real network round trip
        Ok(Self)
    }

    async fn insert_order(&self, id: u64) -> Result<(), DbError> {
        println!("insert order {id}");
        tokio::task::yield_now().await;
        Ok(())
    }

    async fn close(&self) {
        println!("closing the database");
        tokio::task::yield_now().await;
    }
}

// --8<-- [start:handler]
// The handler names the app's state type as the third `Context` generic; `ctx.state()` then borrows
// the typed `Database` directly, with no lookup or downcast.
#[subscriber("orders")]
async fn handle(order: &Order, ctx: &mut Context<'_, (), Database>) -> HandlerResult {
    let db = ctx.state();
    if db.insert_order(order.id).await.is_err() {
        return HandlerResult::retry();
    }
    HandlerResult::Ack
}
// --8<-- [end:handler]

// --8<-- [start:hooks]
// The builder's state type is `Database` once `on_startup` produces it, so the return type names it.
#[ruststream::app]
fn app() -> RustStream<Identity, Database> {
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        // before brokers connect: open the resource; the produced value becomes the typed app state
        .on_startup(async move |()| Database::connect("postgres://localhost/orders").await)
        // after brokers shut down: close it cleanly (the state is shared as `Arc<Database>`)
        .after_shutdown(|db: std::sync::Arc<Database>| async move {
            db.close().await;
            Ok::<_, DbError>(())
        })
        // bound the post-shutdown drain of in-flight handlers
        .shutdown_timeout(Duration::from_secs(10))
        .with_broker(MemoryBroker::new(), |b| b.include(handle))
}
// --8<-- [end:hooks]
