//! Shared state and lifecycle hooks from the Lifespan guide, written without the `macros` feature:
//! a resource opened in `on_startup`, shared with handlers as the typed application state, and
//! closed in `after_shutdown`.
//!
//! The hooks themselves are plain builder methods on `RustStream`, so nothing about them changes
//! here. What changes is the handler: with no `State<T>` extractor parameter to declare, the
//! handler names the state as the last axis of its `Handle` impl and reads it with `ctx.state()`.
//!
//! The `Database` here is a stand-in for any async resource (a `sqlx::PgPool`, an HTTP client);
//! only its `connect` / `close` calls would differ.
//!
//! ```text
//! cargo run --example manual_lifespan --no-default-features --features memory,json
//! ```

use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

use ruststream::memory::prelude::*;
use ruststream::runtime::Identity;
use serde::Deserialize;

#[derive(Debug, Deserialize, schemars::JsonSchema)]
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
// The handler names the app's state type as the state axis of its `Handle` impl; `ctx.state()` then
// borrows the typed `Database` directly, with no lookup or downcast. This is the object the
// `State<T>` extractor would have handed over, reached by hand.
struct Receive;

impl Handle<Order, (), (), (), Database> for Receive {
    async fn handle(
        &self,
        order: &Order,
        _outs: &(),
        ctx: &mut Context<'_, (), Database>,
    ) -> Result<(), HandlerOutcome> {
        let db = ctx.state();
        if db.insert_order(order.id).await.is_err() {
            return Err(HandlerOutcome::retry());
        }
        Ok(())
    }
}

// The state is part of the body's own signature, so `subscriber(source, body)` mounts it unchanged:
// the mount reads `Database` off the `Handle` impl and checks it against the app's.
// --8<-- [end:handler]

// --8<-- [start:hooks]
// The builder's state type is `Database` once `on_startup` produces it, so the return type names it.
fn app() -> RustStream<Identity, Database> {
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        // before brokers connect: open the resource; the produced value becomes the typed app state
        .on_startup(async move |()| Database::connect("postgres://localhost/orders").await)
        // after brokers shut down: close it cleanly (the state is shared as `Arc<Database>`)
        .after_shutdown(|db: Arc<Database>| async move {
            db.close().await;
            Ok::<_, DbError>(())
        })
        // bound the post-shutdown drain of in-flight handlers
        .shutdown_timeout(Duration::from_secs(10))
        .with_broker(MemoryBroker::new(), |b| {
            b.include(subscriber("orders", Receive).build());
        })
}
// --8<-- [end:hooks]

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    app().run().await?;
    Ok(())
}
