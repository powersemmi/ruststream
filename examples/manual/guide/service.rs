//! The shop assembled, without the `macros` feature: the same chain as with them, since none of it
//! is generated.

use std::time::Duration;

use ruststream::memory::prelude::*;
use ruststream::runtime::BrokerRegistration;
use ruststream::runtime::layers::TracingLayer;
use ruststream::{Contact, License, Tag};

use crate::observability::SlowHandlers;
use crate::state::Shop;
use crate::{notifications, orders, payments, shipping};

// --8<-- [start:shop]
/// The whole shop on `broker`. The command-line entry point and the HTTP host both build it here.
pub(crate) fn shop(
    broker: impl BrokerRegistration<Broker = MemoryBroker>,
) -> impl App<State = Shop> {
    RustStream::new(info())
        .on_startup(async move |()| Shop::open().await)
        .layer(TracingLayer::default())
        .layer(SlowHandlers::over(Duration::from_millis(250)))
        .with_broker(broker, |b| {
            b.include_router(orders::routes());
            b.include_router(payments::routes());
            b.include_router(shipping::routes());
            b.include_router(notifications::routes());
        })
}
// --8<-- [end:shop]

/// The broker the tests build the shop on. It is the one place a test names the broker, so
/// moving the shop to another broker changes this function and the tests follow.
#[cfg(test)]
pub(crate) fn test_broker() -> MemoryBroker {
    MemoryBroker::new()
}

// --8<-- [start:info]
/// What the AsyncAPI document says about the shop.
fn info() -> AppInfo {
    AppInfo::new("shop", "0.1.0")
        .description("Takes orders, charges them, ships them and tells the customer.")
        .contact(Contact::new().name("Shop team").email("shop@example.com"))
        .license(License::new("Apache-2.0"))
        .tag(Tag::new("orders"))
}
// --8<-- [end:info]
