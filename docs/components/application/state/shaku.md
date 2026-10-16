---
sidebar_position: 3
title: shaku
description: "shaku is a dependency-injection container with no RustStream support; the service builds it in on_startup and a handler resolves from it."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# shaku

[shaku](https://crates.io/crates/shaku) is a dependency-injection container with no RustStream support; the service builds it in `on_startup` and a handler resolves from it.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```toml title="Cargo.toml"
[dependencies]
ruststream = { version = "0.7", features = ["macros", "memory", "json"] }
serde = { version = "1", features = ["derive"] }
shaku = "0.6"

[dev-dependencies]
ruststream = { version = "0.7", features = ["testing"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

{/* inline-rust: it depends on shaku, a crate outside this repository */}
```rust title="src/lib.rs"
use std::convert::Infallible;
use std::sync::Arc;

use ruststream::memory::prelude::*;
use serde::{Deserialize, Serialize};
use shaku::{Component, HasComponent, Interface, module};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
pub struct Order {
    pub id: u64,
    pub items: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "receipts")]
pub struct Receipt {
    pub order_id: u64,
    pub total: u64,
}

/// The service a handler asks the container for.
pub trait Pricing: Interface {
    fn total(&self, order: &Order) -> u64;
}

#[derive(Component)]
#[shaku(interface = Pricing)]
struct FlatPricing {
    per_item: u64,
}

impl Pricing for FlatPricing {
    fn total(&self, order: &Order) -> u64 {
        order.items * self.per_item
    }
}

module! {
    OrdersModule {
        components = [FlatPricing],
        providers = []
    }
}

// The bridge from the container to `State<T>`: a handler that takes `State<Arc<dyn Pricing>>`
// gets the component the module resolves.
impl FromRef<OrdersModule> for Arc<dyn Pricing> {
    fn from_ref(module: &OrdersModule) -> Self {
        module.resolve()
    }
}

#[subscriber("orders", reply)]
async fn price(order: &Order, State(pricing): State<Arc<dyn Pricing>>) -> Receipt {
    Receipt {
        order_id: order.id,
        total: pricing.total(order),
    }
}

pub fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        // The container is built once, before the brokers connect, and becomes the app state.
        .on_startup(async move |()| {
            let module = OrdersModule::builder()
                .with_component_parameters::<FlatPricing>(FlatPricingParameters { per_item: 25 })
                .build();
            Ok::<_, Infallible>(module)
        })
        .with_broker(MemoryBroker::new(), |b| {
            b.include(price).out_reply(Publish);
        })
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::{Order, Receipt, app};

    #[tokio::test]
    async fn the_handler_prices_the_order() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();
        broker
            .message(&Order { id: 7, items: 3 })
            .to("orders")
            .publish()
            .await?;

        broker
            .published::<Receipt>("receipts")
            .assert_called_once()
            .with(&Receipt {
                order_id: 7,
                total: 75,
            });
        Ok(())
    }
}
```

</TabItem>
<TabItem value="manual" label="Manual">

```toml title="Cargo.toml"
[dependencies]
ruststream = { version = "0.7", default-features = false, features = ["memory", "json"] }
schemars = "1"
serde = { version = "1", features = ["derive"] }
shaku = "0.6"

[dev-dependencies]
ruststream = { version = "0.7", default-features = false, features = ["testing"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

{/* inline-rust: it depends on shaku, a crate outside this repository */}
```rust title="src/lib.rs"
use std::convert::Infallible;
use std::future::{Future, ready};
use std::sync::Arc;

use ruststream::memory::prelude::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use shaku::{Component, HasComponent, Interface, module};

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
pub struct Order {
    pub id: u64,
    pub items: u64,
}

// `#[derive(Outgoing)]` with no `name`, by hand: the call site names the destination.
impl OutgoingDestination for Order {
    type Form = CallerName;
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
pub struct Receipt {
    pub order_id: u64,
    pub total: u64,
}

// `#[outgoing(name = "receipts")]` by hand: the type fixes its destination.
impl OutgoingDestination for Receipt {
    type Form = FixedName;

    const DESTINATION: &'static str = "receipts";
}

impl MessageHeaders for Receipt {
    type Contract = NoHeaders;
}

/// The service a handler asks the container for.
pub trait Pricing: Interface {
    fn total(&self, order: &Order) -> u64;
}

#[derive(Component)]
#[shaku(interface = Pricing)]
struct FlatPricing {
    per_item: u64,
}

impl Pricing for FlatPricing {
    fn total(&self, order: &Order) -> u64 {
        order.items * self.per_item
    }
}

module! {
    OrdersModule {
        components = [FlatPricing],
        providers = []
    }
}

// The bridge from the container to the handler: the module resolves the component.
impl FromRef<OrdersModule> for Arc<dyn Pricing> {
    fn from_ref(module: &OrdersModule) -> Self {
        module.resolve()
    }
}

struct Price;

// The container is the app state, the last parameter of `Handle`; the mount checks it.
impl Handle<Order, Receipt, (), (), OrdersModule> for Price {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        ctx: &mut Context<'_, (), OrdersModule>,
    ) -> impl Future<Output = Result<Receipt, HandlerOutcome>> {
        let pricing = Arc::<dyn Pricing>::from_ref(ctx.state());
        ready(Ok(Receipt {
            order_id: order.id,
            total: pricing.total(order),
        }))
    }
}

pub fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        // The container is built once, before the brokers connect, and becomes the app state.
        .on_startup(async move |()| {
            let module = OrdersModule::builder()
                .with_component_parameters::<FlatPricing>(FlatPricingParameters { per_item: 25 })
                .build();
            Ok::<_, Infallible>(module)
        })
        .with_broker(MemoryBroker::new(), |b| {
            b.include(subscriber("orders", Price).reply().build())
                .out_reply(Publish);
        })
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::{Order, Receipt, app};

    #[tokio::test]
    async fn the_handler_prices_the_order() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<MemoryBroker>();
        broker
            .message(&Order { id: 7, items: 3 })
            .to("orders")
            .publish()
            .await?;

        broker
            .published::<Receipt>("receipts")
            .assert_called_once()
            .with(&Receipt {
                order_id: 7,
                total: 75,
            });
        Ok(())
    }
}
```

</TabItem>
</Tabs>

`cargo test` runs the test at the end of the file:

```text
running 1 test
test tests::the_handler_prices_the_order ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
```

## How it works

A shaku module is the container: `module!` lists its components, and each component implements an interface trait.
`on_startup` builds the module with the components' parameters, and the module becomes the application state.
A `FromRef` impl from the module to `Arc<dyn Pricing>` lets a handler take `State<Arc<dyn Pricing>>`.
The module resolves a component as a clone of its `Arc`, so each delivery pays one reference count and the calls go through `dyn Pricing`.
A handler that reads `ctx.state().resolve_ref()` borrows the component instead, with no reference count.
Components are singletons of the module and are `Send + Sync`, so every handler on every worker shares them.
An interface method that awaits needs a `dyn`-compatible form, such as one returning a boxed future.

## See also

- [Any struct as state](./any-struct.md): the state and `FromRef` this page builds on.
- [froodi](./froodi.md): a container with RustStream support and per-delivery scopes.
- shaku on [crates.io](https://crates.io/crates/shaku), [docs.rs](https://docs.rs/shaku) and [GitHub](https://github.com/AzureMarker/shaku)
- [`State`](api:ruststream::runtime::State), [`FromRef`](api:ruststream::runtime::FromRef), [`RustStream::on_startup`](api:ruststream::runtime::RustStream::on_startup)
