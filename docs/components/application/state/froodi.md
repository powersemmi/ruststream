---
sidebar_position: 2
title: froodi
description: "froodi is a dependency-injection container whose ruststream feature builds a handler's dependencies per delivery, as Inject parameters."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# froodi

[froodi](https://crates.io/crates/froodi) is a dependency-injection container whose `ruststream` feature builds a handler's dependencies per delivery, as `Inject` parameters.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```toml title="Cargo.toml"
[dependencies]
froodi = { version = "1.0.0-beta.19", features = ["ruststream"] }
ruststream = { version = "0.7", features = ["macros", "memory", "json"] }
serde = { version = "1", features = ["derive"] }

[dev-dependencies]
ruststream = { version = "0.7", features = ["testing"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

{/* inline-rust: it depends on froodi, a crate outside this repository */}
```rust title="src/lib.rs"
use froodi::ruststream::ContainerLayer;
use froodi::{Container, DefaultScope, Inject, instance, registry};
use ruststream::memory::prelude::*;
use serde::{Deserialize, Serialize};

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

/// Lives as long as the application.
#[derive(Clone)]
pub struct Config {
    pub price_per_item: u64,
}

/// Built for every delivery from the configuration.
pub struct Pricing {
    per_item: u64,
}

impl Pricing {
    fn total(&self, order: &Order) -> u64 {
        order.items * self.per_item
    }
}

fn container(config: Config) -> Container {
    Container::new(registry! {
        provide(DefaultScope::App, instance(config)),
        scope(DefaultScope::Request) [
            provide(|Inject(config): Inject<Config>| Ok(Pricing {
                per_item: config.price_per_item,
            })),
        ],
    })
}

#[subscriber("orders", reply)]
async fn price(order: &Order, Inject(pricing): Inject<Pricing>) -> Receipt {
    Receipt {
        order_id: order.id,
        total: pricing.total(order),
    }
}

pub fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .layer(ContainerLayer::new(container(Config {
            price_per_item: 25,
        })))
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
froodi = { version = "1.0.0-beta.19", features = ["ruststream"] }
ruststream = { version = "0.7", default-features = false, features = ["memory", "json"] }
schemars = "1"
serde = { version = "1", features = ["derive"] }

[dev-dependencies]
ruststream = { version = "0.7", default-features = false, features = ["testing"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

{/* inline-rust: it depends on froodi, a crate outside this repository */}
```rust title="src/lib.rs"
use froodi::ruststream::ContainerLayer;
use froodi::{Container, DefaultScope, Inject, instance, registry};
use ruststream::memory::prelude::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

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

/// Lives as long as the application.
#[derive(Clone)]
pub struct Config {
    pub price_per_item: u64,
}

/// Built for every delivery from the configuration.
pub struct Pricing {
    per_item: u64,
}

impl Pricing {
    fn total(&self, order: &Order) -> u64 {
        order.items * self.per_item
    }
}

fn container(config: Config) -> Container {
    Container::new(registry! {
        provide(DefaultScope::App, instance(config)),
        scope(DefaultScope::Request) [
            provide(|Inject(config): Inject<Config>| Ok(Pricing {
                per_item: config.price_per_item,
            })),
        ],
    })
}

struct Price;

impl Handle<Order, Receipt> for Price {
    async fn handle(
        &self,
        order: &Order,
        _outs: &(),
        ctx: &mut Context<'_>,
    ) -> Result<Receipt, HandlerOutcome> {
        // What the attribute emits for `Inject(pricing): Inject<Pricing>`: a failed resolution
        // settles the delivery by its outcome and the body does not run.
        let Inject(pricing) = <Inject<Pricing> as FromContext>::from_context(ctx)
            .await
            .map_err(HandlerOutcome::from)?;
        Ok(Receipt {
            order_id: order.id,
            total: pricing.total(order),
        })
    }
}

pub fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .layer(ContainerLayer::new(container(Config {
            price_per_item: 25,
        })))
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

The registry declares each dependency with the scope it lives in.
`Config` lives in the `App` scope, as long as the container.
`Pricing` lives in the `Request` scope and is built from `Config` by its factory.
`ContainerLayer` is a middleware: for every delivery it enters a child `Request` scope, runs the handler inside it and closes the scope once the handler has answered.
Closing the scope runs the finalizers of what it built before the message is settled.
`Inject<Pricing>` resolves from that child scope, so two parameters of one delivery share an instance.
`InjectTransient<T>` builds a fresh instance for each parameter instead.
A dependency the container cannot resolve drops the delivery and logs the channel and the type.
A handler that takes `Inject` in a service with no `ContainerLayer` drops every delivery the same way.
The layer builds a child container for every delivery, which allocates; dependencies in the `App` scope are built once.
With froodi's `async` feature, `AsyncContainerLayer` holds a container whose factories and finalizers can await.

## See also

- [Any struct as state](./any-struct.md): state with no container.
- [shaku](./shaku.md): a container wired by hand.
- froodi on [crates.io](https://crates.io/crates/froodi), [docs.rs](https://docs.rs/froodi) and [GitHub](https://github.com/Desiders/froodi)
- [`RustStream::layer`](api:ruststream::runtime::RustStream::layer), [`FromContext`](api:ruststream::runtime::FromContext)
