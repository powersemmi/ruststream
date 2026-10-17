---
title: Switching to a networked broker
description: "The shop moves to NATS by changing the broker it is built on; the subscribers, the routers' settings and the tests stay as they are."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Switching to a networked broker

The shop moves to NATS by changing the broker it is built on; the subscribers, the routers' settings and the tests stay as they are.

The broker is chosen in one place: the value passed to `shop`.
A broker crate brings the broker type and a prelude with the same names the in-memory prelude has, `Publish` among them.
So a router reads the same on every broker, and only the type it names changes.
The subscriber bodies import nothing from the broker, so they do not change at all.
Every broker has its own [crate](../../brokers/index.md); this chapter uses [`ruststream-nats`](https://docs.rs/ruststream-nats).

## The dependency

```toml title="Cargo.toml"
[dependencies]
ruststream = { version = "0.7", features = ["macros", "json", "cbor", "asyncapi", "logging"] }
ruststream-nats = { version = "0.7", features = ["asyncapi"] }
```

The `memory` feature is no longer needed.
The `testing` features of both crates go into `[dev-dependencies]`.

## The swap

The entry point builds the shop on a `NatsBroker`, which, like the in-memory broker, connects only when the service starts.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

{/* inline-rust: the shop on NATS needs the ruststream-nats crate, which this repository does not depend on; this code was built and run against a NATS 2 server for this page */}
```rust title="main.rs"
use ruststream_nats::prelude::*;

use crate::state::Shop;

#[ruststream::app]
fn app() -> impl App<State = Shop> {
    service::shop(NatsBroker::new("nats://localhost:4222"))
}
```

</TabItem>
<TabItem value="manual" label="Manual">

{/* inline-rust: the shop on NATS needs the ruststream-nats crate, which this repository does not depend on; this code was built and run against a NATS 2 server for this page */}
```rust title="main.rs"
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream_nats::prelude::*;

use crate::state::Shop;

fn app() -> impl App<State = Shop> {
    service::shop(NatsBroker::new("nats://localhost:4222"))
}

fn main() -> ExitCode {
    run_main(app)
}
```

</TabItem>
</Tabs>

The other files change in two ways.
Each imports `ruststream_nats::prelude::*` in place of `ruststream::memory::prelude::*`.
Each router returns `RouterDef<NatsBroker, Shop>`, and `shop` takes a broker whose type is `NatsBroker`.
The codecs, the slots, the attempt cap and the dead-letter topic stay as they are.
`http.rs` binds its publisher token on `NatsBroker::new(..).bindable()` the same way.

## The tests

`NatsBroker` has an in-process mode, so `TestApp` runs the shop without a server, as it did on the in-memory broker.
The tests change only the broker they ask for: `tb.broker::<NatsBroker>()`.
All of them pass unchanged otherwise, the delay of chapter 8 and the AsyncAPI document included.

## What NATS changes

```bash
docker run --rm -p 4222:4222 nats:2-alpine
cargo run -- run
```

A topic name is a subject on Core NATS.
Core NATS keeps no messages: one published while no subscriber listens is gone, and a delivery has no acknowledgement.
A `retry` or a `retry_after` publishes a copy back to the subject, and the attempt cap counts the copies.
A topic whose messages must survive a restart reads a JetStream stream instead: [`JetStreamSubject`](https://docs.rs/ruststream-nats/latest/ruststream_nats/struct.JetStreamSubject.html) in the attribute, with a durable consumer.
The crate's [`nats_jetstream`](https://github.com/powersemmi/ruststream-nats/blob/main/crates/ruststream-nats/examples/nats_jetstream.rs) example shows one, and [`nats_retry`](https://github.com/powersemmi/ruststream-nats/blob/main/crates/ruststream-nats/examples/nats_retry.rs) shows how each kind of subscription retries.
