---
title: Running beside an HTTP server, outbox
description: "The storefront takes orders over HTTP and stores them with their events; a relay hands the events to the broker, and the shop runs beside the server."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Running beside an HTTP server, outbox

The storefront takes orders over HTTP and stores them with their events; a relay hands the events to the broker, and the shop runs beside the server.

When the service also serves HTTP, the [HTTP server owns the process](../../components/application/running/http.md) and the shop runs beside it.
[`start`](../../components/application/running/start.md) starts the shop in the background and returns once it is running, so a broker that refuses to connect stops the process before the server takes a request.
An HTTP request should not publish to the broker itself: a broker outage would fail the request, and an order stored without its event would never reach the shop.
A transactional outbox solves both.
The request writes the order and its event in one transaction, and a relay publishes the stored events later.
A broker outage only delays the events, and none is lost; a crash between a publish and its removal publishes the event again, so the shop sees each order at least once.

## The store

With a real database, the orders and the outbox are two tables written in one transaction.
The mutex over the pair plays that role here.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="http.rs" reference="examples/guide/http.rs#store"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="http.rs" reference="examples/manual/guide/http.rs#store"
```

</TabItem>
</Tabs>

The HTTP side and the outbox are plain functions, so both tabs show the same code.

## The endpoint

`POST /orders` takes the order as JSON and the request id from the `x-request-id` header.
It only writes to the store, so it answers `202 Accepted` without waiting on the broker.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="http.rs" reference="examples/guide/http.rs#endpoint"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="http.rs" reference="examples/manual/guide/http.rs#endpoint"
```

</TabItem>
</Tabs>

## The relay

The relay publishes the oldest event first, with its header contract, and removes it only after the publish succeeded.
A refused publish leaves the row for the next round.
The publisher is any [`Publisher`](../../components/subscribers/publishing/publishers/publisher.md), so the relay does not change with the broker.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="http.rs" reference="examples/guide/http.rs#relay"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="http.rs" reference="examples/manual/guide/http.rs#relay"
```

</TabItem>
</Tabs>

## The process

`bindable` lets the process take a publisher token before the shop starts, and [`publisher`](api:ruststream::runtime::RunningApp::publisher) turns it into a live publisher once the broker is connected.
The server stops on <kbd>Ctrl</kbd>+<kbd>C</kbd>, or when [`stopping`](api:ruststream::runtime::RunningApp::stopping) reports that the shop stopped on its own.
The shop then drains with [`shutdown`](../../components/application/running/shutdown.md).
A hand-written `main` installs the logger itself with `ruststream::logging::init()`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="http.rs" reference="examples/guide/http.rs#main"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="http.rs" reference="examples/manual/guide/http.rs#main"
```

</TabItem>
</Tabs>

Run it and place an order:

```bash
cargo run --example guide_http --features testing,macros,memory,json,cbor,asyncapi,logging
curl -X POST http://127.0.0.1:8080/orders -H 'content-type: application/json' \
  -H 'x-request-id: req-1' \
  -d '{"id":1,"customer":"ada@example.com","lines":[{"sku":"mug","quantity":12,"price_cents":1200}]}'
```

The log of chapter 5 follows the order through the shop.
A [health endpoint](../../components/application/running/health.md) reports the shop's state to an orchestrator from the same server.
