---
title: The AsyncAPI document
description: "asyncapi gen prints the shop's AsyncAPI document: every topic, who reads and writes it, the schemas, the retry cap, and the description the app gives itself."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# The AsyncAPI document

`asyncapi gen` prints the shop's AsyncAPI document: every topic, who reads and writes it, the schemas, the retry cap, and the description the app gives itself.

The document is built from the app itself, so it cannot describe a topic the code does not use.
Each subscription is a `receive` operation, each reply and slot a `send` operation.
A payload's schema comes from its [`JsonSchema`](../../components/asyncapi/index.md) derive, and a doc comment becomes a description.
`max_attempts` and `dead_letter` appear on the operation, so a reader sees where a failing message ends up.
[AsyncAPI](../../components/asyncapi/index.md) covers servers, security schemes and serving the document over HTTP.
The document needs the crate's `asyncapi` feature.

## The entry point

`main.rs` builds the shop on the in-memory broker.
[`#[ruststream::app]`](api:ruststream::app) gives the binary its commands: `run` starts the shop, `asyncapi gen` prints the document.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="main.rs" reference="examples/guide/main.rs#main"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="main.rs" reference="examples/manual/guide/main.rs#main"
```

</TabItem>
</Tabs>

## What the shop says about itself

[`AppInfo`](api:ruststream::runtime::AppInfo) is the document's `info` block: the title, the version, a description, a contact, a license and tags.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="service.rs" reference="examples/guide/service.rs#info"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="service.rs" reference="examples/manual/guide/service.rs#info"
```

</TabItem>
</Tabs>

## The document

```bash
cargo run --example guide --features testing,macros,memory,json,cbor,asyncapi -- asyncapi gen --yaml
```

The operation of `charge` reads the topic, answers on `payments.settled`, and declares the cap:

```yaml
receive_payments_requested:
  action: receive
  channel:
    $ref: '#/channels/payments.requested'
  messages:
  - $ref: '#/channels/payments.requested/messages/PaymentRequest'
  description: Charges an order through the payment provider.
  reply:
    channel:
      $ref: '#/channels/payments.settled'
    messages:
    - $ref: '#/channels/payments.settled/messages/Paid'
  x-ruststream-retry:
    maxAttempts: 5
    deadLetter: payments.dead
```

Both forms of the shop produce the same document.
A test in each compares the document with [`examples/guide/asyncapi.json`](https://github.com/powersemmi/ruststream/blob/main/examples/guide/asyncapi.json), so a change to a topic shows up in review.
