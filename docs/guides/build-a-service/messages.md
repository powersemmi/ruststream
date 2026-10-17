---
title: "Messages: payloads, headers, codecs"
description: "An order grows into a payload with lines, a header contract every publish must carry, and a codec chosen per topic."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Messages: payloads, headers, codecs

An order grows into a payload with lines, a header contract every publish must carry, and a codec chosen per topic.

The payload is any `serde` type; the codec turns it into bytes and back.
The headers travel beside the payload, and a [header contract](../../components/subscribers/typed-headers/index.md) is a `serde` struct with one field per header.
A message type that names a contract cannot be published without it: the publish does not compile.
A subscriber reads the contract with a [`Headers<T>`](api:ruststream::runtime::Headers) parameter, parsed before the body runs.
A delivery whose headers do not parse never reaches the body and is dropped, like a payload that does not decode.
The [codec](../../components/subscribers/codecs/index.md) belongs to the mount site, not to the type, so one type can travel as JSON on one topic and as CBOR on another.

## The order

From here on, the shop's code lives in modules.
`orders.rs` holds the order: its lines, the total, and the contract it travels with.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="orders.rs" reference="examples/guide/orders.rs#order"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="orders.rs" reference="examples/manual/guide/orders.rs#order"
```

</TabItem>
</Tabs>

`headers = RequestMeta` makes the contract part of the type.
The manual form says the same with `type Contract = WithHeaders<RequestMeta>`, and gives the message its name in the AsyncAPI document with a [`MessageInfo`](api:ruststream::MessageInfo) impl.

## The header contract

Each field of the contract is one header.
`request_id` names the storefront request that placed the order, so every step can log it.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="orders.rs" reference="examples/guide/orders.rs#meta"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="orders.rs" reference="examples/manual/guide/orders.rs#meta"
```

</TabItem>
</Tabs>

## Reading the headers

`audit` is a second subscriber on `orders.placed`: it writes every order to the audit log with the request that placed it.
Each subscription gets every message of its topic, so `audit` and the `accept` of the next chapter both see each order.
The manual form takes the pair [`Message<RequestMeta, Order>`](api:ruststream::runtime::Message) as its input: both parts decode before the body runs, and the AsyncAPI document shows the header schema.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="orders.rs" reference="examples/guide/orders.rs#audit"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="orders.rs" reference="examples/manual/guide/orders.rs#audit"
```

</TabItem>
</Tabs>

## The codec of the shop's own topics

JSON is the default codec, and the shop keeps it for `orders.placed` and `notices`, which other teams read.
The topics between the shop's own steps travel as [CBOR](../../components/subscribers/codecs/cbor.md): smaller, and read by no one else.
One constant names that codec, so the routers of the next chapters cannot disagree about it.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="payments.rs" reference="examples/guide/payments.rs#codec"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="payments.rs" reference="examples/manual/guide/payments.rs#codec"
```

</TabItem>
</Tabs>

A [router](../../components/subscribers/router.md) decodes with the codec [`with_codec`](api:ruststream::runtime::Router::with_codec) names, and each publish position names its own with `.codec(..)`.
The next chapter puts both to use.
CBOR needs the crate's `cbor` feature.
