---
sidebar_position: 11
title: Message sets
description: "A handler that receives several message types on one subscription declares them with #[derive(InMessages)]: each variant decodes through its own path, a rule picks the variant, and the document lists one message per variant."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Message sets

A handler that receives several message types on one subscription declares them with [`#[derive(InMessages)]`](api:ruststream::InMessages): each variant decodes through its own path, a rule picks the variant, and the document lists one message per variant.

The set is an enum whose variants each wrap one model, and the handler takes a reference to the enum: `&OrderEvent`, or `&[OrderEvent]` for a [batch](./batches.md).
Each model derives [`MessageInfo`](api:ruststream::MessageInfo), because the document lists it by that name.
A model decodes through its own path: the codec of the mount for a `serde` type, its own reader for a type that derives [`Deserialized`](api:ruststream::Deserialized), such as a [Protobuf](./codecs/protobuf.md) message.
One set may hold both, so a JSON event and a Protobuf event share a subscription and each shows its own format in the [document](../asyncapi/reports/message-sets.md).
The derive is the receiving counterpart of [`OutMessages`](./publishing/out-slots.md), which declares what a slot sends.

## The header rule

`#[in_messages(header = "event")]` on the enum names the header the set reads, and `#[in_messages(value = "..")]` on each variant names the value that picks it.
The chosen variant decodes once; a set costs what a single type costs, plus the header lookup.
A delivery without the header, or with a value no variant declares, is a decode failure naming the header and the value seen, settled by the [failure policy](./failure-policy.md) like any other.
The document carries the rule: each message's headers schema lists the header with its `const` value.
This is the rule to reach for when the producer can set a header.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/in_messages.rs#set"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/in_messages.rs#set"
```

</TabItem>
</Tabs>

The handler matches on the set.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/in_messages.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/in_messages.rs#handler"
```

</TabItem>
</Tabs>

## A rule of your own

`#[in_messages(discriminate = path)]` names a function that reads the delivery, its headers and raw payload, and returns which kind to decode.
The derive generates the kind enum, `OrderUpdateKind` for a set `OrderUpdate`, with one unit variant per model.
The function returning `None` is a decode failure naming the set.
This is the form for a rule no attribute states: a schema id, a field of the payload, a shape of the bytes.
A broker crate may ship such a function for its own format.

```rust reference="examples/in_messages.rs#discriminate"
```

## Left to right

A set with no rule tries its variants in their order, and the first that decodes wins.
The order of the variants is the rule, so put the strictest model first.
Every variant failing is one decode failure that carries each variant's error.
Each attempt costs one decode of that variant, so a delivery of the last variant pays for every variant before it.
Left to right is exact only as far as the formats are strict: a Protobuf reader skips the fields it does not know, so a payload of one Protobuf model may decode as another, and a JSON object with the right fields decodes as any model that has them.
For such sets, name a rule, or order the variants from the strictest to the loosest.

```rust reference="examples/in_messages.rs#left_to_right"
```

## What the derive checks

Every variant wraps exactly one model, each model implements `MessageInfo`, a model appears once in a set, the enum takes no generic parameters, and a variant owns its model: a type that borrows the payload cannot be one.
A set is not a message, so `MessageInfo` is not derived on the set itself.
Each of these is a compile error on the variant or the enum it concerns.

A `serde` enum with `#[serde(tag = "..")]` or `#[serde(untagged)]` is still an ordinary input type, but it is not a set: it works for `serde` formats only, the tagged form buffers the whole payload per delivery, the untagged form needs a self-describing format, and the document shows one message for the enum.
