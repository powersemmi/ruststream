---
sidebar_position: 11
title: Message sets
description: "An enum deriving InMessages declares several message types on one subscription: a rule picks the variant, and the document lists one message per variant."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Message sets

An enum deriving [`InMessages`](api:ruststream::InMessages) declares several message types on one subscription: a rule picks the variant, and the document lists one message per variant.

The set is an enum whose variants each wrap one model, and the handler takes a reference to the enum: `&OrderEvent`, or `&[OrderEvent]` for a [batch](./batches.md).
Each model derives [`MessageInfo`](api:ruststream::MessageInfo), because the document lists it by that name.
A model decodes through its own path: the codec of the mount for a `serde` type, its own reader for a type that derives [`Deserialized`](api:ruststream::Deserialized), such as a [Protobuf](./codecs/protobuf.md) message.
One set may hold both, so a JSON event and a Protobuf event share a subscription and each shows its own format in the [document](../asyncapi/reports/message-sets.md).
The derive is the receiving counterpart of [`OutMessages`](api:ruststream::OutMessages), which declares what a slot sends.

The header rule is the one to reach for when the producer can set a header.
`#[in_messages(header = "event")]` on the enum names the header the set reads, and `#[in_messages(value = "..")]` on each variant names the value that picks it.
The chosen variant decodes once; a set costs what a single type costs, plus the header lookup.
A delivery without the header, or with a value no variant declares, is a decode failure naming the header and the value seen, settled by the [failure policy](./failure-policy.md) like any other.
The document carries the rule: each message's headers schema lists the header with its `const` value.

A rule of your own is a function: `#[in_messages(discriminate = path)]` names one that reads the delivery, its headers and raw payload, and returns which kind to decode.
The derive generates the kind enum, `OrderUpdateKind` for a set `OrderUpdate`, with one unit variant per model.
The function returning `None` is a decode failure naming the set.
This is the form for a rule no attribute states: a schema id, a field of the payload, a shape of the bytes.
A broker crate may ship such a function for its own format.

A rule only the decode codec can answer is a type: `#[in_messages(discriminate_with_codec = ByRecord)]` names one that implements [`CodecDiscriminator`](api:ruststream::runtime::CodecDiscriminator).
A schema registry is the case: each record carries a schema id, and only the registry codec knows which record the id stands for.
The type's `kind(codec, msg)` reads the codec of the scope or router and the delivery, and returns the kind the way a function does; `None` is the same decode failure.
An impl that bounds the codec makes the set mount only under codecs that satisfy the bound: a set mounted under a codec that cannot answer is a compile error at the mount site.
The call is static, so a delivery pays only for what the type itself does.
Such a set decodes per codec, so it does not implement `InMessages` itself: it is a handler input everywhere a set is, and a bound of your own written as `T: InMessages` does not accept it.
A broker crate may ship the type for its own registry.

A set with no rule tries its variants left to right, and the first that decodes wins.
The order of the variants is the rule, so put the strictest model first.
Every variant failing is one decode failure that carries each variant's error.
Each attempt costs one decode of that variant, so a delivery of the last variant pays for every variant before it.
Left to right is exact only as far as the formats are strict: a Protobuf reader skips the fields it does not know, so a payload of one Protobuf model may decode as another, and a JSON object with the right fields decodes as any model that has them.
For such sets, name a rule, or order the variants from the strictest to the loosest.

The derive checks the declaration: every variant wraps exactly one model, each model implements `MessageInfo`, a model appears once in a set, the enum takes no generic parameters, and a variant owns its model, so a type that borrows the payload cannot be one.
A set is not a message, so `MessageInfo` is not derived on the set itself.
Each of these is a compile error on the variant or the enum it concerns.
A `serde` enum with `#[serde(tag = "..")]` or `#[serde(untagged)]` is still an ordinary input type, but it is not a set: it works for `serde` formats only, the tagged form buffers the whole payload per delivery, the untagged form needs a self-describing format, and the document shows one message for the enum.

## Example

The header rule, on a set of a JSON model and a Protobuf one:

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

A rule of your own, over the generated kind enum:

{/* single-form: the rule is a plain function; the manual tab writes the same function inside its `InMessages` impl */}
```rust reference="examples/in_messages.rs#discriminate"
```

A rule that asks the decode codec: the codec names records, and the set mounts under it through a router.

{/* single-form: the per-codec decode of such a set is machinery a hand-written set does not name */}
```rust reference="examples/in_messages.rs#discriminate_with_codec"
```

Left to right, the strictest model first:

{/* single-form: a set with no rule is the derive alone; the manual tab's `decode` is the rule written out */}
```rust reference="examples/in_messages.rs#left_to_right"
```
