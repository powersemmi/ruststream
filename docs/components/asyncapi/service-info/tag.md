---
sidebar_position: 4
title: Tag
description: "A Tag is a label that groups the service among its neighbours, with an optional description and link."
---

# Tag

A `Tag` is a label that groups the service among its neighbours, with an optional description and link.

[`AppInfo::tag`](api:ruststream::runtime::AppInfo::tag) adds one; each call adds another.
[`Tag::new`](api:ruststream::Tag::new) takes the label.
[`description`](api:ruststream::Tag::description) says what it means, and [`external_docs`](api:ruststream::Tag::external_docs) where it is documented.
The document also tags each operation with the protocol of its [server](../servers/with-broker-labeled.md).
The example runs with `cargo test --example asyncapi_info --no-default-features --features asyncapi`.

## Example

{/* single-form: the service info is built from plain values, the same with and without the macros */}
```rust reference="examples/asyncapi_info.rs#tag"
```
