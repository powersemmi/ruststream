---
sidebar_position: 5
title: ExternalDocs
description: "ExternalDocs points at documentation kept outside the document: a URL and what the reader finds there."
---

# ExternalDocs

`ExternalDocs` points at documentation kept outside the document: a URL and what the reader finds there.

[`AppInfo::external_docs`](api:ruststream::runtime::AppInfo::external_docs) sets it for the service, and [`Tag::external_docs`](api:ruststream::Tag::external_docs) for a [tag](./tag.md).
[`ExternalDocs::new`](api:ruststream::ExternalDocs::new) takes the URL.
[`description`](api:ruststream::ExternalDocs::description) says what is there.
The example runs with `cargo test --example asyncapi_info --no-default-features --features asyncapi`.

## Example

{/* single-form: the service info is built from plain values, the same with and without the macros */}
```rust reference="examples/asyncapi_info.rs#external_docs"
```
