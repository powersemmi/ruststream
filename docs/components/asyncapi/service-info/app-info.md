---
sidebar_position: 1
title: Info
description: "AppInfo is the info section of the document: the title and the version, a description and the terms of service."
---

# Info

`AppInfo` is the `info` section of the document: the title and the version, a description and the terms of service.

[`AppInfo::new`](api:ruststream::runtime::AppInfo::new) takes the title and the version, which every document carries.
[`description`](api:ruststream::runtime::AppInfo::description) and [`terms_of_service`](api:ruststream::runtime::AppInfo::terms_of_service) fill two optional fields.
The [contact](./contact.md), the [license](./license.md), the [tags](./tag.md) and the [external docs](./external-docs.md) are pages of their own.
A field left empty is left out of the document.
The app is built from the same `AppInfo`, so the service's logs and its document name it the same way.
The example runs with `cargo test --example asyncapi_info --no-default-features --features asyncapi`.

## Example

{/* single-form: the service info is built from plain values, the same with and without the macros */}
```rust reference="examples/asyncapi_info.rs#info"
```
