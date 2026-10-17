---
sidebar_position: 3
title: License
description: "License names the license the service is published under, with an optional URL of its text."
---

# License

`License` names the license the service is published under, with an optional URL of its text.

[`AppInfo::license`](api:ruststream::runtime::AppInfo::license) sets it.
[`License::new`](api:ruststream::License::new) takes the name, an SPDX identifier by convention.
[`url`](api:ruststream::License::url) points at the license text.
The example runs with `cargo test --example asyncapi_info --no-default-features --features asyncapi`.

## Example

{/* single-form: the service info is built from plain values, the same with and without the macros */}
```rust reference="examples/asyncapi_info.rs#license"
```
