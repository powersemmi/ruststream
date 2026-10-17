---
sidebar_position: 6
title: AppId
description: "AppId is the identifier of the service at the root of the document, a URI parsed when it is built."
---

# AppId

`AppId` is the identifier of the service at the root of the document, a URI parsed when it is built.

AsyncAPI requires the id to be a URI.
[`AppId`](api:ruststream::AppId) checks the part a URI scheme must have: a letter, then letters, digits, `+`, `-` or `.`, then a colon.
What follows the colon is the scheme's business.
A bare title fails with [`AppIdError::NoScheme`](api:ruststream::AppIdError::NoScheme), and a malformed scheme with `MalformedScheme`.
[`AppInfo::id`](api:ruststream::runtime::AppInfo::id) sets it, and only a parsed `AppId` fits there.
An id from the deployment's configuration therefore fails where the app is built, never in a published document.
The example runs with `cargo test --example asyncapi_info --no-default-features --features asyncapi`.

## Example

{/* single-form: the service info is built from plain values, the same with and without the macros */}
```rust reference="examples/asyncapi_info.rs#app_id"
```
