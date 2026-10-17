---
sidebar_position: 2
title: Contact
description: "Contact names who answers for the service: a person or a team, a URL and an e-mail address, each optional."
---

# Contact

`Contact` names who answers for the service: a person or a team, a URL and an e-mail address, each optional.

[`AppInfo::contact`](api:ruststream::runtime::AppInfo::contact) sets it.
[`Contact::new`](api:ruststream::Contact::new) starts empty, and [`name`](api:ruststream::Contact::name), [`url`](api:ruststream::Contact::url) and [`email`](api:ruststream::Contact::email) fill the fields.
A contact with no field filled is left out of the document.
The example runs with `cargo test --example asyncapi_info --no-default-features --features asyncapi`.

## Example

{/* single-form: the service info is built from plain values, the same with and without the macros */}
```rust reference="examples/asyncapi_info.rs#contact"
```
