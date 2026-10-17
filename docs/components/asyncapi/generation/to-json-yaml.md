---
sidebar_position: 2
title: Formats
description: "to_json and to_yaml serialize the document in the two formats AsyncAPI tools read."
---

# Formats

[`to_json`](api:ruststream::asyncapi::Spec::to_json) and [`to_yaml`](api:ruststream::asyncapi::Spec::to_yaml) serialize the document in the two formats AsyncAPI tools read.

`to_json` writes pretty-printed JSON, and `to_yaml` writes YAML.
Each returns its serializer's error; a document [`build_spec`](./build-spec.md) built serializes without one.
Servers, channels, operations and components are maps sorted by name, so the output is the same on every run and a change reads as a plain diff in review.
The [command](./cli-gen.md) the generated `main` understands prints the same text.
The example runs with `cargo test --example asyncapi_generation --features macros,memory,asyncapi`.

## Example

{/* single-form: serializing the document is the same code with and without the macros */}
```rust reference="examples/asyncapi_generation.rs#formats"
```
