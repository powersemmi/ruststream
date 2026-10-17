---
description: "How the document is produced, serialized and served, and where its payload schemas come from."
---

# Generation

How the document is produced, serialized and served, and where its payload schemas come from.

`build_spec` reads the document from the app value, and the document serializes to JSON or YAML.
The `main` that the app attribute generates prints it, and a self-contained page renders it.
Payload schemas come from a `JsonSchema` derive on each message type.
