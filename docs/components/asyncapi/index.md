---
description: "The asyncapi feature generates the AsyncAPI 3.1 document of a service from its app value, with build_spec."
---

# AsyncAPI

The `asyncapi` feature generates the AsyncAPI 3.1 document of a service from its app value, with [`build_spec`](api:ruststream::asyncapi::build_spec).

The document is read from what the app registers, so it describes the handlers the service mounts and nothing else.
It lists the channels and operations, the message schemas, the servers and who owns the service.
It is built on demand, from a test, a command or an HTTP route, and costs the running service nothing.
