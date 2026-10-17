---
description: "A service runs until a signal, until a future of yours, or beside another server that owns the process."
---

# Running

A service runs until a signal, until a future of yours, or beside another server that owns the process.

- [Run](./run.md): `run` serves until `SIGINT` or `SIGTERM`.
- [Until](./until.md): `run_until` serves until a future resolves.
- [Start](./start.md): `start` hands back a `RunningApp` and returns.
- [Shutdown](./shutdown.md): the graceful stop of a started service.
- [Health](./health.md): the service's state for a probe.
- [HTTP](./http.md): the service beside an HTTP server.
