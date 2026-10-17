---
description: "Logs, metrics and traces of a running service: each comes from a layer on the app and a feature that exports it."
---

# Observability

Logs, metrics and traces of a running service: each comes from a layer on the app and a feature that exports it.

RustStream reports what it does through `tracing` events, and the `logging` feature prints them on the console.
The per-delivery signals come from layers that wrap every handler, so a handler body holds only its own logic.
A layer added to the app wraps the handlers of its routers too.
Prometheus metrics need the `metrics` feature; trace propagation and OpenTelemetry export need the `otel` feature.
