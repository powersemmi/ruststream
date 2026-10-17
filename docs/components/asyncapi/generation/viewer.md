---
sidebar_position: 4
title: Viewer
description: "render_viewer_html returns a self-contained HTML page that renders the document with the AsyncAPI React component."
---

# Viewer

[`render_viewer_html`](api:ruststream::asyncapi::render_viewer_html) returns a self-contained HTML page that renders the document with the AsyncAPI React component.

The page loads the document from the URL it is given, so the service serves two routes: the document and the page.
The function renders a string; serving it is the job of the HTTP stack the service already runs.
`examples/asyncapi_http.rs` in the repository serves both routes with axum.
The component and its stylesheet load from jsDelivr, at a pinned version.
[`ViewerOptions`](api:ruststream::asyncapi::ViewerOptions) sets the page [`title`](api:ruststream::asyncapi::ViewerOptions::title) and another asset base with [`cdn_base`](api:ruststream::asyncapi::ViewerOptions::cdn_base): a mirror, or the service's own assets where the network has no way out.
The example runs with `cargo test --example asyncapi_generation --features macros,memory,asyncapi`.

## Example

{/* single-form: the viewer page is the same code with and without the macros */}
```rust reference="examples/asyncapi_generation.rs#viewer"
```
