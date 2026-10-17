---
sidebar_position: 6
title: HTTP
description: "An HTTP server owns the process and the service runs beside it: started before the server, stopped after it."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# HTTP

An HTTP server owns the process and the service runs beside it: started before the server, stopped after it.

The service starts with [`start`](./start.md) before the server accepts traffic, so a startup failure stops the process early.
[`publisher`](api:ruststream::runtime::RunningApp::publisher) pairs a bound token with its connected broker, for a task beside the service that publishes.
The example's relay task publishes the orders an HTTP request stored in a transactional outbox.
The server stops on `Ctrl+C`, or when `stopping` reports a fail-fast failure of the service.
The service then drains with [`shutdown`](./shutdown.md).

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/http_outbox.rs#wiring"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/http_outbox.rs#wiring"
```

</TabItem>
</Tabs>
