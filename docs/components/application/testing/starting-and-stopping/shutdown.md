---
sidebar_position: 4
title: shutdown
description: "TestApp::shutdown tears the service down the way the service does, hooks included, and returns what run would return, so a fail-fast failure fails the test."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# shutdown

[`TestApp::shutdown`](api:ruststream::testing::TestApp::shutdown) tears the service down gracefully and returns what `run` would return, so a fail-fast failure fails the test.

It is the service's own [shutdown](../../running/shutdown.md), step for step.
It runs `on_shutdown`, stops the dispatch loops, and waits for in-flight handlers and post-settle continuations.
It then shuts the brokers down in reverse order of registration and runs `after_shutdown`.
The wait for handlers and continuations is bounded by the app's shutdown timeout.
A failing shutdown hook is logged, and the shutdown goes on, as in the service.
The result is `Ok` for a healthy run and [`RustStreamError::Dispatch`](api:ruststream::runtime::RustStreamError) after a fail-fast failure, such as a handler panic under the default policy.
A broker that fails to shut down returns the service's error for it too.
[`run_result`](../service-state/run-result.md) gives the same result while the harness is still running, and [`assert_shut_down`](../service-state/assert-shut-down.md) asserts the failure.
`shutdown` takes the harness by value, so nothing can be published after it.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/testing_starting.rs#shutdown"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/testing_starting.rs#shutdown"
```

</TabItem>
</Tabs>
