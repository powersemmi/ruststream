---
sidebar_position: 4
title: shutdown
description: "TestApp::shutdown tears the service down gracefully and returns what run would return, so a fail-fast failure fails the test."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# `shutdown`

`TestApp::shutdown` tears the service down gracefully and returns what `run` would return, so a fail-fast failure fails the test.

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

## How it works

It stops the dispatch loops, waits for in-flight handlers and post-settle continuations, and shuts each broker down.
The wait for handlers and continuations is bounded by the app's shutdown timeout.
The result is `Ok` for a healthy run and `RustStreamError::Dispatch` after a fail-fast failure, such as a handler panic under the default policy.
`shutdown` takes the harness by value, so nothing can be published after it.

## See also

- [`run_result`](../service-state/run-result.md): the same result while the harness is still running.
- [`assert_shut_down`](../service-state/assert-shut-down.md): a fail-fast failure stopped the service.
- [`TestApp::shutdown`](api:ruststream::testing::TestApp::shutdown), [`RustStreamError`](api:ruststream::runtime::RustStreamError)
