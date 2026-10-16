---
sidebar_position: 1
title: run_result
description: "TestApp::run_result returns what the service run would return now: Ok while it is healthy, an error once a fail-fast failure stopped it."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# `run_result`

`TestApp::run_result` returns what the service `run` would return now: `Ok` while it is healthy, an error once a fail-fast failure stopped it.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/testing_service_state.rs#run-result"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/testing_service_state.rs#run-result"
```

</TabItem>
</Tabs>

## How it works

A fail-fast failure is a handler panic under the default failure policy, or a decode failure under a `fail_fast` decode policy.
After one the result is `RustStreamError::Dispatch` with the subscription and the reason.
`run_result` reads the harness without consuming it, so a test can check it between publishes.
`shutdown` returns the same result at the end of a test.

## See also

- [`assert_running`](./assert-running.md), [`assert_shut_down`](./assert-shut-down.md): the same state as an assertion.
- [`shutdown`](../starting-and-stopping/shutdown.md): the result at teardown.
- [`TestApp::run_result`](api:ruststream::testing::TestApp::run_result), [`RustStreamError`](api:ruststream::runtime::RustStreamError)
