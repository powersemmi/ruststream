---
sidebar_position: 3
title: assert_shut_down
description: "TestApp::assert_shut_down asserts a fail-fast failure has shut the service down, as a handler panic does under the default policy."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# `assert_shut_down`

`TestApp::assert_shut_down` asserts a fail-fast failure has shut the service down, as a handler panic does under the default policy.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/testing_service_state.rs#assert-shut-down"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/testing_service_state.rs#assert-shut-down"
```

</TabItem>
</Tabs>

## How it works

The panicking delivery is still driven to a standstill, so the publish that caused it returns normally.
The record of the subscription shows the call that panicked.
After the shutdown every publish through the harness fails with `TestError::ShutDown`.
The assertion fails when the service is still running.

## See also

- [`assert_running`](./assert-running.md): the opposite assertion.
- [`run_result`](./run-result.md): the failure as a value.
- [`subscriber(name)`](../records/subscriber.md): the record that shows the panic.
- [`panicked`](../subscriber-assertions/panicked.md): the call that panicked.
- [`TestApp::assert_shut_down`](api:ruststream::testing::TestApp::assert_shut_down), [`TestError::ShutDown`](api:ruststream::testing::TestError::ShutDown)
