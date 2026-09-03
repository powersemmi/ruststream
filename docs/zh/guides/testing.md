# 测试

RustStream 服务在两个层面上测试：

1. **进程内单元测试**用 [`TestApp`](#unit-testing-a-service-with-testapp) 测试套件驱动你真实的
   处理器、中间件和编解码器，不需要服务器、不需要 docker、也不需要网络（进程内 Broker 的 `connect`
   不做 I/O）。这是默认路径，它端到端地覆盖处理器逻辑：解码、分发、结算结果
   （ack / nack / drop / panic / 解码失败），以及处理器发布出去的任何消息。
2. **集成测试**跑在真实 Broker 上，由一个环境变量挡在开关之后，覆盖只有真实服务器才有的语义
   （持久化消费者、重新投递计时器、分区）。

!!! warning "这套测试套件建模了什么，又没有建模什么"
    这套测试套件驱动的是 Broker 的**进程内传输层**：发布一条消息会把它扇出给 subject 匹配的订阅者，
    通过真实的分发路径运行你的处理器，并记录结算结果以及向下游发布的消息。它**不会**建模 JetStream
    的持久化游标、`ack_wait` 重新投递、`max_ack_pending`、保留策略、Kafka 的偏移量或消费者组，也不会
    建模 RabbitMQ 的 exchange 和死信路由。这些都属于真实 Broker 的范畴，放到
    [集成测试](#integration-tests-against-a-real-broker)里去测。

    `MemoryBroker` 是什么、不是什么，写在它自己的页面上：[内存 Broker](../brokers/memory.md)。

## 用 `TestApp` 对服务做单元测试 { #unit-testing-a-service-with-testapp }

`TestApp` 接收一个已经构建好的 `RustStream` 应用，连接它的各个 Broker（进程内总线不做 I/O），挂载
处理器，并记录每一次投递。你发布输入，而这次发布会把整条反应链（处理器、它向下游的发布、跨 Broker 的
级联）一直驱动到彻底静止之后才返回。然后你再去断言。

被测的处理器（在真实服务里它位于你的处理器模块中，由测试导入）：

=== "宏"

    ```rust
    --8<-- "tests/doc_testing_memory.rs:handler"
    ```

=== "手写"

    ```rust
    --8<-- "tests/manual_doc_testing_memory.rs:handler"
    ```

测试本身：

=== "宏"

    ```rust
    --8<-- "tests/doc_testing_memory.rs:test"
    ```

=== "手写"

    ```rust
    --8<-- "tests/manual_doc_testing_memory.rs:test"
    ```

!!! info "该测试会在本仓库的 CI 中运行"
    上面的代码嵌入自
    [`tests/doc_testing_memory.rs`](https://github.com/powersemmi/ruststream/blob/main/tests/doc_testing_memory.rs)，
    每次改动 `cargo test --all-features` 都会运行它，因此该示例不会悄悄腐烂。

在 dev-dependencies 中启用 `testing` feature：

```toml
[dev-dependencies]
ruststream = { version = "0.7", features = ["testing", "memory", "macros", "json"] }
```

### 指定 Broker

`tb.broker::<MemoryBroker>()` 按类型指定 Broker；当一个服务挂载了多个 Broker 而它们的 subject 又
互相冲突时，`tb.broker_named("ingress")` 按 [`with_broker_labeled`](asyncapi.md) 给出的标签来指定。
不带作用域的 `tb.message(&value).to(name)` 是给单 Broker 应用准备的便捷写法，一旦注册了不止一个
Broker，它就报告 `TestError::Ambiguous`。

输入走的是服务自己发布时用的同一个发布构建器：`message(&value)` 编码带 `#[derive(Outgoing)]` 的值，
`raw(bytes)` 原样发送字节（负载无法解码的场景，也是裸订阅者唯一可用的写法），`with_headers(&meta)`
附上类型化的消息头契约，而当值的类型没有声明目的地时，由 `to(name)` 指定 subject。

### 对处理器做断言

`tb.broker::<B>().subscriber(name)` 返回一个流式构建器，用来断言该处理器收到了什么：

| 方法 | 断言内容 |
|---|---|
| `assert_called_once()` / `assert_called(n)` / `assert_not_called()` | 投递次数 |
| `with(&value)` | 最近一次投递解码（用默认编解码器）之后等于 `value` |
| `with_raw(bytes)` | 最近一次的原始载荷 |
| `settled(HandlerOutcome::ack())` | 结算的方式 |
| `assert_outcome(Outcome::Drop)` | 归类之后的结算结果（ack / nack / drop / 解码失败 / panic） |
| `panicked()` | 处理器在最后一次投递上发生了 panic |
| `assert_last_failed_to_decode()` | 载荷解码失败 |

`tb.broker::<B>().published::<T>(name)` 断言处理器向下游发布了什么，数据取自 Broker 的发布日志：
`.assert_called_once().with(&Receipt { id: 1 })`。

除了这些断言，消息本身也可以取出来做自定义检查：`subscriber(name).received::<T>()` /
`.received_raw()` 返回处理器收到的内容，`published::<T>(name).decoded()` / `.messages()` 返回发布到
该通道的每一条消息，两者都保持原有顺序。

解码用的辅助方法（`with`、`received`、`decoded`）使用默认编解码器。如果某个处理器或发布者是用别的
编解码器挂载的（`include_with` / `with_broker_codec`），就用 `_with` / `with_codec` 变体把它显式传入：
`subscriber(name).with_codec(&CborCodec, &expected)`、`.received_with(&CborCodec)`、
`published::<T>(name).with_codec(&CborCodec, &expected)`、`.decoded_with(&CborCodec)`；而 `with_raw` /
`received_raw` / `messages` 与编解码器无关。

### 对 Out 槽位做断言 { #asserting-on-out-slots }

处理器的 [`Out` 槽位](publishing.md#named-slots)同时也是它在测试中的身份：`tb.out::<Marker>()` 恰好
返回经由该注入发布者发出的消息，包含目的地和消息头，并且跨所有 Broker，断言接口与 `published`
相同（`assert_called_once`、`with_raw`、`messages`；链上 `.decoded_as::<T>()` 即可用类型化的 `with`）。
槽位视图只是多给出了归属信息：Broker 按通道记录的发布日志看到的是同一批消息。

```rust
--8<-- "tests/out_slots.rs:slot_capture"
```

有些发布会离开处理器任务（比如另一个 spawn 出来的任务、或者一个已结算的、拥有所有权的事务的
缓冲区），它们不会归属到该槽位上；这类发布改为对 Broker 的发布日志做断言。

### 失败策略、panic 与关闭

测试套件在应用自身真实的 `FailurePolicy` 之下运行分发，因此负面测试也是一等路径。在默认的
`panic = fail_fast` 之下，处理器 panic 会像在生产中一样把服务拆掉：

```rust
--8<-- "tests/testing_harness.rs:panic"
```

在 `on_failure(panic = skip)` 之下，运行时会对 panic 执行 ack，消费继续进行，因此
`tb.assert_running()` 成立。
`run_result()` 返回真实的 [`run`](lifespan.md) 会返回的东西：健康时是 `Ok`，一旦某次 fail-fast 失败
关闭了服务，就是一个错误。

!!! note "捕获 panic 需要 unwinding"
    测试套件依赖运行时的 `catch_unwind`，因此刻意制造的 panic 不会杀死测试线程。用
    `panic = "abort"` 编译出来的构建无法捕获处理器的 panic。

### 延迟重新投递（`retry_after`）

返回 `retry_after(delay)` 的处理器会安排一次延迟重新投递。`publish` 记录下当场的 `NackAfter` 结算并
返回；重新投递则由推进一个暂停的时钟单独驱动：

=== "宏"

    ```rust
    --8<-- "tests/testing_harness.rs:retry_after"
    ```

=== "手写"

    ```rust
    --8<-- "tests/manual_testing_harness.rs:retry_after"
    ```

## 针对真实 Broker 的集成测试 { #integration-tests-against-a-real-broker }

依赖真实 Broker 语义的行为，应当放进一个由环境变量挡住的独立测试集，这样默认的 `cargo test` 才能保持
快速且离线：

<!-- inline-rust: integration-test skeleton with a pseudocode body; it drives a real NatsBroker (external crate) behind an env gate, so it has no compiled home here -->
```rust title="tests/integration_nats.rs"
fn test_url() -> Option<String> {
    std::env::var("NATS_TEST_URL").ok()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn durable_consumer_resumes_after_restart() {
    let Some(url) = test_url() else {
        eprintln!("skipping: set NATS_TEST_URL to run");
        return;
    };
    // connect NatsBroker::new(url), drive the real JetStream consumer ...
}
```

显式针对一个真实运行的服务器执行它：

```bash
docker run -d -p 4222:4222 nats:latest -js
NATS_TEST_URL=nats://127.0.0.1:4222 cargo test --test integration_nats
```

处理器逻辑归内存路径，Broker 语义归真实路径。让两个测试集覆盖同一批处理器模块，生产代码才有唯一的
事实来源。

!!! note "正在写一个 Broker crate？"
    让 `TestApp` 能在某个 Broker 上跑起来的那套机制，也就是进程内传输层和 `TestableBroker` 契约，
    属于 Broker 作者这一侧的故事。相关内容见
    [Broker 作者：测试支持](../broker-authors/index.md#test-support)与
    [Conformance](../broker-authors/conformance.md)。
