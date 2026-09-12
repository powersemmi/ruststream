# 测试

RustStream 服务在两个层面上测试：

1. **进程内单元测试**在 [`TestApp`](#unit-testing-a-service-with-testapp) 测试套件中运行你真实的
   处理器、中间件和编解码器：不需要服务器，不需要 docker，不需要网络。这是默认路径，它完整覆盖
   处理器的逻辑：解码、分发、结算结果（ack / nack / drop / panic / 解码失败），以及处理器向下游
   发布的一切消息。
2. **集成测试**运行在真实 Broker 上，由环境变量开启，覆盖只有真实服务器才有的语义：持久化消费者、
   重新投递计时器、分区。

!!! warning "进程内传输覆盖什么"
    测试套件运行在 Broker 的**进程内传输**上：把一次发布投递给 subject 匹配的订阅者，在真实的分发
    路径上执行处理器，并记录结算结果和向下游的每一次发布。存储与重新投递的语义由真实服务器决定，
    在[集成测试](#integration-tests-against-a-real-broker)里检验这部分行为。

    `MemoryBroker` 是什么，写在它自己的页面上：[内存 Broker](../brokers/memory.md)。

## 用 `TestApp` 对服务做单元测试 { #unit-testing-a-service-with-testapp }

`TestApp` 接收一个组装好的 `RustStream` 应用，连接它的各个 Broker，挂载处理器，并记录每一次投递。
进程内总线的连接不做 I/O。你发布一条输入消息。这次发布把整个反应推进到静止之后才返回：处理器、
它向下游的发布、跨 Broker 的级联。之后你再做断言。

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
    每次改动，`cargo test --all-features` 都会运行它，该示例因此不会悄悄过时。

在 dev-dependencies 中启用 `testing` feature：

```toml
[dev-dependencies]
ruststream = { version = "0.7", features = ["testing", "memory", "macros", "json"] }
```

### 指定 Broker

`tb.broker::<MemoryBroker>()` 按类型指定 Broker。当服务挂载了多个 Broker 而它们的 subject 又互相
重叠时，`tb.broker_named("ingress")` 按 [`with_broker_labeled`](asyncapi.md) 给出的标签指定。
在只有一个 Broker 的应用里，`tb.message(&value).to(name)` 不指名 Broker 也能工作。注册了不止一个
Broker 时，它返回 `TestError::Ambiguous`。

输入走的是服务自己发布时用的同一个发布构建器。`message(&value)` 把带 `#[derive(Outgoing)]` 的值按
它的类型选定的方式发出，`with_headers(&meta)` 附上类型化的消息头契约，
当值的类型没有声明 subject 时，由 `to(name)` 指定。

本身不是模型的字节，包在带 `#[derive(Outgoing, Serialized)]` 的 newtype 里走同一个入口。给解码策略
准备的无法解码的载荷，以及[自己反序列化字节](subscribers.md#raw-subscribers)的处理器的输入，都这样
送进去：测试说得出自己注入的是什么。

### 对处理器做断言

`tb.broker::<B>().subscriber(name)` 返回一个断言构建器，检查这个处理器收到了什么：

| 方法 | 断言内容 |
|---|---|
| `assert_called_once()` / `assert_called(n)` / `assert_not_called()` | 调用次数 |
| `with(&value)` | 最近一次调用的唯一一条投递用默认编解码器解码后等于 `value` |
| `with_raw(bytes)` | 最近一次调用的唯一一份原始载荷 |
| `settled(HandlerOutcome::ack())` | 最近一次调用收到的一切是怎样结算的 |
| `assert_batch_sizes(&[2, 1])` | 函数体收到的批次，按到达顺序 |
| `assert_outcome(Outcome::Drop)` | 归类之后的结算结果（ack / nack / drop / 解码失败 / panic） |
| `panicked()` | 处理器在最后一次调用中 panic |
| `assert_last_failed_to_decode()` | 载荷解码失败 |

这些断言统计的是处理器的调用，不是消息。单条消息的处理器每来一次投递被调用一次，调用和消息因此重合。
批处理器每来一个批次被调用一次：`assert_called_once()` 表示到达了一个批次，不论它有多大，
`settled(..)` 覆盖批次里的每个元素，而 `received_raw()` 仍然逐个列出这些元素。

`with` 和 `with_raw` 只指名一份期望载荷，所以在批次上这样的断言不成立，并报出批次的大小。解码策略
在函数体之前拒绝的元素由该策略结算，处理器的批次里没有它。

批次的边界由 Broker 划定，依据是挂载点写下的 [`batch(n)`](subscribers.md#batch-subscribers)，
`assert_batch_sizes` 看到的正是这些边界：三条记录的日志在 `batch(2)` 之下回放，到达函数体时是
`[2, 1]`。同一轮日志交给单条消息的处理器，得到的是 `[1, 1, 1]`。

!!! note "怎样凑出多于一个元素的批次"
    `tb.message(&value).publish()` 在返回之前把整个反应推到静止，而反应一旦静止，在客户端攒批次的
    Broker 就会关闭当前的批次。因此一次注入一条消息，得到的是每条消息一个批次，每个批次只有
    一个元素，不论挂载点写下多大的尺寸。在应用组装之前从 Broker 取一个发布者句柄，用它把整串消息
    发完，这一路上反应不会静止。最后用 `tb.settle()` 把反应推到静止一次。原生按批次投递的 Broker
    不受影响，那里由 Broker 自己决定一个批次在哪里结束。

`tb.broker::<B>().published::<T>(name)` 读取 Broker 的发布日志，检查处理器向下游发布了什么：
`.assert_called_once()` / `.assert_called(n)` / `.assert_not_called()` 检查发布次数，
`.with(&Receipt { id: 1 })` / `.with_raw(bytes)` 检查最近一条载荷，`.with_header("x-app", b"1")`
检查发布中间件或 [`PublishTransform`](publishing.md) 在出站时加上的消息头。

消息本身也可以取出来做自定义检查：`subscriber(name).received::<T>()` / `.received_raw()` 返回处理器
收到的内容，`published::<T>(name).decoded()` / `.messages()` 返回发布到该通道的每一条消息。
两个列表都按到达顺序排列。

还有两个视图保留了扁平列表丢掉的信息。`subscriber(name).batches::<T>()` / `.batches_raw()` 把投递
按调用分组，每次调用一个内层向量：测试因此看得到这个流是怎样切成批次的，而 `received::<T>()`
不保留这条边界。`subscriber(name).outcomes()` 按顺序返回每次调用归类之后的结算结果，重新投递的
序列（先 nack，重投递时 ack）就用它来核对；`settled(..)` 和 `assert_outcome(..)` 只读最近一次调用。

解码用的辅助方法（`with`、`received`、`decoded`）使用默认编解码器。如果某个处理器或发布者是用别的
编解码器挂载的（`with_broker_codec`、`Router::with_codec`），就用 `_with` / `with_codec` 变体把它显式传入：
`subscriber(name).with_codec(&CborCodec, &expected)`、`.received_with(&CborCodec)`、
`published::<T>(name).with_codec(&CborCodec, &expected)`、`.decoded_with(&CborCodec)`。
`with_raw` / `received_raw` / `messages` 不使用编解码器。

### 自己做序列化的消息 { #a-message-that-serializes-itself }

[字节路径](codecs.md#binary-protocols-are-not-codecs)上的值不经过编解码器，而上面每一个类型化断言
都要用编解码器：`with(&value)`、`received::<T>()` 以及它们的 `_with(codec)` 变体都靠它解码，带
`Serialized` / `Deserialized` 的类型则根本不解析编解码器。这条路径上的测试靠两个与编解码器无关的
断言：`with_raw(bytes)` 检查载荷，`received_raw()` 把投递读回来。其余由类型自己的格式提供：

```rust
--8<-- "tests/self_serialising.rs:assertions"
```

期望的字节来自格式，而不是来自测试套件：手写的帧短到可以整个写出来，生成的消息自己产出字节，
`prost` 消息因此写成 `with_raw(&order.encode_to_vec())`。把一次投递读回来，用的是
`Deserialized::from_payload`，作用在 `received_raw()` 返回的、拥有所有权的 `Bytes` 上。这正是入站时
解析输入的那个读取器，所以断言比对的是模型类型，全程没有编解码器。发布一侧同样分成两半：
`published::<T>(name).with_raw(bytes)` 和 `.messages()` 不使用编解码器，`.with(&value)` 和
`.decoded()` 使用。

### 对 Out 槽位做断言 { #asserting-on-out-slots }

处理器的 [`Out` 槽位](publishing.md#named-slots)在测试里也是它的身份。`tb.out::<Marker>()` 恰好返回
经由这个注入的发布者发出的消息，连同目的地和消息头，并且跨所有 Broker。断言接口与 `published` 相同：
`assert_called_once`、`with_raw`、`messages`；要用类型化的 `with`，在链上加 `.decoded_as::<T>()`。
槽位给这些消息加上归属：Broker 按通道记录的发布日志看到的是同一批消息。

```rust
--8<-- "tests/out_slots.rs:slot_capture"
```

离开处理器任务的发布不归属到槽位上，例如另一个 spawn 出来的任务，或者一个已结算的自有事务的
缓冲区。这类发布对着 Broker 的发布日志断言。

槽位视图还会记录每次发布所带的[逐条消息的 Broker 设置](publishing.md#broker-settings-per-message)。
因此，Broker 映射到协议字段而不是消息头的那种设置，你同样可以对它做断言。`with_options` 点名
Broker 的设置类型并比对取值；`assert_options_default` 断言这次发布上没有任何步骤碰过设置，
生效的是策略定下的默认值：

=== "宏"

    ```rust
    --8<-- "tests/publish_options.rs:options_assert"
    ```

=== "手写"

    ```rust
    --8<-- "tests/manual_publish_options.rs:options_assert"
    ```

两者都读最近一次发布，和 `with_header` 一样。设置只记录在槽位视图上。Broker 的发布日志看到消息时，
这些设置已经映射进了它自己的协议，所以向 `published::<T>(name)` 要设置会 panic。启动钩子或应用状态
拿到的裸发布者不属于任何槽位，它的设置不会记录在任何地方。

### 失败策略、panic 与关闭

测试套件在应用自己真实的 `FailurePolicy` 之下运行分发，负面测试因此也是完整的场景。默认的
`panic = fail_fast` 之下，处理器 panic 会像在生产中一样停掉服务：

```rust
--8<-- "tests/testing_harness.rs:panic"
```

`on_failure(panic = skip)` 之下，panic 以 ack 结算，消费继续进行，`tb.assert_running()` 因此成立。
`run_result()` 返回真实的 [`run`](lifespan.md) 会返回的结果：服务还在运行时是 `Ok`，fail-fast 的
失败停掉服务之后是一个错误。

!!! note "捕获 panic 需要栈展开"
    测试套件依靠运行时的 `catch_unwind`，刻意制造的 panic 因此不会终止测试线程。用
    `panic = "abort"` 编译出来的构建捕获不到处理器的 panic。

### 延迟重新投递（`retry_after`）

返回 `retry_after(delay)` 的处理器会安排一次延迟重新投递。`publish` 记录下当场的 `NackAfter` 结算
就返回，重新投递由你单独驱动，推进暂停的时钟：

=== "宏"

    ```rust
    --8<-- "tests/testing_harness.rs:retry_after"
    ```

=== "手写"

    ```rust
    --8<-- "tests/manual_testing_harness.rs:retry_after"
    ```

## 针对真实 Broker 的集成测试 { #integration-tests-against-a-real-broker }

依赖真实 Broker 语义的行为，放进单独的测试集，由环境变量开启。默认的 `cargo test` 因此保持快速，
也不需要网络：

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

对着运行中的服务器显式运行它：

```bash
docker run -d -p 4222:4222 nats:latest -js
NATS_TEST_URL=nats://127.0.0.1:4222 cargo test --test integration_nats
```

处理器的逻辑由进程内路径检验，Broker 的语义由真实服务器检验。让两个测试集覆盖同一批处理器模块，
生产代码才有唯一的事实来源。

!!! note "正在写 Broker crate？"
    进程内传输和 `TestableBroker` 契约让 `TestApp` 能在一个 Broker 上运行，这是 Broker 作者的事。
    这两件事写在
    [Broker 作者：测试支持](../broker-authors/index.md#test-support)和
    [Conformance](../broker-authors/conformance.md) 里。
