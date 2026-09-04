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

输入走的是服务自己发布时用的同一个发布构建器：`message(&value)` 把带 `#[derive(Outgoing)]` 的值
按它的类型选定的那种传输方式发出，`with_headers(&meta)` 附上类型化的消息头契约，而当值的类型没有声明
目的地时，由 `to(name)` 指定 subject。本身不是模型的字节 - 用来触发解码策略的无法解码的载荷，或者
[自己反序列化字节](subscribers.md#raw-subscribers)的处理器的输入 - 包在一个
`#[derive(Outgoing, Serialized)]` 的 newtype 里走同一个入口，于是测试说得出自己注入的是什么，而不是
往 subject 上丢一串匿名字节。

### 对处理器做断言

`tb.broker::<B>().subscriber(name)` 返回一个流式构建器，用来断言该处理器收到了什么：

| 方法 | 断言内容 |
|---|---|
| `assert_called_once()` / `assert_called(n)` / `assert_not_called()` | 调用次数 |
| `with(&value)` | 最近一次调用的那唯一一条投递解码（用默认编解码器）之后等于 `value` |
| `with_raw(bytes)` | 最近一次调用的那唯一一份原始载荷 |
| `settled(HandlerOutcome::ack())` | 最近一次调用所承载的一切是怎样结算的 |
| `assert_batch_sizes(&[2, 1])` | 交到函数体手里的那几个批次，按到达顺序 |
| `assert_outcome(Outcome::Drop)` | 归类之后的结算结果（ack / nack / drop / 解码失败 / panic） |
| `panicked()` | 处理器在最后一次调用上发生了 panic |
| `assert_last_failed_to_decode()` | 载荷解码失败 |

这些方法数的是处理器的调用，不是消息。单条消息的处理器每来一次投递就被调用一次，两者因此重合；
批处理器每来一个批次被调用一次，所以 `assert_called_once()` 表示到达了一个批次，不论它有多大，
`settled(..)` 覆盖这个批次的每个元素，而 `received_raw()` 仍然逐个列出这些元素。指名一份期望载荷的
那两个断言（`with`、`with_raw`）会报出批次的大小，而不是默默去检查其中某一个元素。在处理器主体运行
之前就被解码策略拒掉的元素，由该策略结算，因此不在处理器看到的那个批次里。

一个批次是整块交到函数体手里的，一个批次因此就是一次调用。批次的边界落在哪里，由 Broker 回应挂载点
写下的 [`batch(n)`](subscribers.md#batch-subscribers) 来决定，而 `assert_batch_sizes` 正是看这件事
的地方：三条记录的日志在 `batch(2)` 之下回放，到达函数体时是 `[2, 1]` - 两次调用，因为 Broker 攒出
了两个批次。
单条消息的处理器每来一次投递就被调用一次，所以同一轮报出来是 `[1, 1, 1]`。

!!! note "怎样凑出多于一个元素的批次"
    `tb.message(&value).publish()` 在返回之前会把整个反应推到静止，而一个静止下来的反应会关闭那些
    在客户端攒批次的 Broker 的当前批次。因此一次注入一条消息，无论挂载点写下多大的尺寸，得到的都是
    每条消息一个批次，每个批次只有一个元素。要凑出一个批次，就在应用组装之前从 Broker 取一个生产者
    句柄，用它把整串消息发完（这一路上什么都不会静止），最后用 `tb.settle()` 把反应推到静止一次。
    原生支持批次投递的 Broker 不受影响：那里由 Broker 决定一个批次在哪里结束。

`tb.broker::<B>().published::<T>(name)` 断言处理器向下游发布了什么，数据取自 Broker 的发布日志：
`.assert_called_once()` / `.assert_called(n)` / `.assert_not_called()` 固定发布次数，
`.with(&Receipt { id: 1 })` / `.with_raw(bytes)` 固定最近一条载荷，`.with_header("x-app", b"1")`
固定发布中间件或 [`PublishTransform`](publishing.md) 在出站时盖上的消息头。

除了这些断言，消息本身也可以取出来做自定义检查：`subscriber(name).received::<T>()` /
`.received_raw()` 返回处理器收到的内容，`published::<T>(name).decoded()` / `.messages()` 返回发布到
该通道的每一条消息，两者都保持原有顺序。

还有两个视图保留了扁平列表丢掉的信息。`subscriber(name).batches::<T>()` / `.batches_raw()` 按调用把
投递分组，每次调用一个内层向量，因此测试可以固定这个流是怎样切成批次的，而 `received::<T>()` 会把这
条边界
抹平。`subscriber(name).outcomes()` 按顺序返回每次调用归类之后的结算结果，重投递的序列（先 nack，
重投递再 ack）就是与它比对的；`settled(..)` 和 `assert_outcome(..)` 只读最近一次调用。

解码用的辅助方法（`with`、`received`、`decoded`）使用默认编解码器。如果某个处理器或发布者是用别的
编解码器挂载的（`with_broker_codec`、`Router::with_codec`），就用 `_with` / `with_codec` 变体把它显式传入：
`subscriber(name).with_codec(&CborCodec, &expected)`、`.received_with(&CborCodec)`、
`published::<T>(name).with_codec(&CborCodec, &expected)`、`.decoded_with(&CborCodec)`；而 `with_raw` /
`received_raw` / `messages` 与编解码器无关。

### 自己做序列化的消息 { #a-message-that-serializes-itself }

[字节路径](codecs.md#binary-protocols-are-not-codecs)上的值与线之间不隔任何东西，而上面每一个
类型化断言里都隔着一个编解码器：`with(&value)`、`received::<T>()` 以及它们的 `_with(codec)` 变体
都要用编解码器解码，而带 `Serialized` / `Deserialized` 的类型根本不解析编解码器。这条路径上的测试
靠的是那两个与编解码器无关的断言 - `with_raw(bytes)` 断言载荷，`received_raw()` 把投递读回来 -
其余由类型自己的格式提供：

```rust
--8<-- "tests/self_serialising.rs:assertions"
```

期望的字节来自格式，而不是来自测试工具：自己手写的帧短到可以直接写出来，生成的消息则自己产出
字节，所以 `prost` 消息写成 `with_raw(&order.encode_to_vec())`。把一次投递读回来，用的是
`Deserialized::from_payload`，作用在 `received_raw()` 返回的持有型 `Bytes` 上 - 正是这条路径在
入站时跑过的那个读取器，因此断言针对的是模型类型，全程没有编解码器。发布一侧的分界也一样：
`published::<T>(name).with_raw(bytes)` 和 `.messages()` 与编解码器无关，`.with(&value)` 和
`.decoded()` 则有关。

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
