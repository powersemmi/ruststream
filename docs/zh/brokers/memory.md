# 内存 Broker

`memory` feature 提供了 `MemoryBroker`，一个完整的进程内 Broker：当队列只属于单个应用、而不属于网络
时，选它即可，不需要任何外部服务。默认的 `cargo generate` 模板（`templates/memory`）用的就是它，所以
一个新建的项目零依赖即可运行。

```toml
ruststream = { version = "0.7", features = ["macros", "memory", "json"] }
```

<!-- inline-rust: two-line constructor sketch; the broker in context is exercised by every memory-feature example (e.g. quickstart.rs:app) -->
```rust
use ruststream::memory::MemoryBroker;

let broker = MemoryBroker::new();
```

## 语义

- **名字精确匹配。** 对 `orders` 的订阅只会收到发布到 `orders` 的消息；没有通配符，也没有模式匹配
  （那些是各个 Broker 自己的事，NATS 测试 Broker 就有真正的 subject 匹配）。
- **扇出。** 某个名字的每一个订阅者，都会收到订阅打开之后发布到该名字的每一条消息；更早发布的消息默认
  不会投递，不过 `Seekable` 能力可以从发布日志里把它们重放出来。
- **ack 是空操作；`nack(requeue: true)` 会把同一份载荷重新投递**给同一个订阅者。
- **克隆开销很低。** 克隆之间共享状态，因此测试里持有的一个克隆能观察到应用发布的一切。

它是一个真正的 Broker，而不是测试替身：运行时驱动它走的分发路径，和驱动生产 Broker 的那条完全相同，
因此 handler、它的中间件以及解码在这里的行为，与它们在生产中的行为一致。它不做的是模拟某个具体
Broker 的投递语义 - 持久游标、重新投递计时器、分区、死信路由 - 所以在这里通过的测试，并不说明同一份
代码能在 Kafka 上通过。

## 能力 { #capabilities }

每一个能力 trait 都基于该 Broker 自身的进程内语义原生实现，而不是去模拟另一个 Broker 的行为：

- **请求-响应。** `broker.requester()` 返回一个 `MemoryRequester`，它的 `request` 会在 `reply-to`
  消息头里带上一个唯一的进程内 inbox 再发布，并在第一条投递到该 inbox 的消息到达时完成；`MemoryRequest`
  策略配对出的就是它，因此带 `Out<impl RequestReply, ..>` 约束的槽位绑定到 `MemoryRequest`。响应方从请求
  中读出 `reply-to`，把回复发布到该名字上。无人应答的请求以 `RequestError::Timeout` 失败。
- **批量。** `MemorySubscriber` 实现了 `BatchSubscriber`：一批由第一条 await 到的投递加上此时已经缓冲
  的全部消息组成，上限由 `set_batch_limit` 控制（默认 64）。不满一批也会立即发出，因此不涉及任何截止
  时间定时器。
- **事务。** `MemoryPublish` 策略配对出的 `MemoryPublisher` 同时具备两种事务，因此带
  `TransactionalPublisher` 或 `OwnedTransactions` 约束的槽位或接线都绑定到 `MemoryPublish`。
  作用域内的发布会进入缓冲，并在提交时按发布顺序一起扇出；中止则把它们丢弃；每个拥有式事务各自缓冲。
  在原始句柄上的误用按 Broker 契约以 `MemoryError` 报错：已有事务打开时再次 begin 返回
  `TransactionBusy`（已打开的事务不受影响），没有事务时的 commit 或 abort 返回 `NoTransaction`。
  发布者句柄的克隆之间不共享事务。
- **分区键。** `MemoryMessage` 实现了 `Partitioned`，从约定的 `partition-key` 消息头
  （`memory::PARTITION_KEY_HEADER`）读取键。
- **定位。** `MemorySubscriber` 基于该 Broker 按名字维护的发布日志实现了 `Seekable`：在打开流之前先取得
  一个 `MemorySeeker`，然后 `seek` 到某个 `MemoryPosition`，该位置可以取自已投递的消息
  （`Positioned::position`，它会把那一条消息原样重新投递），也可以直接构造
  （`MemoryPosition::start()` / `sequence(n)`）。向前定位会跳过目标之前排队的投递；定位到日志末尾或
  更靠后，则会跳过此前发布的全部消息。作用范围是单个订阅者实例；如果句柄别名指向的总线已经关闭，
  通过它定位会以 `MemoryError::ShutDown` 报错。在应用内部，投递上下文（`MemoryContext`）携带位置和
  seeker，处理器通过 `Position` / `SeekHandle` 键读取它们（参见
  [定位](../guides/subscribers.md#seeking)）。批量函数体写的是 `MemoryBatchContext`：它在同一个
  `SeekHandle` 键下携带订阅的 seeker，但不携带位置，因为一批横跨多次投递。
- **关闭。** 这条阶梯是完全带类型的：`MemoryBroker::connect(self)` 产出 `ConnectedMemoryBroker`，而它
  消费自身的 `shutdown` 又产出 `ClosedMemoryBroker`，一个见证值，报告本次拆除丢弃了多少订阅者注册。
  关闭之后再使用别名句柄，无论是发布、提交事务还是发起请求，都会以 `MemoryError::ShutDown` /
  `RequestError::ShutDown` 报错，而不是悄悄成功。

`DescribeServer` 没有实现：内存 Broker 没有网络坐标可供上报。

## 订阅来源

`ConnectedMemoryBroker` 实现了 `Subscribe`，因此 `#[subscriber("orders")]` 可以直接使用。描述符类型是
`MemorySource`，它不带任何额外选项（内存 Broker 本来就没有），只是让描述符的形态在各个 Broker 之间保持
一致。下面取自
[`routed_service`](https://github.com/powersemmi/ruststream/tree/main/examples/routed_service) 示例：

=== "宏"

    ```rust
    use ruststream::memory::MemorySource;

    --8<-- "examples/routed_service/orders.rs:descriptor"
    ```

=== "手写"

    ```rust
    use ruststream::memory::{MemoryPublish, MemorySource};

    --8<-- "examples/manual/routed_service_orders.rs:descriptor"
    ```

## 用于测试

`ConnectedMemoryBroker` 实现了 `TestableBroker` 并用 `register_testable_broker!` 完成注册（校验套件会
先连接每个 Broker，再取回它的进程内传输），因此 [`TestApp`](../guides/testing.md) 套件可以直接驱动它：
在 `MemoryBroker` 上构建一个应用，交给 `TestApp::start`，发布消息，然后对处理器收到了什么、发布了什么
做断言。完整用法参见
[测试](../guides/testing.md#unit-testing-a-service-with-testapp)。
