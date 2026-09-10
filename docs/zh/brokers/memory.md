# 内存 Broker

`memory` feature 下的 `MemoryBroker` 是一个完整的进程内 Broker。当队列只属于单个应用而不属于网络
时，选它即可。默认的 `cargo generate` 模板（`templates/memory`）就建立在它之上，因此新建的项目没有
外部依赖就能运行。

```toml
ruststream = { version = "0.7", features = ["macros", "memory", "json"] }
```

<!-- inline-rust: two-line constructor sketch; the broker in context is exercised by every memory-feature example (e.g. quickstart.rs:app) -->
```rust
use ruststream::memory::MemoryBroker;

let broker = MemoryBroker::new();
```

## 它保留多少 { #retention }

`MemoryBroker::new()` 建出的 Broker 什么都不保留。一条消息从发布活到最后一个订阅者读完为止，因此
长期运行的服务占用的内存只是处理器还没做完的那部分工作，与消息总数无关。

需要回放的服务要保留历史，并说明保留多少：

```rust
--8<-- "examples/seek.rs:retaining"
```

`Retention` 约束的是单个主题：`Messages(n)` 为每个主题保留最新的 `n` 条消息，`Bytes(n)` 保留能装进
`n` 字节的最新负载，`MessagesAndBytes { .. }` 同时应用两个上限。因此，向一千个主题发布的 Broker
会为每个主题各保留这么多。最新的一条消息始终留下，所以比字节上限还宽的负载会被单独保留，而不是在
发布时就丢掉。

两种形态是两个类型：`MemoryBroker::retaining(..)` 给出订阅可 `Seekable` 的那一个，从某个位置开始
读取、或者读取定位句柄的挂载只在它之上才能编译。在不保留日志的 Broker 上回放是编译错误，而不是一次
悄悄什么也找不到的回放。

## 挂载点导入的 prelude { #prelude }

`ruststream::memory::prelude` 是该 Broker 的 glob，形状和每个 Broker crate 的 prelude 一样。它先
重导出核心 prelude，然后是该 Broker 自己的表面（`MemoryBroker`、`MemorySource`、`MemoryError`、
`MemoryPosition`、`Retention` 与日志模式 `Discarding` / `Retaining`，以及上下文键 `MemoryContext` /
`MemoryBatchContext` / `Position` /
`SeekHandle`），最后是统一名字下的发布策略：`Publish`、`TransactionalPublish` 和 `Request`。
这三个名字是 `MemoryPublish` 和 `MemoryRequest` 的别名。该 Broker 的发布者实现了两种事务，因此
`TransactionalPublish` 在这里就是 `Publish` 那一个策略；事务配置独立的 Broker 会把该名字指向
另一个策略。

<!-- inline-rust: the import shape; every memory-feature example under examples/ mounts through it -->
```rust
use ruststream::memory::prelude::*;
```

同一个 glob 还会把该 Broker 实现的能力 trait 带进作用域：`TransactionalPublisher`、
`OwnedTransactions`、`Transaction`、`RequestReply`、`Positioned` 和 `Seeker`。它们带来的操作，在
策略所在的地方同样可用。`Partitioned` 不在其中：它一旦进入作用域，`msg.partition_key()` 就会和
`IncomingMessage` 的同名方法产生歧义。读取分区键的服务自行导入 `Partitioned`。

处理器主体保留 `use ruststream::prelude::*;`：它写的是能力而不是策略，并不知道哪个 Broker 在运行
它。主体和挂载点写在同一个文件里时，一个 Broker glob 就够了。

## 语义

- **主题名精确匹配。** 对 `orders` 的订阅会收到发布到 `orders` 的消息。
- **投递给全部订阅者。** 某个主题的每个订阅者，都会收到订阅之后发布到该主题的每条消息。
- **ack 是空操作。** 带 `requeue: true` 的 nack 把同一份载荷重新投递给同一个订阅者。
- **共享所有权。** `MemoryBroker` 是引用计数的句柄，所有副本共用同一份状态。因此测试持有的副本，
  能看到应用发布的一切。

处理器、中间件和解码在这里的行为，与在网络 Broker 上一致：运行时用同一条路径分发消息。

## 能力 { #capabilities }

每个能力 trait 都实现在该 Broker 自己的进程内语义之上：

- **请求-响应。** `broker.requester()` 给出 `MemoryRequester`。它的 `request` 发布消息，并在
  `reply-to` 消息头里写上一个唯一的进程内响应主题。第一条消息投递到该主题时，`request` 完成。
  响应方从请求里读出 `reply-to`，把回复发布到该主题。没有人应答的请求返回
  `RequestError::Timeout` 错误。`MemoryRequest` 策略构造 `MemoryRequester`，因此带
  `Out<impl RequestReply, ..>` 约束的槽位，你绑定到 `MemoryRequest`。
- **批。** `MemorySubscriber` 实现了 `BatchSubscriber`：一个批是第一条到达的投递，加上此时已经
  缓冲的全部消息。上限是注册处理器时用 `batch(n)` 给出的大小。未满的批也会立即投递。
- **事务。** `MemoryPublish` 策略构造 `MemoryPublisher`，它实现了两种事务。因此带
  `TransactionalPublisher` 或 `OwnedTransactions` 约束的槽位或接线，你绑定到 `MemoryPublish`。
  事务作用域内的发布先进入缓冲：`commit` 按发布顺序把它们一起投递给全部订阅者，`abort` 把它们
  丢弃。每个拥有式事务各自缓冲，发布者句柄的副本之间不共享事务。在发布者上乱序调用会返回
  `MemoryError`：事务已经打开时再次 `begin_transaction` 返回 `TransactionBusy`，已打开的事务不受
  影响；没有事务时 `commit` 或 `abort` 返回 `NoTransaction`。
- **分区键。** `MemoryMessage` 实现了 `Partitioned`，从 `partition-key` 消息头
  （`memory::PARTITION_KEY_HEADER`）读取键。
- **日志定位。** 在[保留日志的 Broker](#retention) 上，`MemorySubscriber` 在每个主题的日志之上实现
  了 `Seekable`：开始读取之前先取得 `MemorySeeker`，再对某个 `MemoryPosition` 调用 `seek`。位置可以
  从已投递的消息上取得（`Positioned::position`，此时那一条消息会重新投递），也可以直接构造
  （`MemoryPosition::start()` / `sequence(n)` / `end()`）。序号是绝对的：保留上限淘汰更旧的消息时，
  同一个序号仍然指同一条消息。`start()` 是仍然保留着的最旧一条，`end()` 是日志末尾，在此前发布的全部
  消息之后。向前定位会跳过排在目标之前的投递。定位到已被上限淘汰的序号，返回
  `MemoryError::PositionEvicted` 错误，并报告仍然保留的最旧位置。定位作用在单个订阅者实例上。通过
  已停止总线的句柄定位，返回 `MemoryError::ShutDown`
  错误。在应用内部，`MemoryContext` 里有消息的位置和 `MemorySeeker`，处理器按 `Position` 和
  `SeekHandle` 两个键读取（参见[定位](../guides/subscribers.md#seeking)）。批量处理器读的是
  `MemoryBatchContext`：那里有 `SeekHandle`，但没有 `Position`，因为一个批横跨多次投递。
- **关闭。** `MemoryBroker::connect(self)` 给出 `ConnectedMemoryBroker`。它的 `shutdown` 消费自身
  并返回 `ClosedMemoryBroker`，报告这次关闭丢弃了多少个订阅者注册。关闭之后，通过先前发出的句柄
  发布消息、提交事务或发起请求，都返回 `MemoryError::ShutDown` 或 `RequestError::ShutDown` 错误。

## 订阅来源

`ConnectedMemoryBroker` 实现了 `Subscribe`，因此 `#[subscriber("orders")]` 可以直接使用。同一个
订阅也可以用 `MemorySource` 描述符写出，形式和任何 Broker 描述订阅时一样。下面取自
[`routed_service`](https://github.com/powersemmi/ruststream/tree/main/examples/routed_service) 示例：

=== "宏"

    ```rust
    use ruststream::memory::prelude::*;

    --8<-- "examples/routed_service/orders.rs:descriptor"
    ```

=== "手写"

    ```rust
    use ruststream::memory::prelude::*;

    --8<-- "examples/manual/routed_service_orders.rs:descriptor"
    ```

## 用于测试

`MemoryBroker` 上的应用，你用 [`TestApp`](../guides/testing.md) 套件来测试：构建应用，交给
`TestApp::start`，发布消息，然后断言处理器收到了什么、发布了什么。完整用法参见
[测试](../guides/testing.md#unit-testing-a-service-with-testapp)。

在一次测试运行期间，套件会记录服务发布的每一条消息，因此无论应用建立在哪一种形态的 Broker 之上，
`published::<T>(..)` 断言读到的都是同一份列表。在套件之外，通过 `TestableBroker::published` 读回
日志，看到的就是那个 Broker 保留的内容：保留日志的那个给出上限之内的全部消息，默认的那个什么都
没有。
