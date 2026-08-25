# Conformance

conformance 校验套件用来证明一个 Broker 遵守了核心契约。它有两个入口，两者都会在第一次失败时带着
一条描述清楚的消息 panic：

- `harness::run_suite` 针对你随 crate 一起提供的进程内传输层（也就是
  [`TestableBroker`](index.md#test-support)）检查**路由接口**。
- `harness::lifecycle` 针对真实的 Broker 端到端地检查**生命周期阶梯**。

两个都要跑：`run_suite` 检查分发方面的保证，`lifecycle` 则证明 `new` -> `connect(self)` -> 订阅 ->
发布 -> ack -> `shutdown(self)` 这条链路在真实传输层上确实走得通。

```toml
[dev-dependencies]
ruststream = { version = "0.7", features = ["conformance"] }
```

`conformance` feature 会连带引入 `testing`，因此你的 crate 提供的那唯一一个 `TestableBroker` 既能用于
这里的 `run_suite`，也能用于用户自己写的 [`TestApp`](../guides/testing.md) 测试套件。

## 路由套件

`harness::run_suite` 接收一个同步工厂（`Fn() -> B`），为每个场景构造一份全新的进程内传输层，这样
场景之间就不会互相泄漏状态。每个场景都会连接 Broker 并驱动它的已连接形态，也就是你的 `TestableBroker`
（它同时实现了 `Subscribe`）。下面就是内存参考 Broker 自己那一次套件运行，一字未改；把工厂里的构造
函数换成你自己传输层的即可：

```rust
use ruststream::conformance::harness;

--8<-- "tests/conformance_self.rs:run_suite"
```

### 它检查什么

| 场景 | 断言内容 |
|---|---|
| 顺序 | 消息按发布顺序投递 |
| 订阅之后再发布 | 订阅者只会收到它挂上之后发布的消息；更早的发布不会进入缓冲 |
| ack 消费掉投递 | 已经 ack 的消息不会重新投递 |
| 带重新入队的 nack 会重新投递 | `nack(requeue = true)` 会再次投递这条消息 |
| 不重新入队的 nack 丢弃消息 | `nack(requeue = false)` 不会重新投递 |
| 消息头会传递 | 消息头在一次往返之后仍然完好 |
| 发布日志能观察到发布 | `published(name)` 记录下每一条已发布的消息 |

这些是核心路由方面的保证，是每个 Broker 都必须满足的契约。该校验套件**不会**测试 Broker 专有的语义
（持久化续传、超时重新投递、分区分配）；那些不属于该契约，要由你自己针对真实服务器的端到端测试集来验证。

## 生命周期检查

`run_suite` 通过进程内传输层演练路由；`harness::lifecycle` 则通过真实的 `Broker` 演练**生命周期
阶梯**：先是不做任何 I/O 的同步构造，然后是消费 `self` 的 `connect` 产出类型化的已连接形态，接着通过
Broker 自己的 `SubscriptionSource` 打开一个订阅，发布一条消息让该订阅收到并 ack，最后由消费 `self` 的
`shutdown` 产出终态见证值。在这条阶梯之下，持有者一侧在关闭之后的误用根本无法通过编译，因此这项检查
真正要持续验证的运行时规则是**别名句柄契约**：在关闭之前创建的发布者，在关闭之后必须报错，不得对着
一条已死的连接悄无声息地成功。它接收三个工厂，从而与具体 Broker 无关：

<!-- inline-rust: worked lifecycle check against the external ruststream-nats crate; its real gated suite lives in that repo, so it has no compiled home here -->
```rust
use ruststream::conformance::harness;
use ruststream_nats::{NatsBroker, SubscribeOptions};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs a running nats-server; set NATS_TEST_URL"]
async fn passes_lifecycle() {
    let url = std::env::var("NATS_TEST_URL").unwrap();
    harness::lifecycle(
        || NatsBroker::new(url.clone()), // sync construction (no I/O)
        |subject| SubscribeOptions::new(subject), // the broker's SubscriptionSource
        |connected| connected.publisher(), // a publisher from the connected form
    )
    .await;
}
```

- **`make_broker`** 是**同步的**（`Fn() -> B`）。只能异步构造的 Broker 无法满足它：构造要廉价，连接
  放到 `Broker::connect` 里做。
- **`make_source`** 为某个 subject 构造订阅描述符（宏订阅者那条路径）。
- **`make_publisher`** 从已连接形态产出一个发布者。

没有 ack 语义的 Broker（Core NATS）只要从 `ack` 返回 `AckError::Unsupported` 就算通过；这项检查既接受
这种结果，也接受一次成功的 ack。由于 `lifecycle` 会执行一次真实的 `connect`，要针对一台真实运行的
服务器来跑它（用类似 `NATS_TEST_URL` 的环境变量把它挡住）；内存 Broker 则可以在进程内跑。

## 能力套件 { #capability-suites }

如果你的 Broker 实现了某个能力 trait，就从 `conformance::capabilities` 里跑对应的套件，以证明这份实现
遵守了该 trait 的契约；不具备该能力的 Broker 不必调用它。每个套件的工厂形状与 `lifecycle` 相同，并且
都会执行一次真实的 `connect`，所以要用同样的方式把它们挡在开关之后：

| 套件 | 要求 | 断言内容 |
|---|---|---|
| `capabilities::request_reply` | `RequestReply` | 请求带着一个可用的 `reply-to` 消息头到达响应方，相互关联的回复能了结这次请求，无人应答的请求在超时之后失败 |
| `capabilities::batches` | `BatchSubscriber` | 每一条已发布的消息都按发布顺序到达，并分布在若干非空的批次中 |
| `capabilities::transactions` | `TransactionalPublisher` | 事务内的任何内容在 `commit` 之前都不可见，提交会按顺序发布整个缓冲区，中止则将其丢弃；误用会报错：没有打开事务却 `commit` / `abort`，或者已有事务打开时再次 `begin_transaction`（这必须让原事务保持不变） |
| `capabilities::owned_transactions` | `OwnedTransactions` 及其 `Transaction` | 发布进一个打开着的事务里的内容在 `commit` 之前都不可见，提交会按发布顺序投递整个缓冲区，中止则将其丢弃，同一个句柄上同时打开的两个事务各自独立结算，并且其中一个打开着时该句柄仍能继续直接发布 |
| `capabilities::seeking` | `Seekable`，且消息实现 `Positioned` | 回退到从某条已投递消息上取得的位置之后，恰好会重新投递那一条消息以及它之后按顺序排列的后缀，向前跳转则会略过目标之前已经排队的投递，而且重新定位之后订阅仍会继续投递新发布的消息 |

<!-- inline-rust: worked request-reply capability check against the external ruststream-nats crate; its real gated suite lives in that repo, so it has no compiled home here -->
```rust
use ruststream::conformance::capabilities;
use ruststream_nats::{NatsBroker, SubscribeOptions};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs a running nats-server; set NATS_TEST_URL"]
async fn passes_request_reply() {
    let url = std::env::var("NATS_TEST_URL").unwrap();
    capabilities::request_reply(
        || NatsBroker::new(url.clone()),
        |subject| SubscribeOptions::new(subject),
        |connected| connected.publisher(), // the RequestReply publisher under test
        |connected| connected.publisher(), // the plain publisher the responder replies through
    )
    .await;
}
```

内存 Broker 原生实现了每一项能力，并在进程内通过了全部五个套件（见
[Memory](../brokers/memory.md#capabilities)）；它就是每个套件究竟期望什么的可执行参考。

## 作者检查清单

发布一个 Broker crate 之前：

- [ ] 已实现 `Broker`、`ConnectedBroker`、`Subscribe`（或一个 `SubscriptionSource`）、`Subscriber`、
      `IncomingMessage`、`Publisher`，以及一个能与之配对的 `PublishPolicy`。
- [ ] `shutdown` 完成了所有可失败的清理工作，并且绝不阻塞、绝不 panic。
- [ ] ack 消费 `self`；nack 遵守 `requeue` 标志。
- [ ] crate 自己拥有它的 `Config`；没有合理默认值的字段不提供 `Default`。
- [ ] 只有 Broker 确实支持的能力才实现对应的能力 trait，并且每一项已实现的能力都通过了它在
      `conformance::capabilities` 中的套件。
- [ ] 在 `testing` feature 之下提供了一个在已连接形态上实现 `TestableBroker` 的进程内传输层（只覆盖
      核心路由），并用 `register_testable_broker!` 完成注册。
- [ ] `harness::run_suite` 通过（路由接口）。
- [ ] `harness::lifecycle` 针对真实服务器通过，并由一个环境变量挡住（这条阶梯是：同步的 `new`、消费
      `self` 的 `connect`、订阅、ack、消费 `self` 的 `shutdown`，以及在此之后别名句柄的报错）。
- [ ] 有一个端到端测试集覆盖 Broker 专有的语义，同样由该环境变量挡住。
- [ ] `Cargo.toml` 的元数据完整（`description`、`license`、`repository`、`keywords`、`categories`），
      并且 CI 会检查 `--no-default-features` 和 `--all-features`。

trait 契约见[编写一个 Broker](index.md)。
