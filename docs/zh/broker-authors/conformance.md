# Conformance

conformance 校验会证明 Broker 遵守核心契约。它有两个入口。两者都在第一次违反契约时 panic，
并说清楚问题出在哪里：

- `harness::run_suite` 针对你随 crate 一起提供的进程内传输（也就是
  [`TestableBroker`](index.md#test-support)）检查**路由接口**。
- `harness::lifecycle` 在真实的 Broker 上端到端地检查**生命周期阶梯**。

两个都要跑。`run_suite` 检查分发方面的保证。`lifecycle` 证明 `new` -> `connect(self)` -> 订阅 ->
发布 -> ack -> `shutdown(self)` 在真实传输上确实走得通。

```toml
[dev-dependencies]
ruststream = { version = "0.7", features = ["conformance"] }
```

`conformance` feature 会连带引入 `testing`。因此你的 crate 只提供一个 `TestableBroker`，它既用于
这里的 `run_suite`，也用于用户编写的 [`TestApp`](../guides/testing.md) 测试。

## 路由套件

`harness::run_suite` 接收一个同步工厂（`Fn() -> B`）。工厂为每个场景构造全新的进程内传输，因此
场景之间看不到彼此的状态。每个场景连接 Broker，驱动它的已连接形态，也就是同时实现了 `Subscribe`
的那个 `TestableBroker`。下面是内存参考 Broker 自己的套件运行，一字未改。把工厂里的构造函数换成
你自己传输的构造函数即可：

```rust
use ruststream::conformance::harness;

--8<-- "tests/conformance_self.rs:run_suite"
```

### 它检查什么

| 场景 | 断言内容 |
|---|---|
| 投递顺序 | 消息按发布顺序投递 |
| 订阅之后再发布 | 订阅者只收到订阅之后发布的消息，更早的发布不进入缓冲 |
| ack 消费掉投递 | 已 ack 的消息不会重新投递 |
| 带重新入队的 nack 会重新投递 | `nack(requeue = true)` 会再投递一次这条消息 |
| 不重新入队的 nack 丢弃消息 | `nack(requeue = false)` 之后没有重新投递 |
| 消息头会传递 | 消息头原样到达订阅者 |
| 发布日志记录发布 | `published(name)` 记录每一条已发布的消息 |

无法确认的传输（ZeroMQ、MQTT `QoS 0`、Redis pub/sub 和 Core NATS）从 `ack` 和 `nack` 返回
`AckError::Unsupported`。套件在每一处结算投递的地方都接受这个答复。你的进程内传输务必答得和
真实传输一模一样，不要声称生产中根本不会发生的结算。只有重新投递那个场景例外：
`nack(requeue = true)` 答 `Unsupported`，场景就到此结束，因为一个什么都不收回的传输没有重新
投递可看。其余断言照旧，丢弃场景也在内：没人能结算的投递，同样不该再回来。

套件从投递读取这个答复，而不是从 Broker 读取。因此答复可以按订阅、按消息而不同，正如你的传输
本身那样：重新入队在一种提交模式下只是建议，在另一种下会回拨位点；确认在一个服务质量等级上
能给，在另一个上给不了。不变的是成功的含义：`nack(requeue = true)` 返回 `Ok(())`，就承诺这条
消息会回来，运行时的重试路径也照这个含义来读。

这些是核心路由方面的保证，是每个 Broker 都必须满足的契约。Broker 专有的语义（持久化续传、
超时重新投递和分区分配）不属于这份契约，由你自己针对真实服务器的端到端测试集来验证。

每项能力在下面都有自己的套件。你的 crate 实现了哪几项能力，就调用哪几个套件。其中
`capabilities::batches` 是唯一一处检查：收到的批不会大于开启时给定的尺寸。只跑了
`run_suite` 的 crate，还没有检查过自己的批。

## 生命周期检查

`harness::lifecycle` 在真实的 `Broker` 上走完**生命周期阶梯**。先是不做任何 I/O 的同步构造。
接着是消费 `self` 的 `connect`，它产出类型化的已连接形态。然后通过 Broker 自己的
`SubscriptionSource` 建立一个订阅，发布一条消息，由该订阅收到并 ack。最后是消费 `self` 的
`shutdown`，它产出终态见证值。

已连接形态的持有者在关闭之后再用它，代码在编译期就通不过。留在运行时的规则是**别名句柄契约**，
这项检查验证的正是它：关闭之前创建的发布者，在关闭之后必须返回错误，不得对着一条已经关闭的
连接悄悄成功。

这项检查接收三个工厂，因此与具体 Broker 无关：

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

- **`make_broker`** 是**同步的**（`Fn() -> B`）。只能异步构造的 Broker 满足不了它。构造要廉价，
  连接放到 `Broker::connect` 里做。
- **`make_source`** 为某个 subject 构造订阅描述符（宏订阅者那条路径）。
- **`make_publisher`** 从已连接形态产出一个发布者。

没有 ack 语义的 Broker（Core NATS）从 `ack` 返回 `AckError::Unsupported` 就算通过：这项检查既
接受这个结果，也接受一次成功的 ack。`lifecycle` 会执行一次真实的 `connect`，所以要针对正在运行
的服务器跑它，并且只在设置了 `NATS_TEST_URL` 这类环境变量时才运行。内存 Broker 在进程内就能通过。

## 能力套件 { #capability-suites }

如果你的 Broker 实现了某个能力 trait，就从 `conformance::capabilities` 跑对应的套件，它证明这份
实现遵守该 trait 的契约。不具备该能力的 Broker 不调用它。每个套件的工厂形状与 `lifecycle` 相同，
也会执行一次真实的 `connect`，所以用同样的环境变量控制它是否运行：

| 套件 | 要求 | 断言内容 |
|---|---|---|
| `capabilities::request_reply` | `RequestReply` | 请求带着一个可用的 `reply-to` 消息头到达响应方，相互关联的回复了结这次请求，无人应答的请求在超时之后返回错误 |
| `capabilities::batches` | `BatchSubscriber` | 每一条已发布的消息都按发布顺序到达，并分布在若干非空的批里 |
| `capabilities::transactions` | `TransactionalPublisher` | 事务里的任何内容在 `commit` 之前都不可见，`commit` 按顺序发布整个缓冲区，`abort` 把它丢弃；误用会返回错误：没有打开事务就 `commit` / `abort`，或者已有事务打开时再次 `begin_transaction`（这必须让原事务保持不变） |
| `capabilities::owned_transactions` | `OwnedTransactions` 及其 `Transaction` | 发布进一个打开着的事务里的内容在 `commit` 之前不可见，`commit` 按发布顺序投递整个缓冲区，`abort` 把它丢弃，同一个发布者上同时打开的两个事务各自独立结算，并且其中一个打开着时该发布者仍能直接发布 |
| `capabilities::seeking` | `Seekable`，且消息实现 `Positioned` | 回退到从某条已投递消息上取得的位置，会重新投递恰好那一条消息以及它之后按顺序排列的后缀；向前跳转会略过目标之前排队的投递；重新定位之后，订阅继续投递新的发布 |

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

每个套件在每次运行时重新取一个 subject，而不是固定一个，因此一次运行只读到自己发布的东西。
固定的 subject 在全新服务器上能过一次，第二次就会在任何保留前一次残留的 Broker 上通不过：留存的
日志会把两次运行一起重放进同一个订阅，持久队列里还压着早先的消息，键空间里还留着早先的类型。
套件为此调用 `conformance::helpers::unique_subject`。你自己的端到端测试集有同样的问题，也有
同样的解法。

内存 Broker 原生实现了每一项能力，五个套件都在进程内通过（见
[Memory](../brokers/memory.md#capabilities)）。它就是可执行的参考，说明每个套件究竟期望什么。

## 作者检查清单

发布一个 Broker crate 之前：

- [ ] 已实现 `Broker`、`ConnectedBroker`、`Subscribe`（或一个 `SubscriptionSource`）、`Subscriber`、
      `IncomingMessage`、`Publisher`，以及构造它的 `PublishPolicy`。
- [ ] `shutdown` 完成所有可能返回错误的清理，并且绝不阻塞、绝不 panic。
- [ ] ack 消费 `self`；nack 遵守 `requeue` 标志。
- [ ] crate 自己拥有它的 `Config`；没有合理默认值的字段不实现 `Default`。
- [ ] 只有 Broker 确实支持的能力才实现对应的能力 trait，并且每一项已实现的能力都通过了
      `conformance::capabilities` 里对应的套件。
- [ ] 在 `testing` feature 之下提供一个在已连接形态上实现 `TestableBroker` 的进程内传输（只做
      核心路由），并用 `register_testable_broker!` 注册。
- [ ] `harness::run_suite` 通过（路由接口）。
- [ ] `harness::lifecycle` 针对真实服务器通过，并由一个环境变量控制是否运行（就是那条阶梯：
      同步的 `new`、消费 `self` 的 `connect`、订阅、ack、消费 `self` 的 `shutdown`，以及在此
      之后别名句柄返回的错误）。
- [ ] 有一个端到端测试集覆盖 Broker 专有的语义，同样由该环境变量控制。
- [ ] `Cargo.toml` 元数据完整（`description`、`license`、`repository`、`keywords`、
      `categories`），并且 CI 检查 `--no-default-features` 和 `--all-features`。

trait 契约见[编写一个 Broker](index.md)。
