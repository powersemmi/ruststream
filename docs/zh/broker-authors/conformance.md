# Conformance

conformance 校验会证明 Broker 遵守核心契约。它有两个入口。两者都在第一次违反契约时 panic，
并说清楚问题出在哪里：

- `harness::run_suite` 检查 Broker [进程内模式](index.md#test-support)的**路由接口**：它构造你的
  生产 Broker，并通过 `InProcess::connect_in_process` 连接它。
- `harness::lifecycle` 在真实的 Broker 上端到端地检查**生命周期阶梯**。

两个都要跑。`run_suite` 检查分发方面的保证。`lifecycle` 证明 `new` -> `connect(self)` -> 订阅 ->
发布 -> ack -> `shutdown(self)` 在真实传输上确实走得通。

```toml
[dev-dependencies]
ruststream = { version = "0.7", features = ["conformance"] }
```

`conformance` feature 会连带引入 `testing`。因此你的 crate 只提供一个进程内模式，它既用于这里的
`run_suite`，也用于用户编写的 [`TestApp`](https://docs.rs/ruststream/latest/ruststream/testing/index.html) 测试。

## 路由套件

`harness::run_suite` 接收一个同步工厂（`Fn() -> B`）。工厂为每个场景构造全新的生产 Broker，因此
场景之间看不到彼此的状态。每个场景通过 `connect_in_process` 连接它，再驱动已连接形态；这个形态
实现了 `TestableBroker` 和 `Subscribe`。下面是内存参考 Broker 自己的套件运行，一字未改。把工厂里的
构造函数换成你自己 Broker 的构造函数，配置与服务里的一致：

```rust
use ruststream::conformance::harness;

--8<-- "tests/conformance_self.rs:run_suite"
```

### 它检查什么

| 场景 | 断言内容 |
|---|---|
| 投递顺序 | 消息按发布顺序投递 |
| 订阅之前先发布 | 订阅打开之前发布的消息：Broker 声明 `Backlog::Delivered` 时它最先到达，声明 `Backlog::Missed`（默认）时它不会到达；订阅之后发布的消息两种情况下都会到达 |
| ack 消费掉投递 | 已 ack 的消息不会重新投递 |
| 带重新入队的 nack 会重新投递 | `nack(requeue = true)` 会再投递一次这条消息 |
| 不重新入队的 nack 丢弃消息 | `nack(requeue = false)` 之后没有重新投递 |
| 消息头会传递 | 消息头原样到达订阅者 |
| 发布日志记录发布 | `published(name)` 记录每一条已发布的消息 |
| 发布日志记录你自己的发布者 | 默认发布者发出的消息出现在 `published(name)` 里，和测试注入的消息按发布顺序排在一起，并带着消息头（适用于用 `register_testable_broker!` 注册过的 Broker） |
| 同名的两个订阅 | 每个名字收到消息的次数与 `TestableBroker::routes` 的答复一致：Broker 扇出时每个订阅都收到全部消息，订阅互相竞争时每条消息只到其中一个，并且消息在两者之间分摊，除非答复指明全部消息去往哪一个 |
| harness 的计数保持平衡 | 在暂停的时钟上，按 `TestApp` 驱动的方式：发布返回之前投递已计入在途，结算时恰好释放一次，重新入队再次计入，没有订阅收到的发布不计入，延迟的重新投递在定时器到期时计入 |

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

每一步都从服务实际调用 Broker 的地方发出：

- 构造函数在一个没有 Tokio 运行时的普通线程上调用，必须在一秒内返回。构造函数里 spawn 任务、
  调用 `Handle::current` 或等待网络，都通不过检查。
- 订阅的建立、第一条消息的发布以及每一次确认，都在另一个线程上的单线程运行时里执行，这个运行时
  在检查继续之前就停止。订阅必须继续收到消息。发布第一条消息的发布者再发布两次，一次在 Broker 的
  运行时里，一次在新的运行时里，两条消息都必须到达。返回 `Ok` 的 `nack(requeue = true)` 必须让
  消息回来，`nack(requeue = false)` 则不能。检查以此要求 Broker 的内部任务运行在它连接时所在的
  运行时上（见[编写一个 Broker](index.md)）。
- 投递支持 `nack_after` 时，检查以 1.5 秒的延迟确认。消息回来的时间不能早于这个延迟，也不能晚于
  延迟之后十秒。忽略延迟、把延迟向下取整，或者在确认线程的运行时里计时，都通不过检查。
- `shutdown` 必须在十秒内返回。

在走阶梯之前，检查会在一条单独的连接上核对你的发布者能把消息的哪些内容送到。一个接一个发布的
三十二条消息，必须按同样的顺序到达同一个订阅。另外四条消息带有头部：大量条目、空值、非 UTF-8
的值，以及框架自己的重试计数和追踪上下文。每个头部都必须逐字节原样回来，否则携带它的那次发布
必须失败。发布失败的消息绝不能到达；途中被丢掉或被改写的头部会让检查失败。

已连接形态的持有者在关闭之后再用它，代码在编译期就通不过。留在运行时的规则是**别名句柄契约**。
关闭之后，通过以下发布者发布都必须返回错误：关闭之前创建、从未用过的发布者，只在 Broker 运行时
里用过的发布者，以及在已停止的运行时里用过的发布者。关闭之前收到、关闭之后重新入队的投递，必须
返回错误，或者再次到达。

订阅描述符、订阅者、发布者和投递都会移到其他线程，所以它们都是 `'static`。

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

- **`make_broker`** 是**同步的**（`Fn() -> B`），并且是 `Sync`，因为检查会从另一个线程调用它。
  只能异步构造的 Broker 满足不了它。构造要廉价，连接放到 `Broker::connect` 里做。
- **`make_source`** 为某个 subject 构造订阅描述符（宏订阅者那条路径）。
- **`make_publisher`** 从已连接形态产出一个发布者。

没有 ack 语义的 Broker（Core NATS）从 `ack` 返回 `AckError::Unsupported` 就算通过：这项检查既
接受这个结果，也接受一次成功的 ack。`lifecycle` 会执行一次真实的 `connect`，所以要针对正在运行
的服务器跑它，并且只在设置了 `NATS_TEST_URL` 这类环境变量时才运行。

## 关闭与共享句柄

另外两项生命周期检查需要只有你的 Broker 才知道的答案，所以由你的 crate 自己调用，在进程内和针对
真实服务器各跑一遍：

| 检查 | 接收 | 断言 |
|---|---|---|
| `lifecycle::shutdown_flushes` | Broker 的 `Backlog` 答案 | `shutdown` 之前刚做的确认和发布由关闭过程完成：已确认的消息不会在新连接上回来，已发布的消息会到达另一个连接（`Backlog::Delivered` 时是新连接，`Backlog::Missed` 时是事先订阅好的连接）；关闭之后返回 `Ok` 的确认同样必须生效；`shutdown` 在十秒内返回 |
| `lifecycle::shared_handle_closes` | 实现了 `Clone` 的已连接形态 | 原件关闭之后，通过副本的每一次使用都返回错误：关闭前后从副本创建的发布者，以及副本建立的订阅（被拒绝，或立即结束） |

`shutdown_flushes` 会连接不止一次，所以 `make_broker` 每次都必须通向同一个 Broker：一台服务器，
或者在进程内由它构造的所有 Broker 共享的同一个世界。内存 Broker 自己的这次运行让各个副本共享一条
总线：

```rust
use ruststream::conformance::lifecycle;
use ruststream::testing::Backlog;

--8<-- "tests/conformance_self.rs:shutdown_flushes"
```

## 在进程内跑同样的套件

`lifecycle`、`redelivery_address` 和各个能力套件都用 `Broker::connect` 连接交给它们的 Broker。把
生产 Broker 包进 `harness::InProcessBroker`，它们就改用 `connect_in_process` 连接，于是每个套件
在没有服务器的情况下再跑一遍，用的仍是你自己的描述符和发布策略。两遍都要跑：进程内这一遍让进程内
模式保持诚实，针对服务器那一遍证明真实传输。下面这个 Broker 的 `connect` 会去连服务器，它在进程内
通过了路由套件和生命周期检查：

```rust
--8<-- "tests/in_process.rs:suites"
```

## 结算套件

`settlement::suite` 在 Broker 所连接的传输上检查每一种结算是否兑现它的含义，包括真实的服务器：

| 检查 | 断言内容 |
|---|---|
| ack 消费投递 | 已 ack 的消息不会回来，在重投超时之内不会，在同一订阅的新连接上也不会 |
| 不重新入队的 nack 丢弃投递 | `nack(requeue = false)` 之后同样如此 |
| 重新入队的 nack 退回投递 | `nack(requeue = true)` 返回 `Ok(())`，消息就会回来 |
| 乱序结算 | 三条消息同时在处理中，ack 第三条、前两条不结算就放掉，前两条会回来：回到原订阅、重新打开的同一订阅，或者同一订阅的新连接上 |
| 未结算就放掉 | 在一个随即停止的运行时里不结算就放掉的消息会回来 |

回答 `AckError::Unsupported` 的结算只在它自己的订阅上检查：传输并不知道这次结算，新连接读到什么取决于它从哪里开始读。最后两项检查在 `nack(requeue = true)` 返回 `AckError::Unsupported` 时结束：什么都不收回的传输没有
可供观察的重投。只提交连续前缀的日志可能把已 ack 的第三条和前两条一起再投递一次；重复可以通过，
丢失不行。

```rust
--8<-- "src/conformance/settlement/tests.rs:matches_in_process"
```

- **`make_broker`** 每建一个连接就调用一次，而检查会连接两次，所以它返回的每个 Broker 都要到达
  同一个服务器：真实运行时是同一个地址，进程内是同一个 Broker 的克隆。
- **`make_source`** 在两个连接上打开同一个订阅：Broker 有持久消费者、队列或消费者组时，就用它们。
  它必须允许三条投递同时在处理中。
- **重投超时**是 Broker 退回无人结算的投递所需的时间，即描述符配置的 ack wait、visibility
  timeout 或 ack deadline。只在连接关闭时才退回这种投递的 Broker 传 `Duration::ZERO`。

`settlement::suite` 返回每一种结算的回答。`settlement::matches_in_process` 分别针对服务器和通过
`harness::InProcessBroker` 运行它，两边回答不同就失败：声称做到了服务器拒绝的结算的进程内传输，
会让处理器的重试在测试里通过，却在生产环境丢掉消息。

## 重试检查 { #retry-checks }

描述符的 `type Copies` 说明一条注册怎样重试，`conformance::retry` 逐一检查那些对服务有所承诺的
答案。

`harness::redelivery_address` 接收声明 `AddressedCopies` 的描述符。它用这个描述符打开两个订阅，就
像服务的两个副本读同一个订阅，然后向描述符报出的地址发布一份副本。副本从一个单线程运行时发出，该
运行时随即停止：专用线程上的处理器正是从这里发布 `retry_after` 的副本。副本必须恰好到达组内的一个
订阅（如果你的传输把一切交给每个订阅，就是每一个），并且消息头和 `RETRY_COUNT_HEADER` 原样到达。
随后检查把它放回队列两次，报出 `redelivery_count` 的投递必须依次数到 1、2、3。如果你的已连接形态
答的是 `Subscribe::Copies = AddressedCopies`，再用光名字的 `Name` 源跑一遍：这个答案就是每条
`#[subscriber("orders")]` 注册的地址。

```rust
--8<-- "src/conformance/retry/tests.rs:redelivery_address"
```

`retry::broker_moves` 接收声明 `BrokerMoves` 的描述符。它声明 `max_attempts(n)` 和一个死信地址，把
消息一次次放回队列直到上限用完，然后期望它恰好在 `n` 次投递之后、只一次出现在死信地址。只声明其中
一半时，Broker 必须在启动时拒绝。传入你的 Broker 接受的 `n`。`make_source(name)` 也用来打开死信地
址，所以它要创建发布到 `name` 所需的一切。

<!-- inline-rust: worked dead-letter check against the external ruststream-lapin crate; its real suite lives in that repo, so it has no compiled home here -->
```rust
use ruststream::conformance::harness::InProcessBroker;
use ruststream::conformance::retry;
use ruststream::nonzero;
use ruststream_lapin::{LapinBroker, LapinPublish, RabbitQuorumQueue};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_quorum_queue_dead_letters_at_the_cap() {
    retry::broker_moves(
        || InProcessBroker::new(LapinBroker::new("amqp://localhost:5672").declare_topology(true)),
        |name| RabbitQuorumQueue::new(name),
        |connected| connected.publisher(LapinPublish::default()),
        nonzero!(2u32),

## 进程内传输对照服务器

`conformance::in_process` 把进程内传输声明的行为和服务器的实际行为对照起来。两个套件每个探测都
连接 Broker 两次，一次用 `Broker::connect`，一次用 `connect_in_process`，所以要在跑针对服务器的
套件的地方运行它们：

<!-- inline-rust: worked check against the external ruststream-nats crate; its real gated suite lives in that repo, so it has no compiled home here -->
```rust
use ruststream::conformance::in_process::{self, Refusal};
use ruststream_nats::{CoreSubject, NatsBroker, NatsPublish};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs a running nats-server; set NATS_TEST_URL"]
async fn in_process_matches_the_server() {
    let url = std::env::var("NATS_TEST_URL").unwrap();
    in_process::backlog_matches_server(
        || NatsBroker::new(url.clone()),
        |connected| connected.publisher(NatsPublish),
    )
    .await;
    in_process::refuses_like_the_server(
        || NatsBroker::new(url.clone()),
        |connected| connected.publisher(NatsPublish),
        [
            Refusal::PayloadOver { name: "conformance.payload".to_owned(), limit: 1024 * 1024 },
            Refusal::Publish { name: "conformance.bad subject".to_owned() },
            Refusal::Subscription { source: CoreSubject::new("conformance..empty") },
        ],
    )
    .await;
}
```

- **`backlog_matches_server`** 在两种传输上各做一遍：向一个新名字发布一条消息，按名字打开订阅，
  再发布一条。`TestableBroker::backlog` 声明 `Backlog::Delivered` 时第一条最先到达，声明
  `Backlog::Missed` 时它不会到达。服务器拒绝第一次发布时（没有人声明过的队列或主题），进程内传输
  也必须拒绝。
- **`refuses_like_the_server`** 接收你知道的拒绝情形，每个都是一个 `Refusal`：超出上限一个字节的
  负载、被拒绝的目的地、被拒绝的订阅、在另一个订阅旁边被拒绝的订阅。两种传输都必须拒绝每一个。
  服务器接受的探测会以失败告终：它什么也证明不了。


## 消息携带的内容

`conformance::message_shape` 中的四项检查需要只有你的 Broker 才能给出的输入，所以由你的 crate
自己调用，和其他套件一样针对服务器跑一遍、在进程内再跑一遍：

| 检查 | 你提供的输入 | 断言 |
|---|---|---|
| `keyed_order` | 一个主题，以及键放在哪里：头部或发布参数的字段 | 每次投递都从 `partition_key` 报告它发布时的键，同一个键的消息按发布顺序到达 |
| `publish_options` | 发布策略、用例，以及从投递上读出设置的方式 | 不带参数的发布呈现策略的设置；调用的参数只对这一次调用覆盖策略；传输无法兑现的值让发布失败，消息也不会到达 |
| `publishes_without_credentials` | 一个配置了密码的发布策略 | 策略加入文档的任何绑定都不含该密码 |
| `describes_addresses_without_credentials` | 由多个带用户名和密码的地址构建的 Broker | 服务器描述中不含这些凭据 |

没有键的传输不调用 `keyed_order`：`None` 就是它诚实的回答。传给 `publish_options` 的策略要配置成
不同于传输默认值的设置，这样忘了策略的发布者不会因为恰好落在默认值上而通过检查。

```rust
--8<-- "tests/conformance_message_shape.rs:keyed_order"
```

## 能力套件 { #capability-suites }

如果你的 Broker 实现了某个能力 trait，就从 `conformance::capabilities` 跑对应的套件，它证明这份
实现遵守该 trait 的契约。不具备该能力的 Broker 不调用它。每个套件的工厂形状与 `lifecycle` 相同，
也会执行一次真实的 `connect`，所以用同样的环境变量控制它是否运行：

| 套件 | 要求 | 断言内容 |
|---|---|---|
| `capabilities::request_reply` | `RequestReply` | 请求带着一个可用的 `reply-to` 消息头到达响应方，相互关联的回复了结这次请求，无人应答的请求在超时之后返回错误；在前一个请求来自一个已经停止的运行时之后，新的请求仍然得到回复 |
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
[内存 Broker](../brokers/memory.md#capabilities)）。它就是可执行的参考，说明每个套件究竟期望什么。

## 作者检查清单

发布一个 Broker crate 之前：

- [ ] 已实现 `Broker`、`ConnectedBroker`、`Subscribe`（或一个 `SubscriptionSource`）、`Subscriber`、
      `IncomingMessage`、`Publisher`，以及构造它的 `PublishPolicy`。
- [ ] `shutdown` 完成所有可能返回错误的清理，并且绝不阻塞、绝不 panic。
- [ ] ack 消费 `self`；nack 遵守 `requeue` 标志。
- [ ] crate 自己拥有它的 `Config`；没有合理默认值的字段不实现 `Default`。
- [ ] 只有 Broker 确实支持的能力才实现对应的能力 trait，并且每一项已实现的能力都通过了
      `conformance::capabilities` 里对应的套件。
- [ ] 在 `testing` feature 之下提供进程内模式：Broker 上的 `InProcess`、已连接形态上的
      `TestableBroker`，以及 `register_testable_broker!(YourBroker)`。进程内传输的每项设置都取自
      生产 Broker，并且真实 Broker 会失败的地方，它绝不成功。
- [ ] `harness::run_suite` 通过（路由接口），其余套件既通过 `harness::InProcessBroker` 在进程内
      通过，也针对真实服务器通过。
- [ ] `harness::lifecycle` 针对真实服务器通过，并由一个环境变量控制是否运行（就是那条阶梯：
      同步的 `new`、消费 `self` 的 `connect`、订阅、ack、消费 `self` 的 `shutdown`，以及在此
      之后别名句柄返回的错误）。
- [ ] `lifecycle::shutdown_flushes` 以 Broker 的 `Backlog` 答案通过；已连接形态实现了 `Clone`
      时，`lifecycle::shared_handle_closes` 也通过。
- [ ] `settlement::matches_in_process` 针对真实服务器通过，同样由该环境变量控制（ack、nack、乱序
      结算和未结算就放掉，在服务器上和进程内含义相同）。
- [ ] `harness::redelivery_address` 对每个声明 `AddressedCopies` 的描述符通过；`Subscribe::Copies`
      为 `AddressedCopies` 时对光名字的 `Name` 也通过；`retry::broker_moves` 对每个声明
      `BrokerMoves` 的描述符通过。
- [ ] 传输有键或单条消息的设置时，`message_shape::keyed_order` 和 `message_shape::publish_options`
      在进程内和针对真实服务器都通过。
- [ ] 有一个端到端测试集覆盖 Broker 专有的语义，同样由该环境变量控制。
- [ ] `Cargo.toml` 元数据完整（`description`、`license`、`repository`、`keywords`、
      `categories`），并且 CI 检查 `--no-default-features` 和 `--all-features`。

trait 契约见[编写一个 Broker](index.md)。
