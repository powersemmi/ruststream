# HTTP 框架

RustStream 不是 HTTP 框架。当一个服务既要对外暴露同步的 HTTP API，又要消费消息时，HTTP 框架
（axum、actix-web，或者任何其他基于 tokio 的技术栈）就与 RustStream 应用并行跑在同一个进程、同一个
运行时里。本页展示在 axum 上的接线方式，以及让这种组合变得可靠的模式：事务性 outbox。

完整的、可编译的示例位于
[`examples/http_outbox.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/http_outbox.rs)：

```text
cargo run --example http_outbox --features macros,memory,json
```

## 与 HTTP 服务器并行运行

两边都在 `main` 里启动。`start()` 在后台把消息这一侧拉起来（状态生产者、Broker 连接、订阅建立），
并在服务真正跑起来之后才 resolve，因此启动失败会在 HTTP 一侧开始接收流量之前就暴露出来。返回的
`RunningApp` 句柄负责协调这两条生命周期：`stopping()` 是一个持有所有权的 future，一旦消息侧自行
拆除（即 fail-fast 失败）它就会 resolve，可以直接接进 axum 的 `with_graceful_shutdown`，让进程不会
在消费方已经死掉的情况下继续提供 HTTP 服务；而 `shutdown()` 则是显式的优雅拆除，在 HTTP 服务器停下
之后执行 `on_shutdown` 钩子、把仍在处理中的处理器排空（时长受[关闭超时](lifespan.md#shutdown-timeout)
约束）、并关闭 Broker。发布者通过一个绑定 token 拿到：`.bindable()` 把 Broker 包起来，`bind(..)`
在应用消费掉它之前铸出该 token。随后 `running.publisher(token)` 会在 `start()` 连上 Broker 之后把
该 token 配对好。配对出来的发布者是一个普通的值，可以放心地克隆进 HTTP 框架所携带的任何状态里：

```rust
--8<-- "examples/http_outbox.rs:wiring"
```

## healthz 端点 { #a-healthz-endpoint }

`start()` 是就绪的关卡，它之后的一切由健康探针负责。`RunningApp::health()` 交出一个廉价、可克隆的
`HealthProbe`，其背后是一个 watch channel：`state()` 是一份无锁的快照（`Running`、`ShuttingDown`、
`Stopped`，或者携带 fail-fast 诊断信息的 `Failed { reason }`），而且该探针比 `shutdown()` 活得
更久，因此这条路由会一直用终态作答。这补上了单靠 `stopping()` 会留下的缺口：当消息侧 fail-fast、而
某个兄弟任务仍让进程活着时，`/healthz` 会翻成 503，而不是为一个已死的消费方永远返回 200：

```rust
--8<-- "examples/http_outbox.rs:healthz"
```

这条路由携带它自己的状态（`get(healthz).with_state(running.health())`），因此它可以与路由器其余
部分持有的任何状态组合；上面那份完整的接线就把它注册在 `/orders` 旁边。

订阅者这一侧就是一个普通的处理器；同一个服务会消费自己的 HTTP 端点所产生的东西，而任何其他订阅了
该 Broker 的服务也同样看得到这些事件：

```rust
--8<-- "examples/http_outbox.rs:handler"
```

## 直接在请求里发布

最简单的集成方式，是把发布者放进 HTTP 框架的状态里，直接在请求路径上发布，做法与
[在处理器内部发布](publishing.md)完全一样：`publisher.message(&event).publish().await`。
[指标指南里的完整服务器](metrics.md)就是这么驱动它的计数器的。

代价是耦合：Broker 一旦故障，HTTP 请求就会失败或卡住；而在写完数据库、还没发布之前崩溃，就会丢掉
该事件（若顺序反过来，则会为一次已回滚的写入发布出事件）。如果该端点还要写数据库，那这道缝隙就是
一个只等某个部署窗口发作的一致性 bug。解决办法就是事务性 outbox。

## 事务性 outbox

端点把事件与业务写入记录在一起，之后由中继搬到 Broker 上，因此两边不会只发生一半。这个模式并不专属
于 HTTP，它有自己的页面：[事务性 outbox](transactional-outbox.md)。本页运行的示例
`examples/http_outbox.rs` 就是同一个。

## 试一试

```text
curl -X POST http://127.0.0.1:8080/orders \
  -H 'content-type: application/json' -d '{"id":1,"item":"book"}'
```

存储一提交，响应就返回；稍后，当中继把事件发布出去时，`fulfil` 处理器才会把这笔订单记进日志。
