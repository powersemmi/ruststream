# HTTP 框架

RustStream 不是 HTTP 框架。服务既提供 HTTP API 又消费消息时，两侧运行在同一个进程、同一个
tokio 运行时里。HTTP 框架（axum、actix-web，或者任何其他基于 tokio 的技术栈）与 RustStream 应用
并行运行。下面的接线用 axum 演示。让两侧保持一致的是事务性 outbox。

完整可编译的示例在
[`examples/http_outbox.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/http_outbox.rs)：

```text
cargo run --example http_outbox --features macros,memory,json
```

## 与 HTTP 服务器并行运行

两侧都在 `main` 里启动。`start()` 在后台启动消息这一侧，并返回 `RunningApp` 句柄，由它协调这
两条生命周期：

=== "宏"

    ```rust
    --8<-- "examples/http_outbox.rs:wiring"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/http_outbox.rs:wiring"
    ```

`start()` 会连上 Broker 并打开订阅。它在服务已经运行起来之后才完成，因此启动的错误会在 HTTP
一侧开始接收流量之前返回。

`stopping()` 返回一个持有所有权的 future。消息这一侧在 fail-fast 模式下因故障自行停止时，
该 future 就完成。可以把它接进 axum 的 `with_graceful_shutdown`，让 HTTP 服务器随之停下。

显式的优雅关闭由 `shutdown()` 发起。在 HTTP 服务器停下之后调用它。关闭的顺序和
[关闭超时](lifespan.md#shutdown-timeout)见生命周期指南。

HTTP 一侧通过绑定令牌拿到发布者。`.bindable()` 包住 Broker，`bind(..)` 在应用消费掉 Broker 之前
生成令牌，`running.publisher(token)` 把令牌与已经连上的 Broker 绑定。绑好的发布者是一个普通的值，
可以克隆进 HTTP 框架的任何状态里。

## healthz 端点 { #a-healthz-endpoint }

`start()` 只负责启动时的就绪。之后的服务状态由健康探针给出。`RunningApp::health()` 交出一个可克隆
的 `HealthProbe`：

```rust
--8<-- "examples/http_outbox.rs:healthz"
```

`state()` 返回一份无锁的快照：`Running`、`ShuttingDown`、`Stopped`，或者带 fail-fast 诊断信息的
`Failed { reason }`。`shutdown()` 之后探针继续工作，返回最终状态。消息这一侧在 fail-fast 模式下
发生故障时，`/healthz` 就返回 503，哪怕同级任务仍让进程保持运行。

这条路由带着自己的状态（`get(healthz).with_state(running.health())`），因此路由器其余部分的状态
可以是任意类型。上面的接线把 `/healthz` 注册在 `/orders` 旁边。

订阅者这一侧是一个普通的处理器。同一个服务消费自己的 HTTP 端点产生的事件，订阅了该 Broker 的其他
服务也能看到这些事件：

=== "宏"

    ```rust
    --8<-- "examples/http_outbox.rs:handler"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/http_outbox.rs:handler"
    ```

## 直接在请求里发布

最简单的集成方式，是把发布者放进 HTTP 框架的状态里，直接在请求路径上发布：
`publisher.message(&event).publish().await`，与[在处理器内部发布](publishing.md)完全一样。
[指标指南](metrics.md)里的完整服务器就是这样驱动它的计数器的。

代价是耦合。Broker 不可用时，HTTP 请求会返回错误或者一直等待。如果端点还要写数据库，写入与发布
之间就有一道缝隙。进程在两者之间崩溃，事件就会丢失。顺序反过来，就会为一次已回滚的写入发布事件。
这是一致性错误，下一次部署就会让它暴露。这道缝隙由事务性 outbox 补上。

## 事务性 outbox

端点把事件与业务写入记录在一起，之后由中继把它送到 Broker。写入和事件因此只会一同出现。该模式
并不专属于 HTTP，它有[单独的一页](transactional-outbox.md)。那一页讲的就是本页运行的示例
`examples/http_outbox.rs`。

## 试一试

```text
curl -X POST http://127.0.0.1:8080/orders \
  -H 'content-type: application/json' -d '{"id":1,"item":"book"}'
```

存储一提交，响应就返回。稍后中继发布该事件时，`fulfil` 处理器才把这笔订单记进日志。
