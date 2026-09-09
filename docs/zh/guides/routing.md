# 路由

随着服务变大，处理器从 `main.rs` 移入各自的模块。`Router` 把一个模块的处理器汇集成一个分组。
`include_router` 把这个分组挂载到 Broker 作用域上。

## 构建路由器

`Router` 的用法与 Broker 作用域相同。`include` 是唯一的入口，它挂载任何形式的定义：普通、原始、
批量、发布回复和带注入。形式由定义自身选定。`with_codec` 为它之后的注册切换解码用的编解码器，
已经挂载的注册则保留自己的（参见[编解码器](codecs.md#per-handler)）。

订阅来源由定义自身给出。`#[subscriber(..)]` 接收 Broker 自己的来源表达式，构建器链也包含在内。
因此在挂载点上不必再指定来源。每次调用都会消费掉路由器并返回一个新的，所以注册可以连成一条链：

=== "宏"

    ```rust title="routes.rs"
    use ruststream::runtime::Router;

    --8<-- "examples/routing.rs:builders"
    ```

=== "手写"

    ```rust title="routes.rs"
    use ruststream::runtime::Router;

    --8<-- "examples/manual/routing.rs:builders"
    ```

<!-- inline-rust: minimal mount fragment with placeholder routes module; the full compiled program is examples/routing.rs (merge form pulled in below) -->
```rust title="main.rs"
RustStream::new(info).with_broker(broker, |b| {
    b.include_router(routes::orders());
});
```

有些处理器需要一个回复发布者，或者一个
[`Out`](publishing.md#publishing-from-inside-a-handler) 槽位。这类处理器在路由器上的注册与在作用域上
一样，区别只有一处：注册由显式的 `.build()` 提交。`.out(marker, policy)` 为一个位置指定发布策略：
`Reply` 对应回复，槽位的标记对应 `Out` 槽位。没有写 `.out(Reply, ..)` 时，`.build()` 为回复采用
Broker 自带的默认发布策略。

缺少 `.build()` 的链不会成为路由器，因此无法通过编译。这些策略仍然是纯粹的声明，所以带策略的
路由器依旧不需要 Broker：

=== "宏"

    ```rust title="routes.rs"
    --8<-- "examples/tutorial/routes.rs:routes"
    ```

=== "手写"

    ```rust title="routes.rs"
    --8<-- "examples/manual/tutorial/routes.rs:routes"
    ```

## 路由器中间件 { #router-middleware }

路由器可以有自己的层栈：挂载时，`Router::layer` 会用这个栈包住路由器里的每一个处理器。
`include_router` 会在这个栈外面再包上应用的全局层栈（用 `RustStream::layer` 添加）。作用域层层
嵌套，最外层是应用：

=== "宏"

    ```rust title="main.rs"
    --8<-- "examples/logging_middleware.rs:layered_router"
    ```

=== "手写"

    ```rust title="main.rs"
    --8<-- "examples/manual/logging_middleware.rs:layered_router"
    ```

路由器隐藏了其中处理器的具体类型，因此包住它们的层必须是 `BlanketLayer`。这两种作用域、
`BlanketLayer` 这项要求和如何编写自己的层，都在[中间件](middleware.md#middleware-scopes)中说明。

## 组合与挂载

按模块构建路由器，再按服务的需要组合它们：

<!-- inline-rust: illustrative multi-router composition with placeholder route modules; the compiled merge form is examples/routing.rs:merge, pulled in below -->
```rust
// 把多个路由器挂到同一个 Broker 上，include_router 可以调用多次。
RustStream::new(info).with_broker(broker, |b| {
    b.include_router(routes::orders());
    b.include_router(routes::shipping());
});
```

也可以在挂载前把几个分组合并进一个路由器（完整程序见
[`examples/routing.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/routing.rs)）：

=== "宏"

    ```rust
    --8<-- "examples/routing.rs:merge"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/routing.rs:merge"
    ```

`merge` 按顺序把另一个路由器的注册追加进来。每个路由器保留自己的编解码器和层栈。挂载时，外层
路由器的层（以及应用的全局层栈）会包在合并进来的路由器的层外面。

## 下一步

- 处理器的契约与 `#[subscriber]` 宏：[订阅者](subscribers.md)。
- `include` 如何确定解码用的编解码器：[编解码器](codecs.md)。
