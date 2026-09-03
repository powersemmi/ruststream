# 路由

服务变大之后，处理器会从 `main.rs` 搬进各自的模块。`Router` 把一个模块里的处理器收拢成一个可挂载的
分组；`include_router` 则把整个分组挂到某个 Broker 作用域上。

## 构建路由器

`Router` 与 Broker 作用域是对应的：`include` 是唯一的入口，各种定义形式（普通、原始、批量、发布回复、
带注入）都由定义自身决定，全部经它挂载，旁边则是 `with_codec`（切换该链上的解码编解码器，参见
[编解码器](codecs.md#per-handler)）以及底层的 `handle` 注册。订阅来源始终来自定义本身，
`#[subscriber(..)]` 直接接收 Broker 自己的来源表达式，包括构建器链在内，因此挂载点没有任何东西需要
再命名。每次调用都会消费掉路由器并返回一个新的，所以注册可以链式书写：

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

需要附加物的处理器，比如一个回复发布者、一个
[`Out`](publishing.md#publishing-from-inside-a-handler) 槽位，在路由器上的注册方式与在作用域上一样，
区别只在于注册要通过一个显式的终结调用来提交：`.publisher(policy)` 指定接线方式，`.build()` 采用
Broker 自带的默认发布策略，`.out(marker, policy)` 则在 `.build()` 之前绑定一个具名槽位。忘记写终结
调用就永远得不到路由器，整条链也无法通过编译。这些策略仍然是纯粹的声明，因此路由器依旧不需要
Broker：

=== "宏"

    ```rust title="routes.rs"
    --8<-- "examples/tutorial/routes.rs:routes"
    ```

=== "手写"

    ```rust title="routes.rs"
    --8<-- "examples/manual/tutorial/routes.rs:routes"
    ```

## 路由器中间件 { #router-middleware }

路由器可以携带自己的层栈：`Router::layer` 会在挂载该路由器时包住其中的每一个处理器。应用的全局层栈
（用 `RustStream::layer` 添加）在 `include_router` 处包在它外面，作用域层层嵌套，应用在最外层：

=== "宏"

    ```rust title="main.rs"
    --8<-- "examples/logging_middleware.rs:layered_router"
    ```

=== "手写"

    ```rust title="main.rs"
    --8<-- "examples/manual/logging_middleware.rs:layered_router"
    ```

由于路由器隐藏了其中处理器的具体类型，能够触及它们的层必须是 `BlanketLayer`。这两种作用域、
`BlanketLayer` 这项要求，以及如何编写自己的层，都在[中间件](middleware.md#middleware-scopes)中说明。

## 组合与挂载

按模块构建路由器，再按服务的需要把它们组合起来：

<!-- inline-rust: illustrative multi-router composition with placeholder route modules; the compiled merge form is examples/routing.rs:merge, pulled in below -->
```rust
// 把多个路由器挂到同一个 Broker 上，include_router 可以调用多次。
RustStream::new(info).with_broker(broker, |b| {
    b.include_router(routes::orders());
    b.include_router(routes::shipping());
});
```

也可以在挂载之前把几个分组合并进一个路由器（完整程序见
[`examples/routing.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/routing.rs)）：

=== "宏"

    ```rust
    --8<-- "examples/routing.rs:merge"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/routing.rs:merge"
    ```

`merge` 会按顺序把另一个路由器的注册追加进来。每个路由器保留自己的编解码器和层栈；挂载结果时，外层
路由器的层（以及应用的全局层栈）会包在合并进来的路由器自己的层外面。

## 下一步

- 处理器的契约与 `#[subscriber]` 宏：[订阅者](subscribers.md)。
- `include` 如何解析出解码用的编解码器：[编解码器](codecs.md)。
