# 快速上手

要最快跑起一个服务，就用 `cargo generate` 生成项目骨架。

## 生成项目骨架

```bash
cargo install cargo-generate
cargo generate --git https://github.com/powersemmi/ruststream templates/memory --name my-service
cd my-service
```

生成骨架只需要 `cargo generate`，不需要 `ruststream` CLI。`templates/memory` 是内存版的起步模板
（不依赖外部 Broker）；每个 Broker crate 都自带模板（例如
`--git https://github.com/powersemmi/ruststream-nats templates/nats`）。上面的命令会生成一个符合
Rust 习惯、由多个文件组成的项目：

```
my-service/
├── Cargo.toml
└── src/
    ├── main.rs      # #[ruststream::app] 构建服务并挂载路由器
    ├── orders.rs    # 以 #[subscriber] 函数编写的处理器（其中一个会发布回复）
    └── routes.rs    # 把这些处理器汇总进一个 Router
```

## 运行起来

`#[ruststream::app]` 会生成 `main`，因此生成的二进制程序已经认识框架的这些命令：

```bash
cargo run -- run                # 或者：装了 CLI 之后用 ruststream run
```

`cargo run -- run` 会启动一个 tokio 运行时并一直运行服务，直到你按下 ++ctrl+c++（`ruststream run`
这个 CLI 命令只是转发到它的便捷写法）。骨架项目用的是内存 Broker，所以运行时不需要任何外部依赖。

## 生成 AsyncAPI 文档

```bash
cargo run -- asyncapi gen
```

这会把 AsyncAPI 文档以 JSON 打印出来；输出相关的参数（`-o`、`--yaml`）以及文档本身，参见
[AsyncAPI 指南](../guides/asyncapi.md)。

## 入口点长什么样

=== "宏"

    ```rust title="src/main.rs"
    --8<-- "examples/tutorial/main.rs:main"
    ```

=== "手写"

    ```rust title="src/main.rs"
    --8<-- "examples/manual/tutorial/main.rs:main"
    ```

你写的是一个构建服务的函数；宏把它变成一个 `main`，由这个 `main` 来分发 `run` 和 `asyncapi gen`。

## 下一步

- 在[教程](tutorial.md)中理解每一个部分。
- 在[订阅者](../guides/subscribers.md)中了解处理器的各种写法。
- 从 [CLI](../guides/cli.md) 驱动这一切。
