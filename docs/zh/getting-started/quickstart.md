# 快速上手

要让服务跑起来，最快的方式是用 `cargo generate` 生成项目骨架。

## 生成项目骨架

```bash
cargo install cargo-generate
cargo generate --git https://github.com/powersemmi/ruststream templates/memory --name my-service
cd my-service
```

生成骨架只需要 `cargo generate`。`templates/memory` 是基于内存 Broker 的起步模板。自带模板的 Broker
crate 用同样的方式生成，指明该 crate 的仓库和模板路径（例如
`--git https://github.com/powersemmi/ruststream-nats templates/nats`）；某个 Broker 有哪些模板，
以它自己的文档为准。`cargo generate` 会生成一个符合 Rust 习惯的多文件项目：

```
my-service/
├── Cargo.toml
└── src/
    ├── main.rs      # #[ruststream::app] 构建服务并挂载路由器
    ├── orders.rs    # 以 #[subscriber] 函数编写的处理器（其中一个会发布回复）
    └── routes.rs    # 把这些处理器汇总进一个 Router
```

## 运行起来

`#[ruststream::app]` 会生成 `main`，因此二进制程序已经支持框架的命令：

```bash
cargo run -- run                # 或者：装了 CLI 之后用 ruststream run
```

`cargo run -- run` 会启动 tokio 运行时。服务会一直运行，直到你按下
++ctrl+c++。运行服务不需要外部 Broker。

## 生成 AsyncAPI 文档

```bash
cargo run -- asyncapi gen
```

该命令以 JSON 格式打印 AsyncAPI 文档。输出选项（`-o`、`--yaml`）和文档本身，参见
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

你写的是一个构建服务的函数，`#[ruststream::app]` 把它变成 `main`。

## 下一步

- 在[教程](tutorial.md)中理解每个部分。
- 在[订阅者](../guides/subscribers.md)中了解处理器的各种写法。
- 用 [CLI](../guides/cli.md) 管理服务。
