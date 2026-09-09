# CLI

`ruststream` 命令行工具用框架的子命令调用 `cargo`。新项目的骨架由 `cargo generate` 生成
（见下面的[生成骨架](#scaffolding)）。

```bash
cargo install ruststream --features cli
```

RustStream 服务就是普通的 Rust 二进制程序。它的 `main` 由 `#[ruststream::app]` 生成。
`run` 和 `asyncapi gen` 会对目标 crate 执行 `cargo run`。

## 命令

```bash
ruststream run                         # 对 ./Cargo.toml 执行 cargo run -- run
ruststream run -p ./my-service         # 针对另一个 crate
ruststream run --release               # release 构建
ruststream asyncapi gen                # 打印 AsyncAPI 文档
ruststream asyncapi gen -o spec.json   # 写入文件
ruststream asyncapi gen --yaml         # 输出 YAML 而不是 JSON
```

`run` 和 `asyncapi gen` 都接受 `-p/--manifest-path`，即服务所在 crate 的路径。默认值是当前目录。

## 生成出来的入口点

`#[ruststream::app]` 把构建器函数变成能识别 `run` 和 `asyncapi gen` 的 `main`：

=== "宏"

    ```rust
    use ruststream::memory::MemoryBroker;
    use ruststream::runtime::{AppInfo, RustStream};

    --8<-- "examples/quickstart.rs:app"
    ```

=== "手写"

    ```rust
    use ruststream::memory::MemoryBroker;
    use ruststream::prelude::*;

    --8<-- "examples/manual/quickstart.rs:app"
    ```

`ruststream run` 和 `cargo run -- run` 以同样的方式启动服务。

## 生成骨架 { #scaffolding }

新项目由 [`cargo generate`](https://github.com/cargo-generate/cargo-generate) 按模板生成。
该命令和生成出来的项目，参见[快速上手](../getting-started/quickstart.md)。

模板属于它所用的 Broker 所在的 crate。内存 Broker 的起步模板在本仓库里。
每个 Broker 仓库提供自己的模板，通常一种传输方式或拓扑对应一个，例如 `nats` 和 `nats-js`。
[模板契约](../broker-authors/templates.md)说明如何为新的 Broker 编写模板。
