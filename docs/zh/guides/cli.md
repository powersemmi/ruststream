# CLI

`ruststream` 这个命令行工具用框架的子命令来驱动 `cargo`。生成新项目骨架是 `cargo generate` 的事
（见下面的[生成骨架](#scaffolding)），因此这个工具本身不提供 `new` 命令。

```bash
cargo install ruststream --features cli
```

一个 RustStream 服务就是普通的 Rust 二进制程序，只不过它的 `main` 由 `#[ruststream::app]` 生成。CLI
不会去内省它；`run` 和 `asyncapi gen` 都是对目标 crate 执行 `cargo run`。

## 命令

```bash
ruststream run                         # 对 ./Cargo.toml 执行 cargo run -- run
ruststream run -p ./my-service         # 针对另一个 crate
ruststream run --release               # release 构建
ruststream asyncapi gen                # 打印 AsyncAPI 文档
ruststream asyncapi gen -o spec.json   # 写入文件
ruststream asyncapi gen --yaml         # 输出 YAML 而不是 JSON
```

`run` 和 `asyncapi gen` 都接受 `-p/--manifest-path`（默认是当前目录），用来指向工作目录之外的另一个
crate。

## 生成出来的入口点

`#[ruststream::app]` 把一个构建器函数变成认识 `run` 和 `asyncapi gen` 的 `main`，因此不需要任何运行时
样板代码：

```rust
use ruststream::memory::MemoryBroker;
use ruststream::runtime::{AppInfo, RustStream};

--8<-- "examples/quickstart.rs:app"
```

由于分发逻辑就在生成出来的二进制程序里，`ruststream run` 和直接 `cargo run -- run` 启动服务的方式完全
一样。`ruststream run` 只是一个便捷写法：找到那个 crate，再把命令转发给 `cargo`。

## 生成骨架 { #scaffolding }

新项目由 [`cargo generate`](https://github.com/cargo-generate/cargo-generate) 从模板生成，而不是由这个
工具生成；具体命令以及它写出的项目见[快速上手](../getting-started/quickstart.md)。模板归它所接线的那个
Broker 所属的 crate 所有：内存版的起步模板在本仓库里，每个 Broker 仓库则各自提供自己的模板，通常是每种
传输方式或拓扑一个（例如 `nats` 与 `nats-js`）。为一个新的 Broker 编写模板，请遵循
[模板契约](../broker-authors/templates.md)。
