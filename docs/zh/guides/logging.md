# 日志

RustStream 在分发、发布以及服务生命周期的各处都会发出结构化的
[`tracing`](https://docs.rs/tracing) 事件。它自己不安装任何订阅者，那是应用该做的选择。`logging`
feature 提供了一个：由 `RUST_LOG` 驱动的、带颜色的控制台订阅者。

这与 [`TracingLayer`](middleware.md#built-in-layers) 中间件是两回事。`TracingLayer` 为每条消息*发出*
一个事件；`logging` feature 装的是一个把事件（RustStream 自己的和你的）*渲染*到终端的订阅者。两者配合
使用，才能看到每条消息的日志。

## 配合生成的 CLI

启用 `logging` feature 之后，`#[ruststream::app]` 生成的 CLI 会在 `run` 命令里替你调用日志器，因此用
骨架生成的服务开箱即有日志：

```toml
ruststream = { version = "0.7", features = ["macros", "memory", "json", "logging"] }
```

```bash
RUST_LOG=ruststream=debug,info cargo run -- run
```

输出走 **stderr**（好让 stdout 干净地留给 `asyncapi gen`），当 stderr 是终端时会自动开启颜色。

## 手工安装

在 `main` 的最开头把默认日志器安装一次：

<!-- inline-rust: manual logger-init fragment; the shipped logging example uses the automatic #[ruststream::app] installer, so there is no compiled call site for the by-hand path -->
```rust
ruststream::logging::init()?;
tracing::info!("service starting");
```

`init` 从 `RUST_LOG` 读取过滤规则，读不到时回落为 `info`。要调整这些默认值，用 `Logging` 构建器：

<!-- inline-rust: manual Logging-builder fragment; the by-hand init path has no compiled call site (the logging example uses the automatic installer) -->
```rust
use ruststream::logging::Logging;

Logging::new()
    .with_default_filter("ruststream=debug,info")  // 在 RUST_LOG 未设置时使用
    .with_target(false)                            // 隐藏事件的 target 列
    .try_init()?;
```

`init` / `try_init` 绝不会替换已经存在的订阅者：第二次调用（或者在别的 crate 已经装了订阅者之后调用）
会返回 `LoggingInitError::AlreadyInitialized`，而不是 panic。

## 换用你自己的订阅者

`logging` feature 只是可选的语法糖。由于 RustStream 只负责发出 `tracing` 事件，任何订阅者都能用：装上
`tracing-subscriber`、`tracing-bunyan-formatter`、某个 OpenTelemetry 层，或者你这套技术栈里用的任何
东西，同样的事件都会流经它。这种情况下就不必启用 `logging` feature 了。
