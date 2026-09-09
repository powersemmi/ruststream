# 日志

RustStream 在分发、发布和服务生命周期中发出结构化的 [`tracing`](https://docs.rs/tracing) 事件。
装哪个订阅者，由应用自己决定。`logging` feature 自带一个彩色的控制台订阅者。它从 `RUST_LOG`
读取过滤规则。

中间件 [`TracingLayer`](middleware.md#built-in-layers) 为每条消息发出一个事件，`logging` feature
装上的订阅者把它打印出来。

## 配合生成的 CLI

启用 `logging` feature 后，`#[ruststream::app]` 生成的 CLI 在 `run` 命令里自己装上订阅者。

```toml
ruststream = { version = "0.7", features = ["macros", "memory", "json", "logging"] }
```

```bash
RUST_LOG=ruststream=debug,info cargo run -- run
```

订阅者写入 **stderr**，以便 stdout 保持干净，留给 `asyncapi gen`。stderr 是终端时，颜色自动开启。

## 手工安装

在 `main` 的最开头安装一次默认订阅者：

<!-- inline-rust: manual logger-init fragment; the shipped logging example uses the automatic #[ruststream::app] installer, so there is no compiled call site for the by-hand path -->
```rust
ruststream::logging::init()?;
tracing::info!("service starting");
```

未设置 `RUST_LOG` 时，过滤规则是 `info`。要改动默认值，用 `Logging` 构建器：

<!-- inline-rust: manual Logging-builder fragment; the by-hand init path has no compiled call site (the logging example uses the automatic installer) -->
```rust
use ruststream::logging::Logging;

Logging::new()
    .with_default_filter("ruststream=debug,info")  // 在 RUST_LOG 未设置时使用
    .with_target(false)                            // 隐藏事件的 target 列
    .try_init()?;
```

`init` 和 `try_init` 不替换已经装好的订阅者，无论它来自你还是别的 crate。这样的调用返回
`LoggingInitError::AlreadyInitialized`。

## 换用你自己的订阅者

你也可以不用 `logging` feature，装上任何 `tracing` 事件的订阅者：基于 `tracing-subscriber` crate
的、基于 `tracing-bunyan-formatter` 的、带 OpenTelemetry 层的，或是你这套技术栈里通行的那一个。
