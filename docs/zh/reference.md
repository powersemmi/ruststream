# API 参考

完整的 Rust API 参考由 rustdoc 生成，发布在 docs.rs 上。本站讲解概念，提供指南。

- **[docs.rs 上的 ruststream](https://docs.rs/ruststream)**，即该 crate。在启用全部 feature 的构建里，
  运行时、编解码器、AsyncAPI、指标和 conformance 各模块都可见：
  [docs.rs/ruststream（全部 feature）](https://docs.rs/crate/ruststream/latest/features)。

`ruststream` 命令行工具由同一个 crate 的 `cli` feature 提供。参见
[CLI 指南](guides/cli.md)。

## 在本地构建参考文档

```bash
cargo doc --all-features --open
```

## 主要入口

| 条目 | 模块 | 用途 |
|---|---|---|
| `RustStream` | `ruststream::runtime` | 应用对象 |
| `RunningApp` | `ruststream::runtime` | 已启动服务的句柄：就绪、fail-fast 故障信号和优雅关闭 |
| `Router` | `ruststream::runtime` | 一组处理器，在挂载时获得 Broker |
| `Handle`、`subscriber` | `ruststream::runtime` | 手写注册：处理器函数体的 trait，以及把函数体绑定到订阅来源 |
| `FromContext`、`State`、`FromRef` | `ruststream::runtime` / `ruststream` | 处理器的提取器参数，以及注入状态的 derive |
| `Broker`、`Subscribe`、`Subscriber`、`Publisher`、`IncomingMessage` | `ruststream` | Broker 契约 |
| `SubscriptionSource`、`Name` | `ruststream` | 订阅描述符 |
| `JsonCodec`、`MsgpackCodec`、`CborCodec` | `ruststream::codec` | 传输格式的编解码器 |
| `build_spec` | `ruststream::asyncapi` | 生成 AsyncAPI 文档 |
| `Metrics` | `ruststream::metrics` | Prometheus 指标 |
| `TestApp` | `ruststream::testing` | 应用的进程内单元测试套件 |
| `TestableBroker` | `ruststream::testing` | Broker 测试传输的契约 |
| `harness::run_suite` | `ruststream::conformance` | 供 Broker 作者使用的校验套件 |
