# API 参考

完整的 Rust API 参考由 rustdoc 生成并发布在 docs.rs 上。本站点讲的是概念与指南；每个类型、trait 和
函数签名，以 docs.rs 为准。

- **[docs.rs 上的 ruststream](https://docs.rs/ruststream)**，即该 crate。启用全部 feature 构建它，
  就能看到运行时、编解码器、AsyncAPI、指标和 conformance 这几个模块：
  [docs.rs/ruststream（全部 feature）](https://docs.rs/crate/ruststream/latest/features)。

`ruststream` 命令行工具就是同一个 crate 的 `cli` feature，并非独立的 crate；参见
[CLI 指南](guides/cli.md)。

## 在本地构建参考文档

```bash
cargo doc --all-features --open
```

## 主要入口

| 条目 | 模块 | 用途 |
|---|---|---|
| `RustStream` | `ruststream::runtime` | 应用对象 |
| `RunningApp` | `ruststream::runtime` | 已启动的服务：就绪状态、fail-fast 信号、优雅关闭 |
| `Router` | `ruststream::runtime` | 延迟绑定的一组处理器 |
| `Handle`、`subscriber` | `ruststream::runtime` | 手写路径唯一的函数体 trait，以及它唯一的挂载构造器 |
| `FromContext`、`State`、`FromRef` | `ruststream::runtime` / `ruststream` | 处理器的提取器参数，以及状态注入的 derive |
| `Broker`、`Subscribe`、`Subscriber`、`Publisher`、`IncomingMessage` | `ruststream` | Broker 契约 |
| `SubscriptionSource`、`Name` | `ruststream` | 订阅描述符 |
| `JsonCodec`、`MsgpackCodec`、`CborCodec` | `ruststream::codec` | 线上格式的编解码器 |
| `build_spec` | `ruststream::asyncapi` | AsyncAPI 生成 |
| `Metrics` | `ruststream::metrics` | Prometheus 指标 |
| `TestApp` | `ruststream::testing` | 进程内的应用单元测试套件 |
| `TestableBroker` | `ruststream::testing` | Broker 测试传输契约 |
| `harness::run_suite` | `ruststream::conformance` | conformance 校验套件 |
