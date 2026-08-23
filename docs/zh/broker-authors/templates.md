# 模板契约

一个 crate 如何提供与自身 API 保持同步的
[`cargo generate`](https://github.com/cargo-generate/cargo-generate) 骨架。这份契约是
[conformance 校验套件](conformance.md)在骨架生成这一侧的对应物：模板是由 CI 编译的产物，属于它所接线
的 Broker 所在的 crate，而不是核心 CLI 里手工维护的一堆字符串。核心只提供内存版的
`templates/memory`，每个 Broker crate 拥有自己各种传输方式的模板。

## 形态

模板就是一个由 `cargo generate` 渲染的目录：

```
templates/<name>/
├── cargo-generate.toml   # 清单：描述，以及声明的占位符
├── Cargo.toml            # name = "{{project-name}}"；固定 ruststream 与 Broker crate 的版本
└── src/
    ├── main.rs           # #[ruststream::app] 构建器
    ├── orders.rs         # #[subscriber] 处理器
    └── routes.rs         # 把这些处理器收拢起来的 Router
```

- 占位符使用 cargo-generate 的 Liquid 语法；`{{project-name}}`（即 `--name` 的取值）是内置的，因此一个
  最简模板一个占位符都不用声明。
- `Cargo.toml` 把 `ruststream` 固定到所支持的次版本，把 Broker crate 固定到它自己的版本。
- 每种 Broker 传输方式或拓扑对应一个模板（例如 `nats` 与 `nats-js`，或者 `redis-stream` /
  `redis-pubsub` / `redis-list`），与“一个模板对应一种形态”的模型保持一致。

模板源文件里带着 `{{...}}` 占位符，因此在渲染之前它们并不是合法的 Rust/TOML，必须留在 crate 的 cargo
workspace 之外（`exclude = ["templates"]`）。

## 由 CI 编译（这就是契约）

各个归属仓库的 CI 会渲染每一个模板，并针对固定的版本编译它，于是破坏骨架的 API 变更会让归属仓库的 CI
失败，也就是让问题暴露在该修它的地方，而不是暴露在用户的第一次 `cargo build` 上。该防漂移任务会：

1. 安装 `cargo-generate`，
2. 把模板渲染到一个临时目录（`cargo generate --path templates/<name> --name smoke`），
3. 对渲染出来的项目执行 `cargo check`。

在所支持的 `ruststream` 版本发布之前，该任务会往渲染出来的项目里注入一段 `[patch.crates-io]` 指向本地
检出（也就是 Broker CI 已经在用的同级检出布局），这样骨架就能针对尚未发布的版本完成编译。

## 只做加法的写法

feature 分支只能做加法，不得出现 `{% else %}` 或 `{% if not flag %}` 这类否定分支。这样一来，不开任何
flag 的渲染结果就是全开渲染结果的严格子集，因此每个模板只需一次全 feature 的 `cargo check` 就能抓住所有
API 漂移导致的破坏，没有任何东西能藏在关闭的分支里。关闭 flag 的各种组合属于静态写法层面的问题（悬空的
`use`、没填上的槽位），在本地检查即可，不必进 CI。

## 归属

- 核心（`ruststream`）只拥有 `templates/memory`（也就是它自带的进程内 Broker），这样默认的
  `cargo generate` 离线可用，且不引入任何 Broker 依赖。
- 每个 Broker crate 拥有自己各种传输方式的模板，并在自己的 CI 里跑该防漂移任务。
- 这样划分能让 Broker 专有的 API 不进入核心 CLI：核心对某个 Broker 的描述符一无所知，模板则与定义它们
  的 crate 待在一起。
