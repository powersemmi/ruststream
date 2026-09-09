# 模板契约

crate 为 [`cargo generate`](https://github.com/cargo-generate/cargo-generate) 提供模板。模板由同一个
crate 的 CI 校验，因此骨架不会偏离该 crate 的 API。

## 形态

模板是一个目录，`cargo generate` 按它生成骨架：

```
templates/<name>/
├── cargo-generate.toml   # 清单：描述，以及声明的占位符
├── Cargo.toml.liquid     # name = "{{project-name}}"；固定 ruststream 与 Broker crate 的版本
└── src/
    ├── main.rs           # #[ruststream::app] 构建器
    ├── orders.rs         # #[subscriber] 处理器
    └── routes.rs         # 把这些处理器收拢起来的 Router
```

- 占位符使用 cargo-generate 的 Liquid 语法。`{{project-name}}` 是内置占位符，取 `--name` 的值，因此
  最简模板不声明自己的占位符。
- 包清单名为 `Cargo.toml.liquid`，生成时 cargo-generate 会去掉 `.liquid` 后缀。该后缀不是装饰：
  cargo 在 git 源里查找包时会解析仓库中的每个 `Cargo.toml`，并且忽略 `exclude`。包名里的占位符会让
  cargo 拒绝这份清单，凡是通过 git 源依赖该 crate 的人都会看到这条错误。对 cargo 会解析的其他模板
  文件，也照此命名。
- 清单把 `ruststream` 固定在所支持的次版本，把 Broker crate 固定在它自己的版本。
- 每种 Broker 传输方式或拓扑对应一个模板：例如 `nats` 和 `nats-js`，或者 `redis-stream`、
  `redis-pubsub` 和 `redis-list`。

模板源文件带有 `{{...}}` 占位符，生成之前既不是合法的 Rust，也不是合法的 TOML。把它们放在 cargo
工作空间之外：`exclude = ["templates"]`。

## 由 CI 编译（这就是契约）

CI 从每个模板生成骨架，并按固定的版本编译它。破坏骨架的 API 变更会让该 crate 的 CI 失败，而不是
让用户的第一次构建失败。检查任务会：

1. 安装 `cargo-generate`，
2. 在临时目录里生成骨架（`cargo generate --path templates/<name> --name smoke`），
3. 在骨架里运行 `cargo check`。

该任务还会对骨架的清单做两处改动。先把 `ruststream` 的版本要求改成正在构建的那个版本，只有这样，
cargo 才能解析到尚未发布的预发布版。原因有二：`[patch.crates-io]` 只改 crate 的来源，不改版本范围；
cargo 不会把预发布版算进没有写明预发布的范围。再追加 `[patch.crates-io]` 本身，指向核心的本地检出。
这就是 Broker CI 已经在用的同级仓库布局。

## 只做加法的写法

按 feature 开启的模板块只添加代码：模板里不得出现 `{% else %}`，也不得出现 `{% if not flag %}`
这样的否定分支。于是不开 flag 的骨架是全开 flag 骨架的严格子集，每个模板运行一次全 feature 的
`cargo check`，就能发现与 API 的任何不一致。

关掉 flag 时只可能出现模板写法上的错误：悬空的 `use`、未填的槽位。在本地检查它们。

## 归属

- 核心（`ruststream`）只拥有 `templates/memory`，即它自带的内存 Broker 的模板，因此默认的
  `cargo generate` 离线可用，也不依赖 Broker crate。
- Broker crate 拥有自己各种传输方式的模板，并在自己的 CI 里校验它们。
