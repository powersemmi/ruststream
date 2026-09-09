# 类型化消息头

消息头是一个无类型的映射，键是名字，值是字节。类型化消息头是一个结构体，它声明消息有哪些消息头，
以及每个消息头是什么类型。你可以在订阅者和发布者上声明它。

## 契约

消息头契约是一个扁平的结构体，字段都是标量：数字、布尔值、字符串、原始字节和不带数据的枚举。

```rust
--8<-- "examples/typed_headers.rs:contracts"
```

如果消息头的名字不是合法的 Rust 标识符，你可以用 `#[serde(rename = "x-task-id")]` 指定它。
`Option` 字段声明一个可选的消息头。

消息头里的值以字符串保存：框架把 `"3"` 解析进 `u32` 字段，写回时也一样。

## 接收侧：`Headers` 提取器

`Headers<T>` 是一个提取器：在函数体运行之前，运行时先把这次投递的消息头解析成契约 `T`。

=== "宏"

    ```rust
    --8<-- "examples/typed_headers.rs:handler"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/typed_headers.rs:handler"
    ```

必需的消息头缺失，或者值无法解析，这次投递就到不了函数体。结算它的是订阅者的
`on_failure(decode = ..)` 策略，载荷无法解码时用的也是这一条，默认为 `drop`。
在此之前，框架会写一条 `WARN`，其中写明订阅的名字和契约类型。

`Headers` 可以和任何其他提取器组合，也可以和自己完成反序列化的函数体组合。

在一个批次里，消息头仍然属于各自的投递，所以批量处理器的输入是 `&[Message<H, T>]`。

=== "宏"

    ```rust
    --8<-- "examples/typed_headers.rs:batch"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/typed_headers.rs:batch"
    ```

载荷或消息头无法解析的元素到不了处理器，结算它的还是同一条策略。在这里写 `Headers<..>` 参数
无法通过编译，编译错误会指出配对的输入。

这样的处理器和其他处理器一样挂载：在 Broker 作用域上写 `b.include(bulk)`，在路由器路径上写
`Router::include`。

当同一个订阅收到的消息带有不同的消息头集合时，你可以不用 `Headers`，自己写一个 [`FromContext`]
提取器：它从无类型的映射里读出判别用的消息头（[`HeaderMap::get_str`]），再组装这类事件所要求的
契约。把所有形状的并集声明在输入类型上（见下一节）。

[`FromContext`]: https://docs.rs/ruststream/latest/ruststream/runtime/trait.FromContext.html
[`HeaderMap::get_str`]: https://docs.rs/ruststream/latest/ruststream/struct.HeaderMap.html#method.get_str

## 在消息类型上声明契约

`#[derive(Outgoing)]` 允许在目的地旁边写 `headers = Meta`，契约就此成为类型的一部分。目的地怎么
声明，见[发布](publishing.md#declaring-where-a-message-goes)。

=== "宏"

    ```rust
    --8<-- "examples/typed_headers.rs:messages"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/typed_headers.rs:messages"
    ```

## 发布侧：调用点上的契约

`Out` 槽位的标记列出该槽位可以发布的消息类型：

=== "宏"

    ```rust
    --8<-- "examples/typed_headers.rs:dictionary"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/typed_headers.rs:dictionary"
    ```

`Out` 参数可选的第三个位置声明这个处理器发布的消息集合：

- `Out<impl Publisher, Events>`（或显式写出 `()`）：不加限制，任何已声明的消息都可以发布；
- `Out<impl Publisher, Events, (ChunkDone, Progress)>`：内联给出的列表；
- `Out<impl Publisher, Events, ChunkDone>`：单个已声明的类型（带 `#[derive(Outgoing)]` 的类型
  声明它自己）；
- `Out<impl Publisher, Events, ConvertSends>`：一个 `#[derive(OutMessages)]` 枚举，每个变体包裹
  一个模型，构成可供多个处理器复用的具名集合。该枚举是类型层面的声明，从不构造实例。

随后函数体通过发布构建器发布（就是上面的处理器），整份声明由编译器检查：

- 用声明集合之外的类型调用 `message(..)` 无法通过编译；
- 声明了 `headers = Meta` 的类型只能经由 `.message(&value).with_headers(&meta)` 发布；
- 目的地取自类型自己的声明：固定的名字在调用点上什么都不用写，模板化的名字要求补上占位符；
- 能力位置在编译期与注册处理器时给出的策略核对：`Out<impl TransactionalPublisher, Events,
  (ChunkDone, Progress)>` 要求该策略构造的发布者是事务性的，而声明过的那些发布在这个发布者的
  事务里完成，遵循同一份声明。

已经编码好的载荷，或者无法承载声明的外部类型（比如 `Vec<Frame>`），你可以包进一个同时 derive
`Outgoing` 和 [`Serialized`](subscribers.md#raw-subscribers) 的 newtype。
它像任何模型一样声明自己的目的地和消息头，经由同一个 `out.message(&export)` 原样发出字节。

发布者可以给出一组自己的消息头作为基础，契约的字段再逐个写在它之上，参见
[消息头从哪里来](publishing.md#where-the-headers-come-from)。

## 回复形式

带 `publish("dest")` 的处理器不需要额外声明：目的地写在属性里，消息头写在回复类型的契约里。

=== "宏"

    ```rust
    --8<-- "examples/typed_headers.rs:reply"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/typed_headers.rs:reply"
    ```

回复的消息头由回复发布者上的 `PublishTransform` 设置，在变换内部由
[`HeaderMap::insert_typed`] 把契约值写进映射。

[`HeaderMap::insert_typed`]: https://docs.rs/ruststream/latest/ruststream/struct.HeaderMap.html#method.insert_typed

## 文档里会呈现什么

启用 `asyncapi` feature 后，`build_spec` 会为每条消息渲染消息头 schema：

- 接收的消息：schema 取自处理器的 `Headers<T>` 参数；处理器手工提取消息头时，取自输入类型的
  `#[message(headers(..))]` 契约；
- 发出的消息：schema 取自类型自身声明的契约。

schema 描述的是字段的逻辑类型：`task_id: integer`。文档的其余内容由
[AsyncAPI 指南](asyncapi.md)讲解。

## 测试

进程内的测试工具会跑通整条路径：注入构建器上的 `with_headers(&meta)` 发出一次带类型化契约的投递，
发布日志则显示一次类型化发布写出的消息头。

```rust
--8<-- "examples/typed_headers.rs:drive"
```
