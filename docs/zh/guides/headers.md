# 类型化消息头

消息头在传输时是一个无类型的 `name -> bytes` 映射，也就是 `HeaderMap`。当应用确实在其中承载了一份
真正的契约时（各种 id、序号、总数），一个结构体就能声明这份契约，并同时驱动三个面：消费侧的运行时
提取、生产侧的发布构建器，以及生成的 AsyncAPI 文档里的消息头 schema。

## 契约

消息头契约是一个扁平的结构体：每个字段对应一个消息头，取值为标量（数字、布尔、字符串、原始字节、
只含单元变体的枚举）或它们的 `Option`。在传输线路上每个值都以字符串编码，框架会把 `"3"` 解析进一个
`u32` 字段，写回时同样如此。schema 则依旧描述逻辑类型。

```rust
--8<-- "examples/typed_headers.rs:contracts"
```

字段名就是线路上的名字；对于不是 Rust 标识符的名字，用 `#[serde(rename = "x-task-id")]`。消息头缺失
时，`Option` 字段取 `None`；而缺失一个非 `Option` 的消息头就是违反契约。

## 接收侧：`Headers` 提取器

`Headers<T>` 是一个提取器参数：在函数体运行之前，运行时就把这次投递的消息头解析成 `T`，因此
处理器从一开始拿到的就是经过校验的类型化值。违反契约的情况（消息头缺失、值无法解析）绝不会到达
函数体，框架会先打出一条点名该订阅与契约类型的 `WARN`，然后按订阅者的 `on_failure(decode = ..)`
策略结算这次投递，也就是载荷解码失败时所用的同一套策略（默认丢弃）。

=== "宏"

    ```rust
    --8<-- "examples/typed_headers.rs:handler"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/typed_headers.rs:handler"
    ```

`Headers` 既能与自己完成反序列化的函数体（`&Frame<'_>` 挨着它的类型化消息头）组合，也能与其他
任何提取器组合。

在批量处理器上，消息头仍然是按投递存在的，所以页面把每个元素与它自己的契约配成一对：输入是
`&[Message<H, T>]`，`element.headers` 就挨着 `element.body`。这种配对在构造上就成立；载荷或消息头
无法成形的元素，会由同一套 `on_failure(decode = ..)` 策略结算，绝不会到达处理器，与单条消息的
路径完全一致。编译器会拒绝在这里写 `Headers<..>` 参数，并提示改用成对输入。

=== "宏"

    ```rust
    --8<-- "examples/typed_headers.rs:batch"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/typed_headers.rs:batch"
    ```

挂载的写法与其他任何形式一样，在两个面上也一样：在 Broker 作用域上写 `b.include(bulk)`，在路由器
路径上写 `Router::include`。契约类型随输入轴一起传递，所以普通的批量路由会在每个载荷旁边把它解码
出来。

如果同一个通道承载的消息，其消息头按事件种类各不相同，那就别让标准提取器插手，自己写一个
[`FromContext`] 提取器：先从无类型的消息头映射里读出用于判别的消息头（[`HeaderMap::get_str`]），
再按这个种类构造对应的契约。把各种形状的并集声明在输入类型上（见下一节），文档就仍然能展示出
完整的契约。

[`FromContext`]: https://docs.rs/ruststream/latest/ruststream/runtime/trait.FromContext.html
[`HeaderMap::get_str`]: https://docs.rs/ruststream/latest/ruststream/struct.HeaderMap.html#method.get_str

## 在消息类型上声明契约

`#[derive(Outgoing)]` 允许在目的地旁边写上 `headers = Meta`：契约就此成为类型的一部分。此后发布
构建器会精确地要求这些消息头，而 AsyncAPI 文档在该类型出现的每一处，都会把 schema 渲染在载荷旁边。
目的地那一半参见[发布](publishing.md#declaring-where-a-message-goes)。

=== "宏"

    ```rust
    --8<-- "examples/typed_headers.rs:messages"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/typed_headers.rs:messages"
    ```

## 发布侧：调用点上的契约

`Out` 槽位的标记列出了该槽位可以发布的消息类型：

=== "宏"

    ```rust
    --8<-- "examples/typed_headers.rs:dictionary"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/typed_headers.rs:dictionary"
    ```

`Out` 参数可选的第三个位置声明了该处理器所发布的消息集合：

- `Out<impl Publisher, Events>`（或显式写一个 `()`）：不设限制，任何已声明的消息都可以；
- `Out<impl Publisher, Events, (ChunkDone, Progress)>`：内联给出的列表；
- `Out<impl Publisher, Events, ChunkDone>`：单个已声明的类型（`#[derive(Outgoing)]` 的类型会声明
  它自己）；
- `Out<impl Publisher, Events, ConvertSends>`：一个 `#[derive(OutMessages)]` 枚举，其每个变体各包裹
  一个模型，构成一个可复用的具名集合（该枚举是类型层面的声明，绝不会构造出实例）。

函数体随后通过构建器发布（就像上面的处理器），而整份声明由编译器强制保证：

- 用声明集合之外的类型调用 `message(..)` 无法通过编译：处理器只发布它声明过的东西，别的一概不发；
- 声明了 `headers = Meta` 的类型只能经由 `.message(&value).with_headers(&meta)` 发布：忘记带消息头，
  或者传入错误的消息头类型，都无法通过编译；
- 目的地来自类型自身的声明，因此固定名字在调用点上什么都不必写，模板化的名字则会要求补齐它的
  占位符；
- 能力位置一如既往地会与挂载点的策略做静态检查：
  `Out<impl TransactionalPublish, Events, (ChunkDone, Progress)>` 要求所用策略的活发布者是
  事务性的，而声明过的那些发布会在条目打开的作用域内部完成，遵循同一份声明。

服务手上已经是编码后形态的载荷，或者无法承载声明的外部类型（比如裸的 `Vec<Frame>`），包在一个既
derive 了 `Outgoing` 又 derive 了 [`Serialized`](subscribers.md#raw-subscribers) 的 newtype 里发出。
这样的 newtype 是词典里的一等成员：它像任何
模型一样声明自己的目的地和消息头，并经由同一个类型化入口发布，写成 `out.message(&export)` -
类型把这次发布引到序列化的那条线上，字节按原样发出，而每个消息头位置照常工作。

契约把这个位置填掉一次。发布者自己补上的那部分走在下面：一个为每条发出的消息都携带同一个参数的
发布者，把该参数作为底交出来，契约的字段再逐个序列化到这层底之上 - 参见
[消息头从哪里来](publishing.md#where-the-headers-come-from)。

## 回复形式

`publish("dest")` 形式的处理器不需要额外声明：回复类型自带的契约会喂给文档，而目的地已经写在属性
里了。

=== "宏"

    ```rust
    --8<-- "examples/typed_headers.rs:reply"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/typed_headers.rs:reply"
    ```

在运行时，回复的消息头依旧沿用原来的做法：由回复发布者上的一个 `PublishTransform` 来设置，而
[`HeaderMap::insert_typed`] 负责在变换内部把一个契约值序列化进该映射。

[`HeaderMap::insert_typed`]: https://docs.rs/ruststream/latest/ruststream/struct.HeaderMap.html#method.insert_typed

## 文档里会呈现什么

启用 `asyncapi` feature 后，`build_spec` 会渲染出：

- 每条接收消息的消息头 schema，它来自处理器的 `Headers<T>` 参数；当处理器手工提取时，则来自
  输入类型的 `#[message(headers(..))]` 契约；
- 每条已声明的出站消息对应一个 `send` 操作，也就是每种 `publish(..)` 形式的回复，以及某个槽位声明
  的每种消息类型，各自带上自己的载荷 schema 与消息头 schema。

schema 描述的是逻辑字段类型（`task_id: integer`），而线路上的值是字符串编码的消息头。

## 测试

进程内的测试工具能驱动整条路径：注入构建器上的 `with_headers(&meta)` 发出一次带类型化契约的投递，
而发布日志会展示一次类型化发布所产生的消息头。

```rust
--8<-- "examples/typed_headers.rs:drive"
```
