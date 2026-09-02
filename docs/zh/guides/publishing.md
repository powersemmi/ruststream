# 发布与回复

发布有两种方式：从处理器返回一个回复，或者通过 `Out` 参数注入到处理器里的发布者显式发布。无论哪一种，
处理器都不会见到一个尚未连接的发布者：注册携带的是发布*策略*（纯粹的声明），运行时会在启动时把它们与
已连接的 Broker 配对。

显式发布用的始终是同一个构建器：发布一个值时从 `message(..)` 进入，发布字节时从 `raw(..)` 进入，最后
以 `publish()` 收尾。调用点必须填哪些位置（目的地、类型化的消息头、编解码器），由消息类型自己的声明
决定，因此一次信息不全的发布是编译错误，而不是运行时的意外。`Publisher::publish` 仍然在底下，但它是
Broker crate 要实现的接口（参见 [Broker 作者](../broker-authors/index.md)）；服务代码写的是构建器。

## 从处理器回复

用 `publish(..)` 指定一个回复目的地，然后返回回复值。运行时会把它编码并发送出去：

=== "宏"

    ```rust
    use ruststream::subscriber;

    --8<-- "examples/publishing.rs:reply"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/publishing.rs:reply"
    ```

用普通的 `include` 挂载它。如果不再多说什么，回复就会以默认编解码器、经由 Broker 的默认发布策略发出；
要指定回复的编解码器或者加上变换，就用 `.publisher(..)` 链上一个架在 Broker 发布策略之上的
[`TypedPublisher`] 栈（`TypedPublisher::new` 用默认编解码器，`TypedPublisher::with_codec` 则可以指定
一个）。该栈是一份声明：运行时会在启动时把它与已连接的 Broker 配对。

=== "宏"

    ```rust
    --8<-- "examples/publishing.rs:reply_mount"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/publishing.rs:reply_mount"
    ```

入站请求的解码遵循作用域（用 `with_broker_codec` 设定的作用域编解码器，没有设定则用默认编解码器）；
回复的编解码器则随着附加上去的栈一起走。参见[编解码器](codecs.md#the-publish-side)。

## 控制确认行为

普通的回复写法总是先发布再 ack。想自己掌控，就改成返回 `Result<Reply, HandlerOutcome>`：`Ok(reply)` 会
发布并 ack，`Err(outcome)` 什么都不发布，由分发器按返回的 `HandlerOutcome` 行事（`HandlerOutcome::drop()`
表示进死信，`HandlerOutcome::retry()` 表示请求重新投递）：

=== "宏"

    ```rust
    --8<-- "examples/publishing.rs:reply_result"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/publishing.rs:reply_result"
    ```

`Result` 这种写法由写出来的签名识别，所以要把它明明白白地写出来（藏起 `Result` 的类型别名，宏会
当成普通的回复类型）。和任何处理器一样，会发布的处理器也可以声明一个可选的第二参数
`&mut Context`，用来读取应用状态或者手动发布。

如果回复的发布本身失败了（Broker 拒绝、连接断开），运行时会以 `requeue = true` nack 入站消息：
Broker 会重新投递它，而不是让回复悄无声息地丢掉。务必让会发布的处理器在重新投递之下保持幂等。

## 在处理器内部发布 { #publishing-from-inside-a-handler }

要发布到单条回复之外的目的地（算出来的目的地、扇出、副作用），就用 `Out` 把发布者作为处理器参数拿
进来：`Out(out): Out<impl Publisher>` 这种模式会把 `out` 绑定成函数体内一个活的发布者。签名里只写出
处理器需要的那项能力，绝不写 Broker 的发布者类型：具体类型由挂载处理器时附上的策略推断出来，运行时会
在 Broker 连接之后完成配对。同一个处理器既能原封不动地挂到生产 Broker 上，也能挂到它的进程内测试
传输上。

=== "宏"

    ```rust
    use ruststream::runtime::Out;

    --8<-- "examples/publishing.rs:forward"
    ```

=== "手写"

    ```rust
    use ruststream::runtime::Out;

    --8<-- "examples/manual/publishing.rs:forward"
    ```

`message(&value)` 用作用域的编解码器编码（想给单次调用换一个，用 `.with_codec(..)`）；`raw(&bytes)`
发送的是服务已经编码好的载荷，因此根本没有编解码器的位置。两者都用 `.with_headers(..)` 填上消息头
这一位置 - 按引用传消息自己声明的契约（`&meta`），或者按值传一张已经建好的 `HeaderMap` - 也都以
`publish()` 收尾。

挂载点指明来源；对作用域自己的 Broker 来说，来源就是发布策略：

=== "宏"

    ```rust
    --8<-- "examples/publishing.rs:forward_mount"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/publishing.rs:forward_mount"
    ```

一个没有绑定的 `Out` 槽位是编译错误，而不是运行时错误：在每个槽位都有策略之前，这次注册根本构建
不出来。

### 具名槽位 { #named-slots }

需要多个发布者的处理器要为每个参数指定一个**槽位标记**：一个派生了 `OutSlot` 的单元结构体，写在第二个
类型参数的位置（`Out<impl Publisher, Primary>`）。挂载点用 `.out(marker, policy)` 绑定每一个标记，再用
收尾的 `.build()` 提交这次注册。这些调用是按标记绑定的，所以先后顺序无关紧要；把同一个槽位绑定两次
（或者绑定一个处理器没有声明的标记）无法通过编译，而 `.build()` 只有在每个槽位都绑定之后才存在，漏掉
一次绑定就是编译错误，其附着类型会点名是哪个槽位（`MissingSlot<Audit>`）。如果只有一个无名的
`Out<impl Publisher>` 参数，它绑定的是隐含的 `DefaultSlot`，用普通的 `.publisher(policy)` 调用即可，
绑定和提交一步完成。

=== "宏"

    ```rust
    use ruststream::OutSlot;

    --8<-- "examples/publishing.rs:slots"
    ```

    ```rust
    --8<-- "examples/publishing.rs:slots_mount"
    ```

=== "手写"

    ```rust
    use ruststream::OutSlot;

    --8<-- "examples/manual/publishing.rs:slots"
    ```

    ```rust
    --8<-- "examples/manual/publishing.rs:slots_mount"
    ```

trait 约束里的能力还可以收窄：`Out<impl OwnedTransactions, Ledger>` 只有在策略的活发布者支持 owned
事务时才能编译，这一点在挂载点检查，并给出点名缺失能力的诊断信息。槽位标记同时也是[测试套件](testing.md#asserting-on-out-slots)
记录发布时所用的身份标识。

`Out` 参数可选的第三个位置声明该处理器会发送什么（`Out<impl Publisher, Marker, (A, B)>`，可以是单个
类型，也可以是一个 `#[derive(OutMessages)]` 的集合枚举）；标记自身的 `#[publishes(A, B)]` 列表则说明
该槽位允许发布什么，处理器若不限制第三个位置，生成的文档报告的就是这份列表。类型化地发布一个标记
没有列出的类型是编译错误，错误会点明缺失的那条成员关系。什么都不列的标记就做不了任何类型化的发布；
而通过
`raw(..)` 的字节发布不受影响，它们没有可列出的消息类型。单个无名 `Out<impl Publisher>` 的隐含
`DefaultSlot` 没有可以列类型的声明处，所以它接受每一种已声明的消息。参见[类型化的消息头](headers.md)。

### 声明消息发往何处 { #declaring-where-a-message-goes }

一个消息类型通过一次派生声明自身发送方式的全部信息，每个参数都写成同样的 `key = value` 形式。
`name` 是目的地，`headers` 指明契约类型（它仍然是一个普通的 serde 结构体，派生不会碰它）：

=== "宏"

    ```rust
    use ruststream::Outgoing;

    --8<-- "examples/publishing.rs:declared"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/publishing.rs:declared"
    ```

这份声明决定了调用点拥有哪一种目的地位置：

- **固定的名字**已经把目的地定死了，所以没有 `to(..)` 可写，也没有办法把该类型发到文档没有提到的
  地方。
- **名字模板**（`"orders.{tenant}.placed"`）会开放 `to()`，它返回一个构建器，每个占位符对应一个
  setter。只有当每个占位符都绑定之后 `publish()` 才能编译通过；未绑定的占位符会体现在构建器的
  类型里，因此编译错误会说明地址还没写完，并点名遗漏的那一段。地址是每次发布时渲染的；固定的名字
  则让发布从一个 `&'static str` 出发。
- **完全不写 `name`** 意味着由调用点来指定：`.to("orders.archived")`，接受一个 `&str` 或者一个算出来
  的 `String`。

声明了 `headers = Meta` 的消息只能带着 `.with_headers(&meta)` 发布，忘了写它或者传了别的类型都无法
通过编译。在生成的文档里，固定的名字成为它的 channel，模板成为一个带模板的地址，其参数块由占位符
填充，而不声明任何目的地的类型什么也不贡献。

正是这次派生让一个值能这样发布，第三种情形也不例外。由别的 crate 拥有的 `Serialize` 类型没法派生
`Outgoing`，因此进不了该构建器：把它包进一个派生了 `Outgoing` 的 newtype，或者在事务内部继续用
作用域的 `publish(name, &value)`。

=== "宏"

    ```rust
    --8<-- "examples/publishing.rs:declared_mount"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/publishing.rs:declared_mount"
    ```

该参数可以和每一种订阅者写法组合：与 `Ctx` 提取器并列、用在以字节为输入的处理器上、也用在批量处理器
上（`b.include(f).publisher(..)`，进来的是一整页，出去的是逐元素的目的地）。在回复写法上，也就是
`publish(..)` / `publish_raw(..)` 以及它们的批量对应形式，`.publisher(..)` 仍然是回复自己的附加项，
注入的发布者则用 `.out(marker, ..)` 加上收尾的 `.build()` 来附加（单个无名槽位用 `DefaultSlot`），
于是一个网关可以在固定的目的地上作答，同时通过注入把副本扇出出去：

=== "宏"

    ```rust
    --8<-- "examples/publishing.rs:publish_out"
    ```

    ```rust
    --8<-- "examples/publishing.rs:publish_out_mount"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/publishing.rs:publish_out"
    ```

    ```rust
    --8<-- "examples/manual/publishing.rs:publish_out_mount"
    ```

### 发布到另一个 Broker

当处理器从一个 Broker 消费而发布到另一个时（从 Kafka 消费，转发到 Redis），先用 `.bindable()` 包住
目标 Broker，并在注册之前铸出一个**绑定令牌**。令牌在任何 `with_broker` 运行之前就已经存在，所以
注册顺序无关紧要，一座双向的桥可以一上来就把两个方向都绑好。随后令牌就是挂载点上的来源（这里用
两个内存 Broker 演示，任意一对 Broker 的写法都一样）：

=== "宏"

    ```rust
    --8<-- "tests/out_injection.rs:cross_broker"
    ```

=== "手写"

    ```rust
    --8<-- "tests/manual_out_injection.rs:cross_broker"
    ```

令牌与铸出它的 `Bindable` 包装器共享同一个槽位，所以要注册同一个包装器（`with_broker(bindable, ..)`），
启动过程才会把已连接的 Broker 填进该槽位；如果某个令牌的 Broker 从未注册，配对时就会带着清晰的错误
快速失败。回复发布（在 `publish("dest")` 处理器上写
`.publisher(token)`）和批量写法用的是同一套形态。在注册之外，令牌会在启动连接了它的 Broker 之后自行
完成配对：`running.publisher(token)` 会把活的发布者交给同级的任务，参见
[与其他服务器并行运行](http.md)。而对于启动时的第一次发布，根本不需要令牌：作用域级别的
`b.after_startup(policy, hook)` 会在订阅打开之后，用一个已经配对好的发布者运行该钩子（参见
[应用生命周期](lifespan.md#lifecycle-hooks)）；发布示例里的预填数据就是搭它的车。

## 消息头从哪里来 { #where-the-headers-come-from }

一次发布的消息头来自两处。调用点用 `.with_headers(..)` 指定它们：按引用传入消息声明的契约，或者
按值传入一份已经建好的 `HeaderMap`。而发出它们的那个发布者还能在下面垫一层自己的底：一个为
一整串消息携带同一个参数的发布者（租户、分区提示、Broker 用消息头表达的某个投递选项），把这个参数
从 `base_headers` 交出来；从它开启的事务同样如此。

构建器只组装一次出站映射：先铺底，再把调用点的消息头逐个键覆盖上去，最具体的一级取胜：

- **调用点** 在它指名的每个键上压过发布者；
- **发布者** 在调用点没有碰过的每个键上压过空无一物；
- 没有底的发布者让调用点的消息头保持原样。

两种写法的合并方式相同：映射逐条覆盖写入，声明的 `headers = Meta` 契约则把自己的字段逐个序列化到
底层之上，因此带契约的消息仍然捎着发布者的那个参数。

`.with_headers(..)` 依然只能填一次：第二次调用是编译期错误。

## 发布管线 { #the-publish-pipeline }

消息离开进程之前会跑过两类变换，而且它们可以组合：

- **`TypedPublisher` 上的静态 `PublishTransform`**，用 `.transform(..)` 添加。这是零成本、按目的地
  生效的变换（一层信封、一个固定的 content type，或者把这次投递的链路追踪 / 关联 id 盖到回复上）。
  它们最先运行，离值最近。
- **应用上的静态 `PublishLayer`**，用 `.publish_layer(..)` 添加。这是横切关注点（发布指标、死信包装），
  作用于每一条发布出去的消息，包在发送外面，因此能观察到发送的结果。整条链会组合成一个具体类型，
  于是它成为应用类型的一部分。构建器通常返回 `impl App`，从不把它写出来；
  一旦写出具体的 `RustStream<L, St, PublishStack<MyMiddleware, PublishIdentity>>`，管线就出现在那里，
  而没有 `publish_layer` 的应用保持默认的 `PublishIdentity`。每个中间件都必须是 `Clone` 的（管线会
  克隆进每一个会发布的处理器），最后添加的中间件跑在最外层。默认情况（没有中间件）就是直接发送。如果
  中间件的组合要到运行时才决定，就把它包进 `PublishDynStack`（`DynStack` 在发布侧的对应物）再添加。

静态的 `PublishTransform` 实现 `apply(&mut Outgoing<'_>, &PublishContext<'_, C>)`；`PublishContext` 是
产生这条回复的那次投递的只读视图（它的 channel、入站消息头，以及按 `Field` 键取用的 Broker 类型化单条
投递上下文），因此一个变换可以把入站消息里的值带到回复上：

```rust
--8<-- "examples/publishing.rs:static_transform"
```

批量处理器的回复会跳过按消息生效的 `.transform(..)` 栈；要在那里加变换，用 `.batch_transform(..)`，
并可以通过 `for_batch(transform)` 复用一个按消息的 `PublishTransform`。

`PublishLayer` 实现的是 around/next 形式的签名，因此它可以短路、重试，或者只做观察（“动态”一词
留给 `PublishDynStack` 里的 `PublishDynLayer`）：

```rust
--8<-- "examples/publishing.rs:app_layer"
```

两个层次都能在应用上组合起来：

=== "宏"

    ```rust
    --8<-- "examples/publishing.rs:pipeline"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/publishing.rs:pipeline"
    ```

这条管线跑在回复路径上（`publish(..)` 那种写法）。注入的 `Out` 发布者是所附策略的活形态，直接使用，
因此按发布者生效的变换要在挂载点用 `TypedPublisher::transform` 组合进策略里。完整的程序见
[`examples/publishing.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/publishing.rs)。

## 批量回复与事务

一个接受 `&[T]` 的 `#[subscriber("in", publish("out"))]` 处理器会消费整个解码后的批量，并返回这一批的
回复，也就是 consume-transform-produce 模式。`Ok(replies)` 把每一条回复发布到回复名下，并 ack 整批；
`Err(outcome)` 什么都不发布，并用 `outcome` 结算整批（全有或全无：逐元素挑选结果与事务无法组合）：

=== "宏"

    ```rust
    --8<-- "examples/publishing.rs:batch_publishing"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/publishing.rs:batch_publishing"
    ```

用 `include` 挂载它，并用 `.publisher(..)` 链上回复的接线：

=== "宏"

    ```rust
    --8<-- "examples/publishing.rs:batch_publishing_mount"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/publishing.rs:batch_publishing_mount"
    ```

用普通的 `TypedPublisher` 时，每条回复各自独立发布；批量中途失败会重试整批，因此先前那些回复可能在
重新投递时再发一次（至少一次）。在 `TypedPublisher` 上调用 `.transactional()`，会把这套接线切换成
每批一个 Broker 事务：运行时开启事务，发布每一条回复，提交，然后才 ack 入站的这一批；任何失败都会中止
事务，所以回复绝不会只露出一半。事务性这项要求在消费接线的地方强制执行：挂载它要求策略的活发布者
实现 `TransactionalPublisher` 能力，因此没有事务的 Broker 依然无法通过编译。单条消息的回复写法仍然
接受普通的 `TypedPublisher` 栈。

## 手动事务

在批量回复这条路径之外，可以手动驱动事务：在事务性接线上调用 `begin()`，会开启一个拥有该事务的
`TransactionScope`。发布都经由该作用域进行，而 `commit()` / `abort()` 会消费它，于是没有 begin 就
commit、第二次 commit、结算之后再发布，都是编译错误，而不是运行时的意外：

```rust
--8<-- "examples/publishing.rs:manual_transaction"
```

该作用域带着与其他各处相同的构建器（`scope.message(&value).publish()`、
`scope.raw(&bytes).to("audit").publish()`），只不过发送的目标是已打开的事务，而不是直接发给 Broker。
它用发布者的编解码器编码值，然后直接发送：按发布者的变换和应用级的 `publish_layer` 中间件属于分发路径
（它们要读取产生回复的那次投递），在这里不会运行。丢弃一个尚未结算的作用域会记录一条警告，并让该句柄
上的 Broker 事务保持打开状态，所以务必显式结算。

该作用域属于借用式的事务：它借用句柄上唯一的 Broker 侧事务，因此每个句柄同一时刻只有一个作用域
处于打开状态。如果某个 Broker 的事务是客户端缓冲区而不是 producer 状态，它还会实现拥有式的那种，即
`OwnedTransactions`：每次调用 `transaction()` 都会开启一个独立的事务，其缓冲区就存放在返回的
`Transaction` 值里，因此同一个句柄上可以并发打开任意多个，结算其中一个也绝不会碰到另一个。`publish`
把内容缓冲进该值，`commit()` / `abort()` 消费它，这与作用域一样是“结算即消费”的纪律；而丢弃一个
这样的事务只是丢掉它的缓冲区（并记录一条警告），不会留下一个打开着的 Broker 事务。像 Kafka 那样客户端
每个 producer 恰好持有一个事务的 Broker，只实现借用式的那种。

拥有式的那种也有类型化的语法糖：在发布者实现了 `OwnedTransactions` 的 `TypedPublisher` 上，
`transaction()` 会开启一个 `TypedTransaction`，它拥有该 Broker 事务，并用发布者的编解码器编码，写作
`let mut txn = typed.transaction().await?;`，然后是 `txn.message(&value).publish().await?;` 和
`txn.commit().await?;`。`.transactional()` 加 `begin()` 给出的是借用式作用域（每个句柄一个），而在同
一个 `TypedPublisher` 上，同一时刻可以打开任意多个 `TypedTransaction`。

## 批量发布

`Publisher` 上没有直接的批量发布 API。对多数 Broker（NATS、Kafka）来说，客户端本来就会把写入合并起来，
因此逐条消息调用 `publish` 的循环能达到同样的吞吐。如果某个 Broker 真的有管线原语（Redis），那就由该
Broker crate 把它作为 Broker 专有的能力暴露出来。
