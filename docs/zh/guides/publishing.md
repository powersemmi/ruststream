# 发布与回复

处理器有两种发布方式。返回回复的写法更短。处理器按一个已声明的目的地作答时，它就是默认的正确写法。
目的地在运行时决定，或者一条消息同时发往多个目的地（包括不同的 Broker）时，用 `Out` 参数把发布者
取进处理器。两种写法都不会让处理器见到尚未连接的发布者：注册时你指定发布*策略*，策略在启动时于
已连接的 Broker 上实例化发布者。

显式发布始终用同一个构建器：从 `message(..)` 开始，以 `publish()` 收尾。发布的是一个已声明类型的值，
传输方式由这个类型自己选定：

```text
message(&order)   -> codec -> bytes -> broker    （Serialize 的值需要编码）
message(&export)  ->          bytes -> broker    （Serialized 的值自己产出字节）
```

服务已经编码好的字节，通过派生 `Serialized` 的 newtype 发布。名字把它们收录进生成的文档。消息类型
声明哪些位置留给调用点：目的地、类型化的消息头和编解码器。信息不全的发布无法通过编译。

## 从处理器回复

回复是服务发出的消息，所以回复类型要 derive `Outgoing`。返回回复值，并在订阅者上写 `publish`；
目的地由类型上的 `#[outgoing(name = "..")]` 声明：

=== "宏"

    ```rust
    use ruststream::Outgoing;

    --8<-- "examples/publishing.rs:reply_declared"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/publishing.rs:reply_declared"
    ```

没有声明名字的回复类型，则从挂载点取名字：属性上写 `publish("responses")`，链上写
`.to("responses")`。类型自己固定了名字时，挂载点的名字只是默认值，不会生效。

=== "宏"

    ```rust
    use ruststream::subscriber;

    --8<-- "examples/publishing.rs:reply"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/publishing.rs:reply"
    ```

用普通的 `include` 挂载这样的处理器。不作其他指定时，回复用默认编解码器编码，经由 Broker 的默认
发布策略发出。`.out(Reply, Publish)` 指定 Broker 的 `prelude` 导出的策略，链上其后的步骤把回复的
接线补齐：`.codec(..)` 指定回复的编解码器，`.transform(..)` 加上静态发布变换，`.transactional()`
让一个批次的回复共用同一个 Broker 事务。编解码器只指定一次，第二个 `.codec(..)` 是编译错误。变换
会叠加，因此 `.transform(..)` 可以调用多次。

处理器的定义不指定发布者，它声明处理器回复什么、发往哪里。发布策略属于 Broker，因此在点名 Broker
的地方指定它，也就是挂载点，用绑定 `Out` 槽位的那个 `.out(marker, policy)` 调用。`Reply` 是一个位置
标记，处理器返回的值经由这个位置发布。

=== "宏"

    ```rust
    --8<-- "examples/publishing.rs:reply_mount"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/publishing.rs:reply_mount"
    ```

入站请求用作用域的编解码器解码，作用域编解码器由 `with_broker_codec` 设定，没有设定就用默认编解码器。
回复的编解码器由这条链搭好的接线决定。参见[编解码器](codecs.md#the-publish-side)。

同一个 `publish(..)` 服务于两种传输方式，选择由回复的类型决定。实现 `serde::Serialize` 的回复按上面
的方式编码。带 `#[derive(Serialized)]` 的回复自己产出字节、按字节原样发出，因此它的
`.out(Reply, ..)` 只接一个策略：这条路径上没有编解码器可指定，其后写 `.codec(..)` 无法通过编译。
参见[原始字节订阅者](subscribers.md#raw-subscribers)。

## 控制确认行为

普通的回复写法总是先发布再 ack。想自己掌控，就返回 `Result<Reply, HandlerOutcome>`。`Ok(reply)` 发布
并 ack，`Err(outcome)` 什么都不发布，分发器按返回的 `HandlerOutcome` 行事（`HandlerOutcome::drop()`
表示进死信，`HandlerOutcome::retry()` 表示请求重新投递）：

=== "宏"

    ```rust
    --8<-- "examples/publishing.rs:reply_result"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/publishing.rs:reply_result"
    ```

`Result` 这种写法由写出来的签名识别，因此把它显式写出来：藏起 `Result` 的类型别名算作普通的
回复类型。和任何处理器一样，会发布的处理器也可以声明可选的第二参数 `&mut Context`，用来读取应用
状态或者手动发布。

如果回复的发布本身返回了错误（Broker 拒绝或连接断开），入站消息收到 `requeue = true` 的 nack，
Broker 重新投递它。让会发布的处理器对重新投递保持幂等。

## 在处理器内部发布 { #publishing-from-inside-a-handler }

目的地在运行时决定，或者一条消息同时发往多个目的地（包括不同的 Broker）时，用 `Out` 把发布者作为
处理器参数取进来。`Out(out): Out<impl Publisher>` 这种模式把 `out` 绑定成函数体内一个活的发布者。
签名只写出处理器需要的那项能力，具体的发布者类型由挂载点指定的策略推断出来。同一个处理器原封不动，
既能挂到生产 Broker 上，也能挂到它的进程内测试传输上。

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

`message(&value)` 按值的类型选定的那种传输方式发布。`serde::Serialize` 的值用作用域的编解码器编码，
单次调用可以用 `.with_codec(..)` 换一个。`Serialized` 的值按字节原样发出，没有编解码器的位置。两种
方式都有消息头这一位置：`.with_headers(..)` 按引用接收消息声明的契约（`&meta`），或者按值接收一份
已经建好的 `HeaderMap`。发布以 `publish()` 收尾。

挂载点指明来源。对作用域自己的 Broker 来说，来源就是它的发布策略：

=== "宏"

    ```rust
    --8<-- "examples/publishing.rs:forward_mount"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/publishing.rs:forward_mount"
    ```

没有绑定的 `Out` 槽位是编译错误：每个槽位都有策略之前，这次注册构建不出来。

### 具名槽位 { #named-slots }

需要多个发布者的处理器，为每个参数指定一个**槽位标记**：一个派生了 `OutSlot` 的空结构体，写在第二个
类型参数的位置（`Out<impl Publisher, Primary>`）。挂载点用 `.out(marker, policy)` 绑定每一个标记，再用
收尾的 `.build()` 提交这次注册。这些调用按标记绑定，因此先后顺序无关紧要。把同一个槽位绑定两次，
或者指定一个处理器没有声明的标记，都是编译错误。`.build()` 只在每个槽位都绑定之后才存在，因此漏掉
一次绑定同样无法通过编译，类型里直接点名了那个槽位（`MissingSlot<Audit>`）。唯一的无名参数
`Out<impl Publisher>` 绑定到隐含的 `DefaultSlot`（`.out(DefaultSlot, Publish).build()`）。

`.out(..)` 之后的 `.transform(..)` 作用在这次 `.out(..)` 点名的位置上（见[发布管线](#the-publish-pipeline)），
因此既回复又分发副本的注册，在每个位置上各指定一次变换：
`.out(Reply, Publish).transform(StampSource).out(Audit, Publish).transform(Envelope)`。

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

约束里的能力可以收窄：`Out<impl OwnedTransactions, Ledger>` 只有在策略的活发布者支持拥有式事务时
才能编译。约束在挂载点检查，编译错误点名缺失的那项能力。约束里写的是 Broker 的能力
trait（`Publisher`、`TransactionalPublisher`、`OwnedTransactions`、`RequestReply`，或者 Broker crate
自己定义的那一个），不是任何 Broker 类型，因此函数体与 Broker 无关。手动路径上，同一个约束写在条目
的活值上：`where L: OutEntry<Ledger, Wire: OwnedTransactions>`。在每一个这样的约束之下，条目在挂载点
的编解码器和标记的列表之上，给出该能力的类型化形态：发布构建器、事务作用域和拥有式事务。槽位标记
也是[测试套件](testing.md#asserting-on-out-slots)记录发布时所用的名字。

`Out` 参数可选的第三个位置声明该处理器发布什么：`Out<impl Publisher, Marker, (A, B)>`，可以是单个
类型，也可以是带 `#[derive(OutMessages)]` 的枚举集合。标记自身的 `#[publishes(A, B)]` 列表规定该槽位
允许发布什么；处理器不限制第三个位置时，生成的文档报告的就是这份列表。类型化地发布标记没有列出的
类型是编译错误。什么都不列的标记什么都发布不了：每次发布都有一个消息类型，因此这份列表管辖所有
发布。单个无名 `Out<impl Publisher>` 的隐含 `DefaultSlot` 没有可以列类型的声明处，因此它接受每一种
已声明的消息。参见[类型化的消息头](headers.md)。

带 `#[derive(Serialized)]` 的类型按字节原样发进槽位，路径上没有编解码器。其余一切遵循平常的规则：
给它 `#[derive(Outgoing)]`，再像任何模型一样列进 `#[publishes(..)]`。文档用它自己的名字收录它，
不带载荷 schema，因为这里的格式就是字节本身。目的地取自类型的声明，而词典、声明的消息集合和消息头
位置对它的把关，与对编码模型完全一致。

=== "宏"

    ```rust
    --8<-- "tests/lanes.rs:serialized_out"
    ```

=== "手写"

    ```rust
    --8<-- "tests/manual_out_slots.rs:serialized_out"
    ```

### 声明消息发往何处 { #declaring-where-a-message-goes }

消息类型用一次派生声明自身发送方式的全部信息，参数都写成 `key = value` 形式。`name` 是目的地，
`headers` 指明消息头契约的类型；契约本身仍然是普通的 serde 结构体，派生不碰它：

=== "宏"

    ```rust
    use ruststream::Outgoing;

    --8<-- "examples/publishing.rs:declared"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/publishing.rs:declared"
    ```

声明决定调用点留有哪一种目的地位置：

- **固定的名字**指定了目的地，因此没有 `to(..)` 可写：这种类型只发往声明里的地址。
- **名字模板**（`"orders.{tenant}.placed"`）开放 `to()`，它返回一个构建器，每个占位符对应一个
  setter。每个占位符都绑定之后 `publish()` 才能编译；未绑定的占位符留在构建器的类型里，因此
  编译错误说明地址还没写完，并点名遗漏的那一段。地址每次发布重新拼装，固定的名字则从一个
  `&'static str` 取用。
- **不写 `name`** 表示由调用点指定名字：`.to("orders.archived")` 接受 `&str` 或者算出来的 `String`。

声明了 `headers = Meta` 的消息只能通过 `.with_headers(&meta)` 发布：忘了写它或者传了别的类型都无法
通过编译。在生成的文档里，固定的名字成为消息的 channel，模板成为带参数的地址，参数块由占位符填充。
不声明目的地的类型进不了文档。

正是这次派生让一个值能经由构建器发布，第三种情形也不例外。别的 crate 拥有的 `Serialize` 类型无法
派生 `Outgoing`：可以把它包进一个派生了 `Outgoing` 的 newtype，或者在事务内部用作用域的
`publish(name, &value)` 发布。

=== "宏"

    ```rust
    --8<-- "examples/publishing.rs:declared_mount"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/publishing.rs:declared_mount"
    ```

`Out` 参数可以和任何一种订阅者写法组合：与 `Ctx` 提取器并列、用在自己完成反序列化的处理器上、也用在
批量处理器上（`b.include(f).out(marker, policy).build()`，进来的是一整个批次，出去的是逐元素的目的地）。
在回复写法上，也就是 `publish(..)` 和它的批量对应形式，回复是同一条链上的又一个位置。因此一个网关
同时指定两个位置：它按固定的目的地作答，并通过注入的发布者分发副本：

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

处理器从一个 Broker 消费而发布到另一个时（从 Kafka 消费，转发到 Redis），先用 `.bindable()` 包住
目标 Broker，并在注册之前生成一个**绑定令牌**。随后令牌就是挂载点上的来源。任意一对 Broker 的
写法都一样，这里用两个内存 Broker 演示：

=== "宏"

    ```rust
    --8<-- "tests/out_injection.rs:cross_broker"
    ```

=== "手写"

    ```rust
    --8<-- "tests/manual_out_injection.rs:cross_broker"
    ```

令牌在任何 `with_broker` 运行之前就已经存在，因此注册顺序无关紧要：双向的桥预先绑好两个方向。

令牌与生成它的 `Bindable` 包装器共享同一个槽位，因此要注册同一个包装器（`with_broker(bindable, ..)`），
启动才会把已连接的 Broker 填进该槽位。令牌的 Broker 始终没有注册时，绑定就返回清晰的错误。同一套
形态也用于回复发布（在 `publish("dest")` 处理器上写 `.out(Reply, token)`）和批量写法。在注册之外，
启动连接了令牌的 Broker 之后，令牌自行完成绑定：`running.publisher(token)` 把活的发布者交给同级的
任务，参见[与其他服务器并行运行](http.md)。启动时的第一次发布根本不需要令牌：作用域级别的
`b.after_startup(policy, hook)` 在订阅打开之后，用一个已经绑好的发布者运行该钩子（参见
[应用生命周期](lifespan.md#lifecycle-hooks)）；发布示例里的预填数据也在它上面完成。

## 消息头从哪里来 { #where-the-headers-come-from }

一次发布的消息头来自两处。调用点用 `.with_headers(..)` 指定消息头：按引用传入消息声明的契约，
或者按值传入一份已经建好的 `HeaderMap`。发布者可以再加上自己的一份基础消息头。一系列消息共用同
一个参数（租户、分区提示、Broker 用消息头表达的投递选项）时，发布者从 `base_headers` 交出这个参数。
从这个发布者开启的事务同样如此。

构建器只组装一次出站的消息头：先写入基础消息头，再把调用点的消息头逐个键覆盖上去。每个键的取值
按下面的顺序确定：

- **调用点**指名的键，取调用点的值；
- 调用点没有碰过的键，取**发布者**基础消息头里的值；
- 没有基础消息头的发布者，让调用点的消息头保持原样。

两种写法的合并方式相同：`HeaderMap` 逐条覆盖写入，声明的 `headers = Meta` 契约把自己的字段逐个
序列化到基础消息头之上。因此带契约的消息同样得到发布者的那个参数。

`.with_headers(..)` 只能填一次：第二次调用是编译期错误。

回复按同一套顺序装配，尽管回复上没有 `.with_headers(..)`。挂载点为 `Reply` 位置指定了策略，
策略构造的发布者先写入自己的基础消息头，同一条链上的 `.transform(..)` 步骤再覆盖上去。因此，用
消息头表达的 Broker 选项会出现在三个地方：`publish("dest")` 处理器的回复、批次里的每一条回复和
函数体经由 `Out` 槽位发出的消息。处理器对它一无所知。

## 发布管线 { #the-publish-pipeline }

消息离开进程之前，有四类变换运行，而且它们可以组合：

- **回复接线上的静态 `PublishTransform`**，在 `.out(Reply, ..)` 之后用 `.transform(..)` 添加。这是
  零成本、按目的地生效的变换：一层信封、一个固定的 content type，或者把这次投递的链路追踪 /
  关联 id 写进回复。它们改写消息头和负载，不动目的地。
- **某一个 `Out` 槽位上的静态 `OutTransform`**，在 `.out(marker, policy)` 之后用 `.transform(..)`
  添加。它改写从该槽位出去的消息的消息头和负载：一层 outbox 信封、一个固定的 content type 和一个
  租户标记。它不接受 `PublishContext`。槽位上的发布由处理器函数体自己发出，因此那次投递也由函数体
  自己读取、自己写进消息。
- **回复接线上的静态 `RedirectTransform`**，在 `.out(Reply, ..)` 之后用 `.redirect(..)` 添加；槽位
  上与之对应的是 `.out(marker, policy)` 之后的 `OutRedirect`。它是唯一指定消息目的地的变换，而且
  逐条消息指定。
- **应用上的静态 `PublishLayer`**，用 `.publish_layer(..)` 添加。这是横切关注点（发布指标、死信
  包装），作用于每一条发布出去的消息。它包在发送外面，因此能观察到发送的结果。整条链会组合成一个
  具体类型，于是它成为应用类型的一部分：构建器通常返回 `impl App`，从不把它写出来，而具体的
  `RustStream<L, St, PublishStack<MyMiddleware, PublishIdentity>>` 把管线直接写在类型里；没有
  `publish_layer` 的应用保持默认的 `PublishIdentity`。每个中间件都必须是 `Clone` 的（管线会克隆
  进每一个会发布的处理器），最后添加的中间件在最外层运行。默认情况没有中间件，就是直接发送。中间件
  的组合要到运行时才决定时，可以把它包进 `PublishDynStack`（`DynStack` 在发布侧的对应物）再添加。

静态的 `PublishTransform` 实现 `apply(&mut Outgoing<'_>, &PublishContext<'_, C>)`。`PublishContext`
只读地给出产生这条回复的那次投递：它的 channel、入站消息头和 Broker 的单条投递类型化上下文。该
上下文按 `Field` 键读取。因此变换可以把入站消息里的值转移到回复上：

```rust
--8<-- "examples/publishing.rs:static_transform"
```

批量处理器的回复不经过按消息生效的 `.transform(..)` 栈。用 `.batch_transform(..)` 给它们添加变换，
按单条消息写成的 `PublishTransform` 可以用 `for_batch(transform)` 复用。

回复逐条经过变换，但它们共用同一个 `PublishContext`，而该上下文属于整个批次，不属于某
一条投递。一个批次跨越许多条投递，因此 `name()` 是订阅，`headers()` 是空的，`context(..)` 读到的
是 Broker 的批次上下文。要读入站消息的变换，应当留在按消息的那条路上：在那里，一条回复和它的投递是
同一件事。

`OutTransform` 实现 `apply(&mut Outgoing<'_>)`，只作用在一个槽位上：

```rust
--8<-- "examples/publishing.rs:slot_transform"
```

### 按消息指定目的地 { #naming-a-destination-per-message }

消息发往何处是声明出来的：在消息类型上用 `#[outgoing(name = "..")]`，在挂载点用
`publish("dest")`，或者在槽位的调用点用 `.to(..)`。有些回复没有目的地可声明。AMQP 请求把作答用的
队列放在 `reply-to` 消息头里，ZeroMQ 的 `ROUTER` 则把每条回复发给提问的那一方。

`RedirectTransform` 读取这次投递，指定回复的目的地：

```rust
--8<-- "examples/publishing.rs:redirect"
```

在链上用 `.redirect(..)` 指定它：

```rust
--8<-- "examples/publishing.rs:redirect_mount"
```

`.redirect(..)` 适用于把目的地留空的回复类型。写了 `#[outgoing(name = "receipts")]` 的类型上，它
是一个点名该回复类型的编译错误。挂载点的 `publish("answers")` 仍然是这条回复已声明的目的地：生成
的文档报告这个名字，重定向没有改名字时，回复也发往这里。

批的回复无法重定向：它们以整个批的名义发布，而一个批作答许多条投递，不带其中任何一条的消息头。一个
位置只接受一次重定向，因此在它上面写第二个 `.redirect(..)` 无法通过编译。

`Out` 槽位接受同一个步骤，用的是 `OutRedirect`。它和 `OutTransform` 的签名一样，都是
`apply(&mut Outgoing<'_>)`，作用是给每条从该槽位出去的消息指定目的地。

被重定向的槽位，其 `#[publishes(..)]` 列表里的每个类型都必须把目的地留空。函数体每次发布仍然要写
`.to(..)`，重定向再改写这个名字。没有列表的标记接受任何已声明的消息，因此根本无法重定向，单个匿名
`Out<impl Publisher>` 的隐式 `DefaultSlot` 也在其内。

这样的槽位只提供普通发送：事务和一次 request / reply 往返都绕过槽位的发布路径直达 Broker，因此
索要其中任何一项的处理器无法通过编译。

`PublishLayer` 实现 around/next 形式的签名，因此它可以中断这条链、重试发送，或者只做观察：

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

应用级的这一层包住处理器发出的每一次发布：`publish(..)` 写法的回复和从注入的 `Out` 槽位出去的
每一条消息。

挂载点上的变换作用在指定它的那个位置上：`.out(Reply, Publish).transform(StampSource)` 扩充回复的
栈，`.out(Audit, Publish).transform(OutboxEnvelope)` 扩充这个槽位的栈。两个位置都用到时，注册就
把两个调用都写上，而 `.transform(..)` 归属它前面点名的那个位置。一个位置按固定顺序运行它的步骤，
无论链上把它们写成什么次序：先是重定向，然后是这个位置的变换，然后是应用级的中间件，最后是发送。

有两种发布不经过这条管线，都由处理器函数体自己驱动：在槽位上开启的事务（`begin()`、`transaction()`）
发往 Broker 的事务，而一次 request / reply 往返（`request(..)`）等待回复，不以一次发送收尾。

路由器的槽位只带自己的变换，不带应用级的那条链：`include_router` 挂载的路由，其类型在应用成型之
前就已定型。槽位发布需要经过这条链时，把处理器用 `b.include(..)` 挂在 Broker 作用域上。完整的程序见
[`examples/publishing.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/publishing.rs)。

## 批量回复与事务

`#[subscriber("in", publish("out"))]` 处理器接受 `&[T]` 时，消费整个解码后的批次，并返回这个批次
的回复，也就是 consume-transform-produce 模式。`Ok(replies)` 把每一条回复发布到已声明的目的地，
并 ack 整个批次；`Err(outcome)` 什么都不发布，并用 `outcome` 结算整个批次（全有或全无：逐元素
挑选结果与事务无法组合）：

=== "宏"

    ```rust
    --8<-- "examples/publishing.rs:batch_publishing"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/publishing.rs:batch_publishing"
    ```

用 `include` 挂载它，并在链上用 `.out(Reply, ..)` 添加回复的接线：

=== "宏"

    ```rust
    --8<-- "examples/publishing.rs:batch_publishing_mount"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/publishing.rs:batch_publishing_mount"
    ```

不写 `.transactional()` 时，每条回复各自独立发布。批次中途失败会让整个批次重新投递，因此先前那些
回复可能再发一次（至少一次）。

`.out(Reply, ..)` 之后的 `.transactional()` 步骤把这套接线切换成每个批次一个 Broker 事务：运行
时开启事务，发布每一条回复，提交，然后才 ack 入站的这个批次。任何失败都会中止事务，因此回复绝不
会只露出一半。

这样的接线只能用活发布者是事务性的策略来挂载，因此没有事务的 Broker 无法通过编译。单条消息的回复
没有一个批次可做成原子的，因此 `.transactional()` 只存在于批量的写法上。

## 手动事务

在批量回复这条路径之外，事务由你手动驱动。在任何事务性发布者上调用 `begin()`，得到拥有该事务的
`TransactionScope`。发布都经由该作用域进行，`commit()` / `abort()` 消费它。因此没有开始事务就
提交、第二次提交和结算之后再发布，都是编译错误，而不是运行时的意外：

```rust
--8<-- "examples/publishing.rs:manual_transaction"
```

该作用域用的构建器与其他表面相同（`scope.message(&value).publish()`），只是发送到已打开的事务，
而不是直接发给 Broker。它用发布者的编解码器编码值，然后直接发送：按发布者的变换和应用级的
`publish_layer` 中间件属于分发路径，它们要读取产生回复的那次投递，在这里不会运行。丢弃一个尚未
结算的作用域会记录一条警告，并让该句柄上的 Broker 事务保持打开，因此要显式结算。

`Out` 槽位打开的是同一种作用域。用 `Out<impl TransactionalPublisher, Journal>` 约束该槽位（手动
路径上写成 `where W: TransactionalPublisher`），在条目上调用 `begin()` 就得到这个作用域，驱动方式
与上面完全一样。

在槽位上打开的作用域，接纳的类型和该槽位自己的 `message` 一样：标记的列表，再按参数声明的集合
收窄。因此事务不可能发布生成的文档从未声明过的消息，而它的发布在测试套件里记在该槽位名下。

该作用域是借用式的事务：它借用句柄上唯一的 Broker 侧事务，因此每个句柄同一时刻只有一个作用域
处于打开状态。

如果某个 Broker 的事务是客户端缓冲区而不是 producer 状态，它还会实现拥有式的那种，即
`OwnedTransactions`。每次调用都开启一个独立的事务，它的缓冲区就存放在返回的 `TypedTransaction`
里，因此同一个句柄上可以同时打开任意多个，结算其中一个也绝不会影响另一个。
`message(..).publish()` 把内容缓冲进该值，`commit()` / `abort()` 消费它，这与作用域一样遵循“结算即
消费”的规则。丢弃这样一个事务只是丢掉它的缓冲区（并记录一条警告），不会留下一个打开着的 Broker
事务。Kafka 这类 Broker 的客户端每个 producer 恰好持有一个事务，因此它们只实现借用式的那种。

拥有式事务在两种表面上的写法一样：把事务缓冲在客户端的发布者提供 `owned_transaction()`，带
`Out<impl OwnedTransactions, Ledger>` 约束的槽位提供 `transaction()`。两者都开启一个
`TypedTransaction`，它拥有该 Broker 事务，并用该表面的编解码器编码，写作
`let mut txn = publisher.owned_transaction().await?;`，然后是 `txn.message(&value).publish().await?;`
和 `txn.commit().await?;`。`begin()` 给出的借用式作用域每个句柄只有一个，而同一个发布者上可以同时
打开任意多个 `TypedTransaction`。拥有式事务的缓冲区在槽位之外结算，因此它的发布会落在 Broker 的
发布日志里，而不是槽位自己的记录里。

## 批量发布

发布很多条消息时，在循环里逐条发布。多数 Broker（NATS、Kafka）的客户端本来就会合并写入，因此这个
循环能达到和专门的批量调用一样的吞吐。某个 Broker 有真正的管线原语（Redis）时，它的 crate 把这个
原语作为 Broker 专有的能力提供出来。
