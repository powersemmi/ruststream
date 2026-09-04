# 失败策略

在处理器逻辑运行之前，有两件事可能出错：处理器函数体可能 **panic**，传入的载荷可能**解码**失败。
RustStream 用同一套词汇结算这两者，都通过 `on_failure(..)` 子句按订阅者设置。但两者的默认值不同，
因为这两种失败的含义并不一样。

## 默认值

不写该子句时，订阅者采用内置的默认值：

- **panic = `fail_fast`**：panic 属于内部缺陷。运行时会打出一条醒目的错误日志并点名是哪个订阅，然后
  开始优雅关闭（取消关闭令牌并执行关闭钩子），并让 [`run`](../index.md) 以非零状态码返回 `Err`。
  于是编排器会重启服务，运维人员也能在日志里看到问题。
- **decode = `drop`**：解码失败通常意味着外部输入有问题。丢掉这一条坏消息（不重新入队的 nack）可以
  避免一份畸形的载荷把消费者拖垮，而在不可信的主题上，那正是毒消息或拒绝服务的隐患。同一个策略也覆盖
  解析失败的[带类型消息头契约](headers.md)：消息头与载荷同属外部输入这一类，所以一个 `decode` 键就把
  两者都结算了；它同样覆盖[自己完成反序列化](subscribers.md#raw-subscribers)的载荷类型
  （`#[derive(Deserialized)]`）在自己的构造里拒绝这些字节的情况：一个坏掉的 flatbuffers 根和一次
  失败的 JSON 解析属于同一类坏输入，因此由同一个键结算。

=== "宏"

    ```rust
    --8<-- "examples/failure_policy.rs:defaults"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/failure_policy.rs:defaults"
    ```

## 设置策略

`on_failure(panic = .., decode = ..)` 可以覆盖其中任意一个键（两个都是可选的；省略的键保持自己的默认
值）：

=== "宏"

    ```rust
    --8<-- "examples/failure_policy.rs:tuned"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/failure_policy.rs:tuned"
    ```

各个策略取值如下：

| 取值                  | 效果                                                                  |
|-----------------------|-----------------------------------------------------------------------|
| `fail_fast`           | 记录日志，开始优雅关闭，并让 `run` 返回 `Err`。                        |
| `drop`                | 丢弃这条消息（不重新入队的 `nack`）。                                  |
| `retry`               | 把消息重新入队（带重新入队的 `nack`）。                                |
| `retry_after(<dur>)`  | 延迟一段时间后重新入队（参见[订阅者](subscribers.md)中关于延迟重新投递的一节）。 |
| `skip`                | 对失败的消息做 ack 以便越过它。这不算成功：消息就此消失，未经处理。    |

`skip` 是特意留出的毒消息逃生口：它越过一条无法处理的消息，而不是把它丢弃或反复重试。给解码失败选
`retry` 时要谨慎：除非 Broker 有死信或最大投递次数策略，一份永远解不出来的载荷会无限重新投递下去。

=== "宏"

    ```rust
    --8<-- "examples/failure_policy.rs:skip"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/failure_policy.rs:skip"
    ```

## 具体行为

- 运行时会捕获 panic（`catch_unwind`），所以一个 panic 的处理器绝不会拖死分发循环。在 `fail_fast` 下，
  消息保持未结算状态，因此具备重新投递能力的 Broker 会在服务重启之后把它交回来；在其他策略下，运行时会
  结算消息，订阅者继续消费。捕获只在展开式 panic 配置下有效；用 `panic = "abort"` 时进程早已不复存在。
- 解码失败以 `Result` 的形式浮现，不涉及任何展开；`decode` 策略直接结算这条消息（参见
  [编解码器](codecs.md#decode-failures)）。
- 在批量路径上，该策略作用于每个批次的解码（其中每个元素独立解码）以及批量处理器中的 panic。不存在按
  元素的 panic 处理。

完整示例在这里：[`examples/failure_policy.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/failure_policy.rs)。
