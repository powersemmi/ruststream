# 失败策略

处理器给不出结果的原因有两个：它的函数体 **panic**，或者传入的载荷无法**解码**。
同一组策略取值覆盖这两种情况。你可以用 `on_failure(..)` 子句按订阅者分别设定取值。
panic 和解码的默认值不同，因为这两种失败的含义不一样。

## 默认值

不写该子句时，订阅者用内置的默认值：

- **panic = `fail_fast`**：panic 是代码里的缺陷。运行时记录一条错误日志，并写上订阅的名字，然后开始
  优雅关闭：取消关闭令牌，执行关闭钩子。[`run`](../index.md) 返回 `Err`，退出码非零，编排器据此重启
  服务。
- **decode = `drop`**：解码失败通常意味着外部数据有问题。运行时丢掉这一条消息（不重新入队的 nack），
  服务继续运行：在不可信的主题上，停下服务就是一次拒绝服务。[类型化消息头契约](headers.md)解析不出来
  时，同一个键结算这次投递。载荷类型[自己完成反序列化](subscribers.md#raw-subscribers)
  （`#[derive(Deserialized)]`）并在构造函数里拒绝这些字节时，也由该键结算。

=== "宏"

    ```rust
    --8<-- "examples/failure_policy.rs:defaults"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/failure_policy.rs:defaults"
    ```

## 设置策略

`on_failure(panic = .., decode = ..)` 子句给两个键中的任意一个设定取值。你没有写的键保持自己的默认
值：

=== "宏"

    ```rust
    --8<-- "examples/failure_policy.rs:tuned"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/failure_policy.rs:tuned"
    ```

策略的取值如下：

| 取值                  | 效果                                                                  |
|-----------------------|-----------------------------------------------------------------------|
| `fail_fast`           | 记录错误日志，开始优雅关闭，`run` 返回 `Err`。                         |
| `drop`                | 丢弃这条消息（不重新入队的 `nack`）。                                  |
| `retry`               | 把消息重新入队（带重新入队的 `nack`）。                                |
| `retry_after(<dur>)`  | 延迟之后重新入队（见[订阅者](subscribers.md)中讲延迟重新投递的一节）。 |
| `skip`                | 对失败的消息做 ack，越过它。这不是成功：消息未经处理就丢失了。         |

给解码失败选 `retry` 要谨慎：永远解不出来的载荷会无限重新投递，除非 Broker 有死信或最大投递次数策略。
`skip` 是毒消息场景下有意留出的逃生口。

=== "宏"

    ```rust
    --8<-- "examples/failure_policy.rs:skip"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/failure_policy.rs:skip"
    ```

## 具体行为

- `catch_unwind` 捕获 panic，所以 panic 的处理器不会停掉分发循环。在 `fail_fast` 下，这次投递保持
  未结算，支持重新投递的 Broker 会在重启之后把消息再交一次。在其余取值下，运行时结算这次投递，订阅者
  继续消费。捕获只在栈展开时有效：用 `panic = "abort"` 构建时，进程已经没了。
- 解码返回 `Result`，不会 panic，所以这里没有栈展开。`decode` 键直接结算这条消息（见
  [编解码器](codecs.md#decode-failures)）。
- 在批量路径上，每个元素独立解码，`decode` 键分别作用于每一个。
  `panic` 键作用于批量处理器里的 panic。没有按单个元素的 panic 捕获。

完整示例：[`examples/failure_policy.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/failure_policy.rs)。
