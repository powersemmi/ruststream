# 事务性 outbox { #transactional-outbox }

发布事件和写入它所描述的那一行是两个操作。进程在两者之间崩溃，系统状态就会不一致：订单已记录却没有
事件，或者事件已发出而订单回滚了。outbox 消除这种不一致：事件成为写入的一部分，稍后再发布到 Broker。

该模式不限于 HTTP。凡是发布必须与数据库写入保持一致的地方，都需要它。下面的示例在 axum 端点上演示，
这也是最常见的场景。完整可编译的源码在
[`examples/http_outbox.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/http_outbox.rs)：

```text
cargo run --example http_outbox --features macros,memory,json
```

## 把事件与业务写入记录在一起 { #recording-the-event-beside-the-write }

端点不在请求路径上发布，而是把事件与业务写入记录在一起。记录下来的事件随后由一个中继送到 Broker：

=== "宏"

    ```rust
    --8<-- "examples/http_outbox.rs:event"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/http_outbox.rs:event"
    ```

```rust
--8<-- "examples/http_outbox.rs:store"
```

端点只写存储。记录订单和把它的事件入队是同一个原子步骤。请求路径上没有 Broker I/O。因此 Broker
不可用既不会拖慢响应，也不会让响应变成错误：

```rust
--8<-- "examples/http_outbox.rs:endpoint"
```

## 把 outbox 转移到 Broker { #draining-the-outbox }

中继在后台运行，把 outbox 里的事件发布到 Broker。只有发布成功，它才删除对应的行。因此 Broker
不可用只会延迟事件，不会丢失事件。如果进程在发布与删除之间崩溃，重启后中继会重新发布这一行。
消费方得到的是至少一次投递，这是 outbox 一贯的契约。它们处理重复消息的方式，与处理来自 Broker
本身的重复投递完全相同：

```rust
--8<-- "examples/http_outbox.rs:relay"
```

换成真正的数据库，`Store` 就是一张业务表和一张 `outbox` 表，两者在同一个 SQL 事务里写入。中继按
插入顺序读取 `outbox` 的行，发布，然后删除。其余部分不变：Broker、发布者和订阅者都不知道 outbox
的存在。
