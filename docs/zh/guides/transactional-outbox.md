# 事务性 outbox { #transactional-outbox }

发布事件和写入它所描述的那一行是两个操作，中间崩溃就会让系统前后不一致：订单已记录却没有事件，或者
事件已发出而订单回滚了。outbox 把这个缝隙合上：事件成为写入的一部分，之后再搬到 Broker 上。

这个模式并不专属于 HTTP，凡是发布必须与数据库写入保持一致的地方都用得上。下面的示例从 axum 端点驱动
它，这也是最常见的场景；完整可编译的源码在
[`examples/http_outbox.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/http_outbox.rs)：

```text
cargo run --example http_outbox --features macros,memory,json
```

## 事件与写入记录在一起 { #recording-the-event-beside-the-write }

端点不再在请求路径上发布，而是把事件与业务写入放在一起原子地记录下来。随后由一个中继把记录下来的
事件搬到 Broker 上：

```rust
--8<-- "examples/http_outbox.rs:event"
```

```rust
--8<-- "examples/http_outbox.rs:store"
```

端点只写存储。记录订单和把它的事件入队是同一个原子步骤，没有任何 Broker I/O 能让响应失败或卡住：

```rust
--8<-- "examples/http_outbox.rs:endpoint"
```

## 把 outbox 排空到 Broker { #draining-the-outbox }

一个后台任务把 outbox 排空到 Broker 里。只有在某一行的发布成功之后，中继才会删除这一行，因此 Broker
故障只是延迟事件，而不会丢失事件；如果在发布与删除之间崩溃，重启后中继会重新发布这一行。因此消费方
看到的是至少一次投递，这也是 outbox 一贯的契约：它们处理重复消息的方式，与处理来自 Broker 本身的
重复投递完全相同：

```rust
--8<-- "examples/http_outbox.rs:relay"
```

换成真正的数据库时，`Store` 就是一张业务表加一张 `outbox` 表，二者在同一个 SQL 事务里写入；中继按
插入顺序读取 `outbox` 的行，发布，然后删除它们。其余一切都与示例中一样：Broker、发布者和订阅者都
不知道 outbox 的存在。
