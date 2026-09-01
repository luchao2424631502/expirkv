# RustKV

RustKV 是一个用 Rust 实现的单进程嵌入式有序 Key-Value 数据库原型。索引由 Fjall 提供，Value 存放在 RustKV 自己管理的 Value Log 中；公共 API 不暴露 Fjall 类型。

唯一实现规范是仓库根目录的 [`系统设计文档_v2.md`](../系统设计文档_v2.md)。

## 当前状态与边界

当前代码已完成基础骨架、60 分基础功能以及阶段15、16、18的 P0 功能，已经具备：

- 创建、打开、独占根目录锁和自动 Open Recovery；
- `Put`、`Get`、`Delete` 和原子 `WriteBatch`；
- `sync=false`、`sync=true` 和空 `sync=true` 前缀持久化屏障；
- 固定视图的 `Snapshot`、双向 `DbIterator` 和流式 `RangeCursor`；
- `Db::stats()`、完整生命周期、后台 Descriptor cleanup 和 `Db::destroy`；
- 多线程共享同一 `Db`、并发读写、固定 Snapshot 视图和提交历史线性化验收；
- 正常 Drop、未执行 Drop 的进程退出以及 SIGKILL 后重开恢复测试。

本项目目前用于正常环境下的原型功能和性能验证，还不是生产就绪数据库：

- 阶段17“完整错误状态机和故障注入矩阵”暂缓，不能把现有故障测试等同于完整生产故障认证；
- 不实现 Value Log GC，覆盖、删除和历史 Snapshot 留下的不可达记录会继续占用磁盘；
- 只提供原子 `WriteBatch` 和一致性 Snapshot，不提供通用事务、跨进程共享实例或网络服务；
- SIGKILL 测试不等同于掉电、内核崩溃或完整存储栈持久化认证。

## 环境与依赖

- Rust edition：2024
- 最低 Rust 版本：1.90
- 当前 crate 尚未发布到 crates.io，请使用本地路径依赖：

```toml
[dependencies]
rustkv = { path = "/absolute/path/to/kv/rustkv" }
```

在 RustKV 仓库中验证：

```bash
cd /absolute/path/to/kv/rustkv
cargo build --locked
cargo test --locked
```

## 快速开始：完整可运行示例

下面的程序会创建或打开数据库、写入单条记录、提交一个原子批次、读取数据并执行显式持久化屏障：

```rust
use std::path::PathBuf;

use rustkv::{Db, Options, ReadOptions, WriteBatch, WriteOptions};

fn main() -> rustkv::Result<()> {
    let path = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("./rustkv-data"));

    let options = Options {
        create_if_missing: true,
        ..Options::default()
    };
    let db = Db::open(&options, &path)?;

    // sync=false：成功返回后立即对普通读取可见，但不承诺已经持久化。
    db.put(&WriteOptions::default(), b"name", b"RustKV")?;

    if let Some(value) = db.get(&ReadOptions::default(), b"name")? {
        println!("name={}", String::from_utf8_lossy(&value));
    }

    // 一个非空 WriteBatch 是一个原子事务，读取者只能看到批次前或完整批次后。
    let mut batch = WriteBatch::new();
    batch.put(b"user:1", b"Alice")?;
    batch.put(b"user:2", b"Bob")?;
    batch.delete(b"name")?;
    db.write(&WriteOptions::default(), &batch)?;

    assert_eq!(
        db.get(&ReadOptions::default(), b"user:1")?,
        Some(b"Alice".to_vec())
    );
    assert_eq!(db.get(&ReadOptions::default(), b"name")?, None);

    // 空 sync=true Batch 不创建新事务；它把此前成功写入的前缀推进到持久化前沿。
    db.write(&WriteOptions { sync: true }, &WriteBatch::new())?;

    let stats = db.stats();
    println!(
        "state={:?}, head_seq={}, durable_seq={}, lag={}",
        stats.instance_state,
        stats.head_seq,
        stats.durable_seq,
        stats.durability_lag
    );

    Ok(())
}
```

将代码放入依赖 RustKV 的 Cargo 项目 `src/main.rs` 后运行：

```bash
cargo run --release -- /absolute/path/to/database
```

## 数据库创建与打开

`Db::open` 借用 `Options`，不会消费它。数据库路径是 RustKV 管理目录，而不是单个文件。

```rust
use rustkv::{Db, Options};

let create = Options {
    create_if_missing: true,
    ..Options::default()
};
let db = Db::open(&create, "./data/example")?;
```

`create_if_missing` 和 `error_if_exists` 的主要行为如下：

| 数据库状态 | `create_if_missing` | `error_if_exists` | 结果 |
|---|---:|---:|---|
| 不存在 | `false` | 任意 | `NotFound` |
| 不存在 | `true` | 任意 | 初始化并打开 |
| 合法数据库已存在 | 任意 | `true` | `InvalidArgument` |
| 合法数据库已存在 | 任意 | `false` | 验证、必要时恢复，然后打开 |
| 无法解释的受管残留 | 任意 | 任意 | `InvalidLayout` 或 `Corruption` |

同一数据库目录在一个时刻只能存在一个打开的 RustKV 实例。需要多个句柄或多个线程访问时，应克隆已经打开的 `Db`，不要再次调用 `Db::open`：

```rust
let another_handle = db.clone();
```

`Db` 实现 `Clone + Send + Sync`。所有 clone 共享同一个运行时状态、写闸门、索引、Value Log 和根目录锁。

### Options 默认值

```text
create_if_missing                 = false
error_if_exists                   = false
write_buffer_size                 = 4 MiB
max_open_files                    = 1000
block_cache_size                  = 8 MiB
block_size                        = 4 KiB
block_restart_interval            = 16
max_file_size                     = 2 MiB
compression                       = Compression::NoCompression
vlog_read_handle_cache_capacity   = 64
```

其中 `write_buffer_size`、`max_open_files`、`block_cache_size`、`block_size`、`block_restart_interval`、`max_file_size` 和 `compression` 配置索引。`vlog_read_handle_cache_capacity` 单独控制 Value Log 只读文件句柄缓存；设为 `0` 表示不保留缓存句柄。

Value Log 的 64 KiB 页面、单文件 4 GiB 上限和 60000 字节 Key+Value 上限是固定磁盘格式，不能通过 `Options` 修改。

## Key 和 Value 规则

- Key 和 Value 都是任意二进制字节，不要求 UTF-8；
- Key 不能为空，长度必须在 `1..=60000` 字节；
- Value 可以为空；
- `key.len() + value.len()` 必须不超过 60000；
- Key 按字节序排列；
- `Get` 不存在的 Key 返回 `Ok(None)`；
- 空 Value 返回 `Ok(Some(Vec::new()))`，与不存在不同；
- 删除不存在的 Key 返回 `Ok(())`。

```rust
use rustkv::{ReadOptions, WriteOptions};

let write = WriteOptions::default();
db.put(&write, &[0x00, 0xff, 0x80], &[])?;
assert_eq!(
    db.get(&ReadOptions::default(), &[0x00, 0xff, 0x80])?,
    Some(Vec::new())
);

db.delete(&write, b"missing-key")?;
```

## 单条写入与持久化

```rust
use rustkv::{ReadOptions, WriteOptions};

db.put(&WriteOptions::default(), b"key", b"value")?;
assert_eq!(
    db.get(&ReadOptions::default(), b"key")?,
    Some(b"value".to_vec())
);

db.delete(&WriteOptions { sync: true }, b"key")?;
assert_eq!(db.get(&ReadOptions::default(), b"key")?, None);
```

`WriteOptions::sync` 的语义：

| 配置 | 成功返回时的保证 |
|---|---|
| `sync=false` | 事务已经原子发布，普通读取立即可见；不保证此时已经到达持久化前沿 |
| `sync=true` | 当前事务以及提交全序中位于它之前的成功事务前缀已经持久化 |

Drop 不会隐式执行同步屏障，也不会把 `sync=false` 升级为 `sync=true`。如果业务需要在关闭前明确持久化此前的异步写，应主动执行空同步批次：

```rust
use rustkv::{WriteBatch, WriteOptions};

db.write(&WriteOptions { sync: true }, &WriteBatch::new())?;
```

空 `sync=false` Batch 是无 I/O 的成功操作；空 `sync=true` Batch 不分配事务序号，但会在统一写队列中的位置持久化此前成功前缀。

## 原子 WriteBatch

`WriteBatch::put` 和 `WriteBatch::delete` 本身是可失败操作，因此构造 Batch 时也要使用 `?` 或显式处理错误。

```rust
use rustkv::{WriteBatch, WriteOptions};

let mut batch = WriteBatch::new();
batch.put(b"account:1", b"new-state")?;
batch.delete(b"account:2")?;
batch.put(b"account:3", b"created")?;

assert_eq!(batch.len(), 3);
assert!(!batch.is_empty());

db.write(&WriteOptions { sync: true }, &batch)?;

// Db::write 只借用 Batch。Batch 不会被消费或修改，可以再次提交或清空复用。
db.write(&WriteOptions::default(), &batch)?;
batch.clear();
assert!(batch.is_empty());
```

一个非空 Batch 的全部操作按加入顺序执行，但只作为一个事务发布：

- 读取者只能观察 Batch 前或完整 Batch 后状态，不能观察中间结果；
- 同一 Key 可以在一个 Batch 中重复出现，最终状态等价于依次执行全部操作；
- 任一成员预检失败时，整个 Batch 为 `NotCommitted`，不会产生部分 Value Log 或索引效果；
- 批量插入或删除应使用一个 `WriteBatch`，循环调用单条 API 不具备批次原子性。

## Snapshot 一致性读取

`Snapshot` 是创建时的固定读取视图。它没有独立的 `get`、`iter` 或 `range` 方法，而是通过 `ReadOptions` 传入这些 Db API。

```rust
use rustkv::{ReadOptions, WriteOptions};

db.put(&WriteOptions::default(), b"versioned", b"old")?;
let snapshot = db.snapshot()?;

db.put(&WriteOptions::default(), b"versioned", b"new")?;

let current = db.get(&ReadOptions::default(), b"versioned")?;
let fixed = db.get(
    &ReadOptions {
        snapshot: Some(&snapshot),
    },
    b"versioned",
)?;

assert_eq!(current, Some(b"new".to_vec()));
assert_eq!(fixed, Some(b"old".to_vec()));
```

`Snapshot` 实现 `Clone + Send + Sync`，并持有数据库资源和根目录锁。Snapshot 可以在创建它的某个 `Db` 变量被释放后继续使用，但仍需通过同一数据库实例的其他 `Db` clone 发起读取。其他数据库创建的 Snapshot 会被拒绝为 `InvalidArgument`。

## DbIterator

普通 Iterator 创建时取得隐式固定 Snapshot；传入显式 Snapshot 时使用该固定视图。它不会随着后续写入改变。

```rust
use rustkv::ReadOptions;

let mut cursor = db.iter(&ReadOptions::default())?;
cursor.seek_to_first();

while cursor.valid() {
    let key = cursor.key().expect("valid cursor has a key");
    let value = cursor.value().expect("valid cursor has a value");
    println!("{:?} => {:?}", key, value);
    cursor.next();
}

if let Err(error) = cursor.status() {
    eprintln!(
        "iterator failed: kind={:?}, state={:?}, retry={:?}",
        error.kind, error.instance_state, error.retry_advice
    );
}
```

定位和移动方法：

- `seek_to_first()`：定位到最小 Key；
- `seek_to_last()`：定位到最大 Key；
- `seek(target)`：定位到第一个 `key >= target` 的条目，`seek(&[])` 合法；
- `next()` / `prev()`：向后或向前移动；
- `valid()`：只有 Key 和 Value 都已完整读取并校验时才为 `true`；
- `status()`：正常未定位或遍历结束返回 `Ok(())`，读取错误返回保存的首个错误。

新建 Iterator 处于 `Unpositioned`。正常越界后处于 `Exhausted`，可再次 seek；读取或校验错误后进入终止 `Failed` 状态，不能通过重新定位恢复。

调用任何定位或移动方法后，之前从 `key()` 或 `value()` 获得的借用不再可用。`DbIterator` 不实现 `Sync`；不要让多个线程同时推进同一个游标，不同 Iterator 可以并发使用。

## Range 范围查询

Range 是流式、只向前的 `[start, end)` 查询，创建成功时已经定位到第一项，不会一次性构造全部结果。

```rust
use rustkv::{KeyRange, ReadOptions};

let mut range = db.range(
    &ReadOptions::default(),
    KeyRange {
        start: Some(b"user:"),
        end: Some(b"user;"),
    },
    100,
)?;

while range.valid() {
    println!(
        "{:?} => {:?}",
        range.key().expect("valid range has a key"),
        range.value().expect("valid range has a value")
    );
    range.next();
}

if let Err(error) = range.status() {
    eprintln!("range failed: {error:?}");
}
```

- `None` 表示对应方向无界；
- `Some(&[])` 是合法的最小边界，不等于无界；
- `start >= end` 或 `limit == 0` 返回正常的空 Cursor；
- 最多返回 `limit` 项；
- Range 在整个生命周期使用创建时的隐式或显式 Snapshot；
- `RangeCursor` 只提供 `next()`，不提供反向移动。

使用显式 Snapshot 查询范围：

```rust
use rustkv::{KeyRange, ReadOptions};

let snapshot = db.snapshot()?;
let reads = ReadOptions {
    snapshot: Some(&snapshot),
};
let range = db.range(
    &reads,
    KeyRange {
        start: None,
        end: None,
    },
    1000,
)?;
```

## 多线程使用

同一 `Db` 可以直接 clone 后移动到不同 OS 线程，不要使用覆盖整个 Db 的 `Mutex<Db>`：

```rust
use std::thread;

use rustkv::{ReadOptions, WriteOptions};

let writer = db.clone();
let write_thread = thread::spawn(move || {
    writer.put(&WriteOptions::default(), b"thread:key", b"value")
});

write_thread.join().expect("writer thread panicked")?;

let reader = db.clone();
let read_thread = thread::spawn(move || {
    reader.get(&ReadOptions::default(), b"thread:key")
});

assert_eq!(
    read_thread.join().expect("reader thread panicked")?,
    Some(b"value".to_vec())
);
```

并发语义：

- 所有 Put、Delete、WriteBatch 和同步屏障经过同一 WriteGate，形成单一提交全序；
- 每个线程内的写顺序和不重叠调用的实时顺序得到保留；
- 重叠写可以按任一合法顺序线性化，但每个 Batch 始终全有或全无；
- 写成功返回之后才开始的普通读，会看到该提交或更晚状态；
- Get、Snapshot、Range 和不同 Cursor 不进入写闸门，可与写入并发；
- 显式 Snapshot 和已创建的隐式 Iterator/Range 视图在并发写期间保持固定；
- 单个可变 Cursor 不支持被多个线程无同步地共同推进。

## 运行状态与 Stats

`Db::stats()` 返回不会失败的内存快照，不执行磁盘 I/O、同步、恢复、Flush 或 Compaction。

```rust
let stats = db.stats();
assert_eq!(stats.durability_lag, stats.head_seq - stats.durable_seq);

println!("instance_state={:?}", stats.instance_state);
println!("state_epoch={}", stats.state_epoch);
println!("head_seq={}", stats.head_seq);
println!("durable_seq={}", stats.durable_seq);
println!("vlog_files={}", stats.vlog_file_count);
println!("vlog_logical_bytes={}", stats.vlog_logical_bytes);
```

主要字段：

| 字段 | 含义 |
|---|---|
| `instance_state` | 当前 `Healthy`、`WriteStopped` 或 `Poisoned` 状态 |
| `state_epoch` | 状态升级计数，只单调增加 |
| `first_latched_error` | 首个使实例离开 Healthy 的结构化错误摘要 |
| `head_seq` | 已原子发布的最新非空事务序号 |
| `durable_seq` | 已持久化事务前缀的末端序号 |
| `durability_lag` | 精确等于 `head_seq - durable_seq` |
| `durable_vlog_end` | 已持久化 Value Log 末端，仅用于诊断 |
| `active_vlog_file_id` | 当前可追加的 Value Log 文件编号 |
| `vlog_file_count` | 受管 Value Log 文件数量 |
| `vlog_logical_bytes` | 受管 Value Log 文件逻辑大小总和 |

实例状态决定可用能力：

| 状态 | 写入 | 读取 | 其他 |
|---|---|---|---|
| `Healthy` | 接受 | 接受 | `stats()`、Drop |
| `WriteStopped` | 拒绝新写 | Get、Snapshot、Iterator、Range 继续完整校验 | `stats()`、Drop |
| `Poisoned` | 拒绝 | 普通读取不再保证成功 | `stats()`、Drop |

同一个实例不会从 `WriteStopped` 或 `Poisoned` 恢复为 `Healthy`。应根据结构化错误处理环境或数据问题，释放旧实例及其 Snapshot/Cursor，再重新打开。

## 结构化错误与安全重试

所有可失败 API 返回 `rustkv::Result<T>`，错误是 `StorageError`。不要只解析 `message`，应优先检查稳定的结构化字段：

```rust
use rustkv::{
    InstanceState, RetryAdvice, StorageError, WriteOutcome,
};

fn may_retry_live_write(error: &StorageError) -> bool {
    error.write_outcome == Some(WriteOutcome::NotCommitted)
        && error.instance_state == Some(InstanceState::Healthy)
        && matches!(
            error.retry_advice,
            RetryAdvice::FixRequestAndRetrySameInstance
                | RetryAdvice::RetrySameInstance
        )
}
```

常用字段包括：

- `kind`：错误原因分类；
- `operation`：Put、Get、WriteBatch、Open、Destroy 等操作；
- `protocol_stage`：Admission、Preflight、VLogAppend、IndexCommit、Recovery 等阶段；
- `write_outcome`：失败写请求的 `NotCommitted` 或 `CommitUnknown`；
- `instance_state`：返回时活实例状态；
- `retry_advice`：调用方后续动作；
- `os_code`、`commit_seq`、`vlog_file_id`、`vlog_offset`：可选诊断信息；
- `destroy_failure`：只用于 Destroy 失败的对象、阶段和部分删除状态。

重试规则：

- 只有 `NotCommitted + Healthy` 且建议为 `FixRequestAndRetrySameInstance` 或 `RetrySameInstance` 时，活实例写请求才适合在同一实例重试；
- `NotCommitted + WriteStopped`：修复环境，释放旧实例及其所有外部读取对象，重新 Open 后再决定是否提交；
- `CommitUnknown + Poisoned`：不能盲目重放，必须重新 Open，并通过应用自己的幂等标识或业务状态验证整个请求存在或不存在；
- `Corruption`、`InvalidLayout` 和 `IncompatibleFormat` 需要按 `retry_advice` 修复、恢复或人工检查；
- Batch 构造期错误没有绑定活实例，`instance_state` 为 `None`；失败的 `put/delete` 不会改变 Batch 已有内容。

RustKV 的公开错误文本和 source chain 不应包含原始 User Key 或 User Value，但应用日志仍应避免自行打印敏感输入。

## Drop、重开与恢复

RustKV 没有显式 `close()`：

- Drop 一个 `Db` clone 只释放该句柄；
- Snapshot、DbIterator 和 RangeCursor 也持有数据库资源及根目录锁；
- 最后一个外部对象释放后停止接纳新操作；
- 已经 started 的写运行到协议可判定结果，尚未 started 的排队写以 `NotCommitted` 撤销；
- Drop 不执行隐式持久化屏障，也不会清除锁存错误。

打开已有数据库时，`Db::open` 会先验证 FORMAT、数据库身份、索引与 Value Log 拓扑，然后执行必要 Recovery；Open 完成前不会发布半可用 Db。

如果需要在同一进程中重新打开同一路径，必须先释放所有相关对象：

```rust
drop(range_cursor);
drop(iterator);
drop(snapshot);
drop(db);

let reopened = Db::open(&Options::default(), "./data/example")?;
```

## 销毁数据库

`Db::destroy` 是独立的关联函数，不依赖已打开实例：

```rust
use rustkv::{Db, Options};

// 必须先释放所有 Db clone、Snapshot、Iterator 和 RangeCursor。
drop(db);
Db::destroy("./data/example", &Options::default())?;
```

销毁行为：

- 数据库仍被当前进程或其他进程打开时返回 `Busy`；
- 路径不存在时返回成功；
- 删除前严格验证受管对象和数据库身份；
- 不执行 Recovery 或隐式 Repair；
- 只删除确认属于该数据库的 FORMAT、索引目录和 Value Log 文件；
- 保留数据库根目录、`LOCK` 和普通非受管文件；
- 失败不承诺回滚，可通过 `destroy_failure.partially_deleted` 判断是否已经发生部分删除，并在修复原因后重试。

## API 速查

| API | 用途 | 读取视图/原子性 |
|---|---|---|
| `Db::open` | 创建或打开并恢复数据库 | 成功后才发布 Healthy 实例 |
| `Db::put` | 写入或覆盖一个 Key | 单事务原子发布 |
| `Db::get` | 读取一个 Key | 普通视图或显式 Snapshot |
| `Db::delete` | 删除一个 Key | 单事务；不存在也成功 |
| `Db::write` | 提交 WriteBatch 或同步屏障 | 非空 Batch 全有或全无 |
| `Db::snapshot` | 创建固定读取视图 | `Clone + Send + Sync` |
| `Db::iter` | 双向有序遍历 | 创建时固定隐式/显式 Snapshot |
| `Db::range` | `[start, end)` 流式范围读取 | 创建时固定隐式/显式 Snapshot |
| `Db::stats` | 获取运行时内存快照 | 不失败、无磁盘 I/O |
| `Db::destroy` | 验证并删除受管数据库对象 | 独立操作，不运行 Recovery |

## 测试

普通构建和全量测试：

```bash
cargo build --locked
cargo test --locked
```

阶段18并发正确性和历史检查：

```bash
cargo test --locked \
  --test concurrency_reads \
  --test concurrency_writes \
  --test concurrency_snapshots \
  --test concurrency_failures \
  --test concurrency_history
```

仓库还包含显式忽略的 10 GiB 重型多 Value Log 测试。它们会消耗大量时间和磁盘空间，只应在准备好的测试环境中单独运行：

```bash
cargo test --locked --test core_multivlog_10g -- --ignored --nocapture
cargo test --locked --test lifecycle_multivlog_10g -- --ignored --nocapture
```

进行性能测试时应使用 Release 构建、独立数据库目录和明确的 `sync` 策略，并分别记录吞吐、延迟、`head_seq/durable_seq`、Value Log 文件数量和逻辑字节数；不要把 `sync=false` 与 `sync=true` 的结果直接混为一组。
