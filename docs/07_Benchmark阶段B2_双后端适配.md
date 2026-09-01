# RustKV Benchmark 阶段 B2：双后端适配

## 全局约束

1. 唯一性能执行规范是 `/Users/Admin/work/kv/benchmark_plan.md`，SHA-256 必须与 [`04_Benchmark分阶段实现方案.md`](./04_Benchmark分阶段实现方案.md) 记录值一致；RustKV API 只以系统设计文档和现有公共 API 为准。
2. 本阶段只完成语义等价的 Backend 适配，不实现线程调度、吞吐量统计或正式工作量。
3. RustKV 直接调用 Rust API；LevelDB 基础操作直接调用官方 C API，只允许 Batch 和 Iterator 两个 C 聚合函数。
4. 不得修改 RustKV 根 crate，不得把 Fjall 或 RustKV 私有类型暴露给 Benchmark。
5. 只修改【实现文件】；测试结束后等待用户 Review，不得自行提交。

---

【任务】定义一次性的 `BenchBackend` 能力边界，分别实现 RustKV 公共 API 和 LevelDB 1.23 官方 C API 适配，并用真实小数据库证明六种底层调用语义一致。

【读取章节】只读以下章节，其余章节本次不读：

- `benchmark_plan.md` 第2节“测试架构”
- `benchmark_plan.md` 第3节“固定配置”中的数据库配置
- `benchmark_plan.md` 第6节“并发与计时”中共享实例要求
- `benchmark_plan.md` 第7节“正确性要求”
- `系统设计文档_v2.md` 第4.1节“DB”
- `系统设计文档_v2.md` 第4.2节“Options”
- `系统设计文档_v2.md` 第4.3节“WriteBatch”
- `系统设计文档_v2.md` 第4.5.1节“DbIterator”
- `系统设计文档_v2.md` 第7.7节“Get流程”
- `系统设计文档_v2.md` 第7.9节“Iterator和范围查询流程”

【实现文件】

- `docs/04_Benchmark分阶段实现方案.md`（仅更新 B2 状态和验收提交）
- `benchmarks/build.rs`
- `benchmarks/src/lib.rs`
- `benchmarks/src/backend/mod.rs`
- `benchmarks/src/backend/rustkv.rs`
- `benchmarks/src/backend/leveldb.rs`
- `benchmarks/src/backend/leveldb_ffi.rs`
- `benchmarks/native/leveldb_aggregate.h`
- `benchmarks/native/leveldb_aggregate.c`
- `benchmarks/tests/backend_contract.rs`
- `benchmarks/tests/backend_rustkv.rs`
- `benchmarks/tests/backend_leveldb.rs`

【接口契约】

- `BenchBackend: Send + Sync` 的完整能力固定为 `get`、`put`、`delete`、`write_batch`、`iterator_scan`；阶段验收后不得为具体负载另加捷径方法。
- `get` 必须完成数据库读取和 Value 缓冲区访问，返回 `found` 与 `value_length`；NotFound 是成功结果，其他错误是失败。
- `put`/`delete` 每次恰好发出一次对应数据库请求；`sync` 来自固定配置。
- `write_batch` 接受按序排列的 Put 或 Delete 项，构造并提交一次原子 WriteBatch；成功只表示整个 Batch 成功。
- `iterator_scan(ScanRequest)` 每次新建 Iterator，执行 lower-bound Seek，依次访问 Key 与完整 Value，验证 Key 严格递增、首项不小于 start、Value 长度正确，返回记录数和 Value 总字节数，最后销毁 Iterator。`ScanRequest` 的正式计时模式只验证边界、顺序和长度；计时外全量验证模式还必须逐条验证期望编号及完整 Value 字节，但不得增加另一个 Backend 方法或 C 聚合函数。
- RustKV Backend 只能使用 `Options`、`Db::open/get/put/delete/write/iter`、`ReadOptions`、`WriteOptions`、`WriteBatch` 和 `DbIterator` 公共接口；范围查询必须调用 `Db::iter()`，禁止调用 `Db::range()`。
- RustKV 与 LevelDB 均映射固定的 write buffer、block cache、block size、restart interval、max open files、max table file size、关闭压缩和 `sync=false`。
- LevelDB Open、Close、Get、Put、Delete 由 Rust 直接声明和调用官方 `leveldb_*` C API，不增加这些操作的 C 包装函数。
- 只允许以下两个额外 C 符号：
  - `bench_leveldb_write_batch(...)`：创建 Batch、按输入顺序加入 Put/Delete、调用一次 `leveldb_write()`、销毁 Batch；
  - `bench_leveldb_iterator_scan(...)`：创建 Iterator、Seek、连续访问限定条数的 Key/Value、验证状态、销毁 Iterator。
- C 函数使用显式长度，不调用 `strlen`；所有 `char **errptr`、Get 返回缓冲区、Options、ReadOptions、WriteOptions、Iterator、Batch 和 Cache 必须按官方 API 成对释放。
- 每次运行只打开一个数据库对象并由工作线程共享；Batch 和 Iterator 只属于当前调用线程。Rust 对 LevelDB 裸指针的 `Send/Sync` 声明必须封装在唯一拥有者中，并写明官方线程安全前提；Close 只能在全部工作线程退出后发生。
- Backend 错误必须保留后端、操作和原始错误文本；禁止把错误转换为成功或 NotFound。

【禁止事项】

- 禁止使用 LevelDB C++ `DB` 接口、第三方 Rust LevelDB crate或新增 C++ 适配层。
- 禁止出现第三个 `bench_leveldb_*` 聚合符号。
- 禁止在 Backend 内缓存查询结果、合并请求、重试、生成随机 Key或创建额外线程。
- 禁止用 RustKV `range()` 或一次性收集整个库来实现 Range。
- 禁止把 Batch 拆成 100 次单条写，或把 Iterator Scan 拆成多次 Get。
- 禁止在计时调用路径计算 Value checksum；本阶段测试可在计时外逐字节验证。
- 禁止泄漏 LevelDB 分配的错误字符串和 Get 缓冲区。

【测试要求】

- 共同契约测试必须对真实 RustKV 和真实 LevelDB 临时数据库运行同一向量，覆盖创建、关闭重开、空 Value、1 KiB Value、覆盖写、删除存在 Key、删除不存在 Key和 Get NotFound。
- 对两个 Backend 执行混合 Batch，验证操作顺序、覆盖/删除终态和原子一次提交语义；Put Batch、Delete Batch 各覆盖 100 条测试向量。
- Iterator 测试覆盖空库、首 Key、精确 Seek、落在两 Key 之间、尾部越界、limit 0/1/100、严格递增和 Value 总字节数。
- 测试逐字节对比双方逻辑终态；不能只比较条数。
- 注入非法路径或只读/布局错误，验证错误传播且运行被判失败。
- LevelDB 专项测试核对版本 `1.23`，循环 Get/NotFound 后无错误指针残留，并通过链接符号检查证明额外聚合符号恰好两个。
- RustKV 专项测试从源码静态检查或受控接口装配证明 Range 路径使用 `Db::iter()`，不存在 `Db::range()` 调用。

【验收】

```bash
cd /Users/Admin/work/kv/rustkv/benchmarks
cargo fmt --check
cargo build --locked
cargo test --locked --test backend_contract --test backend_rustkv --test backend_leveldb
cargo test --locked
cargo build --release --locked

cd /Users/Admin/work/kv/rustkv
cargo build --locked
cargo test --locked
git diff -- benchmarks docs
```

全部通过且差异只包含本阶段文件。等待用户 Review 后，才允许提交 `benchmark stage B2: 双后端适配`。

【输出】

1. 改动文件清单。
2. Backend 完整接口、配置映射、FFI 所有权和两个 C 函数说明。
3. 双后端真实数据库契约测试结果。
4. 内存/错误释放与符号边界检查结果。
5. 明确写出“等待用户 Review，尚未提交”。
