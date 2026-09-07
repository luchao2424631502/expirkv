# RustKV Benchmark 阶段 B0：工程骨架与构建基线

## 全局约束

1. 唯一性能执行规范是 `/Users/Admin/work/kv/benchmark_plan.md`，SHA-256 必须与 [`04_Benchmark分阶段实现方案.md`](./04_Benchmark分阶段实现方案.md) 记录值一致。
2. RustKV API 语义只以 `/Users/Admin/work/kv/系统设计文档_v2.md` 为准，SHA-256 必须为 `e5cbc3517f20874bd83bb13bd694b9f4ee74b37863f16fd6927dea22287ea21e`。
3. 所有代码均放在 `/Users/Admin/work/kv/rustkv/benchmarks/`；它是 RustKV Git 仓库内的独立 Rust 子 crate，不建立嵌套 Git 仓库。
4. 不得修改 RustKV 根 crate 的 `src/`、`tests/`、`Cargo.toml` 或 `Cargo.lock`。
5. 本阶段只搭建可编译、可链接、可测试的骨架，不实现任何性能负载。
6. 只修改【实现文件】列出的文件；需要扩大范围时停止并报告。
7. 测试结束后必须等待用户 Review，无论成功或失败均不得自行提交。

---

【任务】建立 `kv_bench` 子 crate、固定 LevelDB 1.23 获取与 Release 构建方式，跑通 RustKV 路径依赖、LevelDB 官方 C API 链接及最小 CLI。

【读取章节】只读以下章节，其余章节本次不读：

- `benchmark_plan.md` 第2节“测试架构”
- `benchmark_plan.md` 第8节“执行环境与结果”
- `系统设计文档_v2.md` 第9.5节“实现安全与平台I/O边界”
- `系统设计文档_v2.md` 第10.8节“性能Benchmark”中的构建隔离与 Release 构建要求

【实现文件】

- `docs/04_Benchmark分阶段实现方案.md`（仅更新 B0 状态和验收提交）
- `benchmarks/Cargo.toml`
- `benchmarks/Cargo.lock`
- `benchmarks/build.rs`
- `benchmarks/.gitignore`
- `benchmarks/README.md`
- `benchmarks/src/lib.rs`
- `benchmarks/src/main.rs`
- `benchmarks/src/backend/mod.rs`
- `benchmarks/src/backend/leveldb_ffi.rs`
- `benchmarks/scripts/bootstrap_leveldb.sh`
- `benchmarks/tests/build_smoke.rs`

【接口契约】

- Cargo package 名称和二进制名称固定为 `kv_bench`，edition 为 `2024`，Rust 版本不得低于 RustKV 根 crate 的 `1.90`。
- 使用 `rustkv = { path = ".." }` 路径依赖；不得把 Benchmark 加入 RustKV 根 crate 的依赖或 workspace 配置。
- `bootstrap_leveldb.sh` 只获取 Google LevelDB 官方 `1.23`/commit `99b3c03`，以 Release、关闭压缩库的方式构建到 `benchmarks/.deps/`；重复执行必须可安全复用或重建明确目标。
- `.deps/`、临时数据库和运行结果缓存不得进入 Git；LevelDB 源码和构建产物不是 RustKV 项目源码。
- `build.rs` 默认只从 `benchmarks/.deps/leveldb-install` 查找头文件和库，校验 LevelDB major/minor 为 `1.23`，并在 macOS 正确链接 `libleveldb` 和 C++ 运行库。
- `leveldb_ffi.rs` 本阶段只声明并安全调用官方版本查询函数；不得提前增加数据库操作包装或 C 聚合函数。
- `kv_bench --help` 和 `kv_bench --version` 必须成功；其他命令返回明确的未支持退出码，不得伪装执行完成。
- `Cargo.lock` 必须纳入版本控制，后续一律使用 `--locked`。

【禁止事项】

- 禁止建立 `benchmarks/.git` 或把 Benchmark 做成独立仓库。
- 禁止使用 Homebrew 当前版本代替固定 LevelDB 1.23。
- 禁止使用第三方 LevelDB 高层 Rust crate、YCSB、`db_bench`、Criterion 或 Google Benchmark。
- 禁止提交 LevelDB 源码、静态库、动态库、数据库目录或测试结果缓存。
- 禁止在本阶段定义虚假的 Backend、Trace、统计或负载成功路径。
- 禁止用跳过版本检查、忽略链接错误或硬编码开发者私有绝对库路径取得构建成功。

【测试要求】

- `bootstrap_leveldb.sh` 首次执行能构建固定版本，第二次执行行为确定且不会切换版本。
- 单元/集成测试调用 LevelDB 官方版本函数并严格断言 `1.23`。
- 编译期证明 `rustkv::Db`、`Options`、`ReadOptions`、`WriteOptions`、`WriteBatch` 和 `DbIterator` 可从路径依赖访问，不增加 RustKV 公共 API。
- CLI 测试验证 `--help`、`--version` 的退出码和关键字段；未知参数必须非零退出。
- 验证 Debug、Release 均能链接同一固定 LevelDB 安装目录。
- 在 RustKV 根目录执行全量回归，证明根 crate 未受影响。

【验收】

```bash
cd /Users/Admin/work/kv/rustkv/benchmarks
./scripts/bootstrap_leveldb.sh
cargo fmt --check
cargo build --locked
cargo test --locked --test build_smoke
cargo test --locked
cargo build --release --locked
cargo run --locked -- --help
cargo run --locked -- --version

cd /Users/Admin/work/kv/rustkv
cargo build --locked
cargo test --locked
git diff -- benchmarks docs
```

上述命令全部退出码为 0；`git diff` 只包含本阶段允许文件。等待用户 Review 后，才允许提交 `benchmark stage B0: 工程骨架与构建基线`。

【输出】

1. 改动文件清单。
2. 子 crate、LevelDB 来源/版本校验和链接方式说明。
3. 所有测试命令、退出码及通过/失败数量。
4. `git diff` 范围说明。
5. 明确写出“等待用户 Review，尚未提交”；失败时报告原因并按规范修复、重跑。

# RustKV Benchmark 阶段 B1：固定数据与 Trace

## 全局约束

1. 唯一性能执行规范是 `/Users/Admin/work/kv/benchmark_plan.md`，SHA-256 必须与 [`04_Benchmark分阶段实现方案.md`](./04_Benchmark分阶段实现方案.md) 记录值一致。
2. 只能在已验收 B0 骨架内填充固定配置和 Trace，不得实现 Backend 调用、并发计时或负载执行。
3. 正式数据、Key 分布、随机种子和请求集合必须逐字段符合规范，不得为性能或内存自行简化。
4. 测试可使用显式 `test_only` 小配置，但正式配置必须只有一个不可变入口。
5. 只修改【实现文件】；测试结束后等待用户 Review，不得自行提交。

---

【任务】一次性实现正式配置、Key/Value 编码、确定性伪随机生成及六类负载所需的全局 Trace，使两个 Backend 和全部并发度消费同一请求集合。

【读取章节】只读以下章节，其余章节本次不读：

- `benchmark_plan.md` 第3节“固定配置”
- `benchmark_plan.md` 第4节“固定测试矩阵”
- `benchmark_plan.md` 第5节“初始状态”中 Key 集合定义
- `benchmark_plan.md` 第6节“并发与计时”中全局 Trace 切分要求

【实现文件】

- `docs/04_Benchmark分阶段实现方案.md`（仅更新 B1 状态和验收提交）
- `benchmarks/src/lib.rs`
- `benchmarks/src/config.rs`
- `benchmarks/src/key.rs`
- `benchmarks/src/rng.rs`
- `benchmarks/src/trace.rs`
- `benchmarks/tests/config_key.rs`
- `benchmarks/tests/trace.rs`

【接口契约】

- `BenchConfig::formal()` 固定返回：10,000,000 条记录、16-byte Key、1024-byte Value、Range 100、Batch 100、`sync=false`、关闭压缩、种子 `20260720`、重复 5 次、线程数 `[1,10,100,1000]`。
- 正式配置同时固定双方可对应参数：write buffer 4 MiB、block cache 8 MiB、block size 4 KiB、restart interval 16、max open files 1000、max table file size 2 MiB。
- 正式配置字段不得由 CLI、环境变量或配置文件覆盖；小配置构造器只能在测试或显式 smoke 模式使用，并带有不可混淆的模式标记。
- Key 编码严格为 `[8个0字节][u64大端编号]`；只接受正式逻辑编号范围，字节比较顺序必须与编号顺序一致。
- Value 是由固定种子确定的同一份 1024-byte 字节串；Trace 中不得复制一千万份 Value。
- 自实现固定算法的 SplitMix64、无偏有界抽样和 Fisher–Yates 排列；算法及常量一经本阶段验收不得变更。
- `Workload` 仅包含 `random_get`、`range_scan`、`single_put`、`batch_put`、`single_delete`、`batch_delete` 六项。
- `Trace` 保存逻辑编号/请求边界，不保存 Backend 类型或物理指针。
- `random_get` 对 `0..record_count` 有放回均匀抽样；`range_scan` 起点对 `0..=record_count-range_len` 有放回均匀抽样。
- Put/Delete 使用同一编号全集的确定性随机排列，编号不重复；Batch 仅将排列按 100 条分组，不改变全局次序。
- 每个负载和重复编号先生成与线程数无关的全局 Trace；按连续位置均分给 N 线程，前 `total % N` 个线程各多一个请求，拼接各分片必须恢复原 Trace。
- 五次重复的种子只能由全局种子、负载标识和重复编号通过固定公开函数派生；双方 Backend 不得分别派生。

【禁止事项】

- 禁止使用线程本地随机生成正式请求。
- 禁止因线程数不同重新洗牌、重新抽样或改变请求总数。
- 禁止使用系统时间、OS 随机源、HashMap 随机种子或未固定算法的第三方 RNG。
- 禁止把点查/Range 改成无重复抽样，或允许 Put/Delete 重复编号。
- 禁止用小配置生成的数据标记为正式结果。
- 禁止在本阶段访问真实数据库。

【测试要求】

- 断言 `BenchConfig::formal()` 每个固定字段及六类工作量：Get/Put/Delete 各 10,000,000 op，Range 1,000,000 op，Batch 各 100,000 op。
- Key 测试覆盖 0、1、255、256、最大编号、长度、namespace、大端序排序、越界拒绝和反解往返。
- Value 测试断言长度、固定 golden digest/首尾字节及多次调用完全相同。
- RNG 使用固定种子 golden 向量；有界抽样永不越界，并覆盖非二次幂上界和上界 1。
- 排列测试断言固定 golden、小全集不重不漏、相同种子相同、不同重复编号不同。
- 点查测试证明允许重复且全部在合法集合；Range 起点允许重复、上界可确保完整返回 100 条。
- Batch 测试断言每批恰好 100 条、批内/批间不重复、扁平化后等于单条写排列。
- 对六类负载及 1/10/100/1000 线程，断言分片无遗漏无重复消费、拼接恢复全局 Trace、总请求数不变。
- 以小配置保存各负载固定 golden Trace，防止后续算法漂移；测试不得在常规 `cargo test` 中分配正式 10M Trace。

【验收】

```bash
cd /Users/Admin/work/kv/rustkv/benchmarks
cargo fmt --check
cargo build --locked
cargo test --locked --test config_key --test trace
cargo test --locked
cargo build --release --locked

cd /Users/Admin/work/kv/rustkv
cargo build --locked
cargo test --locked
git diff -- benchmarks docs
```

全部通过且差异只包含本阶段文件。等待用户 Review 后，才允许提交 `benchmark stage B1: 固定数据与Trace`。

【输出】

1. 改动文件清单。
2. 正式配置、随机算法、种子派生和 Trace 切分说明。
3. golden 与性质测试结果。
4. Benchmark 和 RustKV 全量验证结果。
5. 明确写出“等待用户 Review，尚未提交”。

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

# RustKV Benchmark 阶段 B3：并发执行与统计

## 全局约束

1. 唯一性能执行规范是 `/Users/Admin/work/kv/benchmark_plan.md`，SHA-256 必须与 [`04_Benchmark分阶段实现方案.md`](./04_Benchmark分阶段实现方案.md) 记录值一致。
2. 本阶段只实现通用 OS 线程执行器与统计；不得写六类数据库负载的业务分派。
3. 固定工作量、Barrier、延迟边界和单位不得由 Backend 改写。
4. 所有统计错误、线程 panic 和 Backend 错误都必须使运行失败。
5. 只修改【实现文件】；测试结束后等待用户 Review，不得自行提交。

---

【任务】实现与 Backend 无关的并发运行器、请求级延迟采集、吞吐量和分位数统计，保证 1/10/100/1000 个 OS 线程消费固定全局 Trace。

【读取章节】只读以下章节，其余章节本次不读：

- `benchmark_plan.md` 第4节“固定测试矩阵”中的 op 计数规则
- `benchmark_plan.md` 第6节“并发与计时”
- `benchmark_plan.md` 第7节“正确性要求”中的失败判定
- `benchmark_plan.md` 第8节“执行环境与结果”中的 CSV 指标字段

【实现文件】

- `docs/04_Benchmark分阶段实现方案.md`（仅更新 B3 状态和验收提交）
- `benchmarks/src/lib.rs`
- `benchmarks/src/metrics.rs`
- `benchmarks/src/runner.rs`
- `benchmarks/tests/metrics.rs`
- `benchmarks/tests/runner.rs`

【接口契约】

- `RunSpec` 明确包含 Backend、负载、线程数、重复编号、全局 Trace、预期 op 数和每 op 记录数；正式线程数只允许 1、10、100、1000。
- `run_concurrent` 使用 `std::thread` 创建恰好 N 个 OS 线程；禁止协程、线程池或异步运行时替代。
- 一个已打开的 `Arc<dyn BenchBackend>` 由所有工作线程共享；每线程只消费 B1 分配的连续 Trace 分片。
- 主线程与全部工作线程使用同一个启动 Barrier；计时起点必须位于线程创建、Trace 分配之后，并与 Barrier 释放建立明确顺序。
- 墙钟结束时间只能在所有工作线程完成固定请求数并 Join 后取得；创建线程、打开数据库、预热和验证不计时。
- 每次 Backend 调用前后用单调时钟记录一个请求延迟；内部保留整数纳秒，输出时换算为 `us/请求`，不得先截断到微秒。
- 每线程独占预分配延迟数组和计数器；热路径禁止全局 mutex、原子直方图或共享日志。
- `ops/s = completed_ops / wall_seconds`。Range/Batch 的 `records/s = completed_records / wall_seconds`；其他负载不伪造辅助 records/s。
- 平均延迟是所有请求延迟的算术平均；P50/P95/P99 对合并后的全部样本按固定 nearest-rank 规则计算，并在文档/代码注释中给出边界定义。
- 一个完整 Get、Iterator Scan、Put、Delete 或 WriteBatch 计 1 op。Range/Batch 的 100 条记录不能计为 100 op。
- Backend 返回错误、完成数不符、延迟样本数不符、线程 panic 或 Join 失败时，`RunResult` 必须标记无效并保留首错及错误数；不得输出可参与汇总的成功指标。

【禁止事项】

- 禁止按“每线程固定工作量”导致总工作量随线程数增加。
- 禁止在工作线程内生成/洗牌 Trace。
- 禁止把 Barrier 等待时间、线程创建时间或验证时间计入请求延迟。
- 禁止抽样延迟、只统计部分线程或把各线程百分位数再平均。
- 禁止用 CPU time 代替墙钟时间。
- 禁止遇错后补发请求以凑数，或忽略 panic/错误继续报告成功。
- 禁止在本阶段访问真实 RustKV/LevelDB；并发正确性使用可控假 Backend。

【测试要求】

- 使用手写延迟数组验证平均值和 P50/P95/P99 golden，覆盖 1、2、100 个样本及非整数微秒。
- 验证零耗时、零请求、NaN/Infinity 不会形成有效成功结果。
- 对 1/10/100/1000 线程和不能整除的请求数，断言实际创建线程数、每个请求恰好消费一次、总 op 数固定、分片拼接与全局 Trace 相同。
- 使用可控假 Backend 记录最大同时在途调用，证明 10 线程确实发生并发；Barrier 前不得发生调用。
- 验证 Range/Batch 一次请求计 1 op 和 100 records，Get/Put/Delete 一次请求计 1 op。
- 注入第 K 次 Backend 错误，断言运行无效、首错保留、错误数非零且不补发请求。
- 注入工作线程 panic，断言主线程不 panic且结果无效。
- 用小工作量验证每个线程的样本数、合并样本数、completed_ops、completed_records 和墙钟吞吐公式。
- 1000 线程测试只做最小 Barrier/分片/Join 冒烟，不访问数据库、不长时间睡眠，避免把机器调度能力误作性能结果。

【验收】

```bash
cd /Users/Admin/work/kv/rustkv/benchmarks
cargo fmt --check
cargo build --locked
cargo test --locked --test metrics --test runner
cargo test --locked
cargo build --release --locked

cd /Users/Admin/work/kv/rustkv
cargo build --locked
cargo test --locked
git diff -- benchmarks docs
```

全部通过且差异只包含本阶段文件。等待用户 Review 后，才允许提交 `benchmark stage B3: 并发执行与统计`。

【输出】

1. 改动文件清单。
2. 线程/Barrier/计时边界和分位数算法说明。
3. 1/10/100/1000 分片与错误注入测试结果。
4. 指标单位和 op/records 计数断言结果。
5. 明确写出“等待用户 Review，尚未提交”。

# RustKV Benchmark 阶段 B4：六类负载

## 全局约束

1. 唯一性能执行规范是 `/Users/Admin/work/kv/benchmark_plan.md`，SHA-256 必须与 [`04_Benchmark分阶段实现方案.md`](./04_Benchmark分阶段实现方案.md) 记录值一致。
2. 本阶段只把 B1 Trace、B2 Backend 和 B3 Runner 组合成六类负载；不得实现模板复制、240 次矩阵或报告生成。
3. 六类负载的请求边界、Key 集合和 op 计数不得根据数据库实现改变。
4. 所有真实数据库测试使用小配置并明确标记 `smoke`，不得产出“正式性能”结论。
5. 只修改【实现文件】；测试结束后等待用户 Review，不得自行提交。

---

【任务】实现点查、Iterator 范围查询、单条/批量插入、单条/批量删除六种执行路径，并对 RustKV 与 LevelDB 运行相同的小规模端到端正确性测试。

【读取章节】只读以下章节，其余章节本次不读：

- `benchmark_plan.md` 第2节“测试架构”中的 Get、Iterator 和 Batch 语义
- `benchmark_plan.md` 第4节“固定测试矩阵”
- `benchmark_plan.md` 第5节“初始状态”
- `benchmark_plan.md` 第6节“并发与计时”
- `benchmark_plan.md` 第7节“正确性要求”

【实现文件】

- `docs/04_Benchmark分阶段实现方案.md`（仅更新 B4 状态和验收提交）
- `benchmarks/src/lib.rs`
- `benchmarks/src/workload.rs`
- `benchmarks/src/runner.rs`
- `benchmarks/tests/workload_unit.rs`
- `benchmarks/tests/workload_e2e.rs`

【接口契约】

- 每个负载只实现一次通用分派，工作线程只能依赖 `BenchBackend`，不得按 RustKV/LevelDB 分叉业务逻辑。
- `random_get`：从 Trace 取得已有编号、编码 Key、调用一次 `get`；必须命中且 `value_length == 1024`，否则本次运行失败。
- `range_scan`：从 Trace 取得起点、编码 Key、调用一次 Iterator Scan，limit 固定 100；必须返回恰好 100 条、Key 严格递增、边界正确、每条 Value 长度 1024。一整个 Scan 计 1 op、100 records。
- `single_put`：对全新空库中不存在的编号调用一次 `put`，Value 为固定 1 KiB；每个编号只出现一次。
- `batch_put`：把排列中连续 100 个编号按原顺序组成一次全 Put 原子 Batch；一整个 Batch 计 1 op、100 records。
- `single_delete`：对模板中存在的编号调用一次 `delete`；每个编号只出现一次。
- `batch_delete`：把排列中连续 100 个编号按原顺序组成一次全 Delete 原子 Batch；一整个 Batch 计 1 op、100 records。
- Put/Delete 的线程分片来自请求级全局 Trace；Batch 不得被跨线程拆分，单条请求不得被合并。
- 六类负载均使用固定总工作量；改变线程数只能改变 Trace 分片，不能改变操作集合或 op 数。
- `WorkloadRun` 明确携带 `formal` 或 `smoke` 模式；只有 formal 配置、正式总工作量和完整验证均满足时才有资格写正式 CSV。

【禁止事项】

- 禁止将范围查询实现为 `Db::range()`、多次 Get 或预先收集数据库全部内容。
- 禁止 Range/Batch 把 100 条记录报告为 100 ops。
- 禁止为写冲突制造重复 Key、覆盖写或重复删除。
- 禁止 Batch 失败后逐条重试，或 Get 未命中后换 Key 重试。
- 禁止在计时路径生成随机数、格式化日志、计算 checksum或执行最终全量验证。
- 禁止为两个 Backend 使用不同 Value、Trace、请求顺序或小配置。

【测试要求】

- 使用记录调用的假 Backend 逐项断言六类负载发出的 Key、Value、顺序、方法、调用次数、op 数和 records 数。
- 对 1 和 10 线程分别验证相同全局 Trace；扁平化实际调用集合后与期望完全一致。
- 点查注入 NotFound、错误 Value 长度和 Backend 错误，必须使运行失败。
- Range 注入 99 条、乱序、起点前 Key、错误 Value 长度和 Backend 错误，必须使运行失败。
- Batch 测试断言恰好 100 条、全 Put/全 Delete、只提交一次；注入原子提交错误后不能产生成功 op。
- 对真实 RustKV 和 LevelDB 分别用同一小配置执行六类负载：读取库预装数据；插入使用空库；删除使用满库。
- 真实端到端测试至少覆盖 1、10 线程；完成后在计时外逐 Key/逐 Value 验证插入终态、删除空终态、读取结果和 Range 结果。
- 对双方 `RunResult` 只断言正确性、计数和单位，不断言机器相关吞吐量大小。

【验收】

```bash
cd /Users/Admin/work/kv/rustkv/benchmarks
cargo fmt --check
cargo build --locked
cargo test --locked --test workload_unit --test workload_e2e
cargo test --locked
cargo build --release --locked

cd /Users/Admin/work/kv/rustkv
cargo build --locked
cargo test --locked
git diff -- benchmarks docs
```

全部通过且差异只包含本阶段文件。等待用户 Review 后，才允许提交 `benchmark stage B4: 六类负载`。

【输出】

1. 改动文件清单。
2. 六类负载到 Backend 调用及 op/records 的映射表。
3. 假 Backend 失败矩阵和双真实 Backend 端到端测试结果。
4. 证明 Range 使用 Iterator、Batch 一次原子提交的说明。
5. 明确写出“等待用户 Review，尚未提交”。

# RustKV Benchmark 阶段 B5：模板恢复与正确性验证（历史能力保留）

## 现行效力声明

1. B5 已按提交 `27831c17074b7bc4f4cd8f3727540ea647d908af` 验收；下文的模板生成、`cp -cR` 恢复和副本隔离要求只记录当时已经实现的历史能力，不再是最终 Benchmark 正式执行规范。
2. 现行唯一性能执行规范采用逐 RunUnit Load → Run：每个 RunUnit 创建独立新目录并直接完成 Load、关闭重开初始验证、正式 Run 和终态验证。
3. B6/B7 禁止调用模板生成、模板恢复、模板发布、密封模板、APFS COW 克隆或物理目录复制来生成正式 Run 的初始数据库，也不得为了节省 Load 时间重新接回这些能力。
4. B5 已实现代码继续保留且维持既有测试；最终路径只继承安全目录所有权、完整顺序预热以及关闭重开全量验证的语义，不继承模板/复制流程。
5. 本声明优先于下文所有历史模板条款；下文不得被解释为对 B6/B7 的授权。

## 全局约束

1. 唯一性能执行规范是 `/Users/Admin/work/kv/benchmark_plan.md`，SHA-256 必须与 [`04_Benchmark分阶段实现方案.md`](./04_Benchmark分阶段实现方案.md) 记录值一致。
2. 本阶段历史实现负责当时的计时外数据库状态准备、恢复、预热和验证；不得改变 B4 的计时内请求。
3. 当前代码中保留的模板复制曾允许使用 macOS `/bin/cp -cR` 的 clonefile 优先语义，但该能力不得进入现行正式 Benchmark 路径。
4. 只能操作 Benchmark 自己创建并登记的模板/运行目录；禁止跟随符号链接或删除用户目录。
5. 只修改【实现文件】；测试结束后等待用户 Review，不得自行提交。

---

【历史任务】为两个 Backend 建立关闭状态模板，实现每次运行的独立目录恢复、读取预热和计时后全量正确性验证。该任务已经验收，但其中模板与恢复部分已被现行方案替代。

【读取章节】只读以下章节，其余章节本次不读：

- `benchmark_plan.md` 第3节“固定配置”
- `benchmark_plan.md` 第5节“初始状态”
- `benchmark_plan.md` 第6节“并发与计时”中的计时外边界
- `benchmark_plan.md` 第7节“正确性要求”
- `benchmark_plan.md` 第8节“执行环境与结果”中的当前 Mac 环境约束
- `系统设计文档_v2.md` 第4.7.2节“Drop”
- `系统设计文档_v2.md` 第9.3节“Get和Iterator验证”

【实现文件】

- `docs/04_Benchmark分阶段实现方案.md`（仅更新 B5 状态和验收提交）
- `benchmarks/src/lib.rs`
- `benchmarks/src/fs.rs`
- `benchmarks/src/template.rs`
- `benchmarks/src/validation.rs`
- `benchmarks/tests/fs.rs`
- `benchmarks/tests/template.rs`
- `benchmarks/tests/validation.rs`

【历史接口契约】以下条款只描述 B5 已验收能力，不约束 B6/B7 正式执行路径。

- RustKV 和 LevelDB 各自拥有一个包含同样 10,000,000 条逻辑记录的模板；物理格式不同，禁止跨 Backend 复用目录。
- 模板只能由 `BenchConfig::formal()` 建立：Key 按编号递增，使用固定 1 KiB Value，每个原子 Batch 恰好 1000 条，`sync=false`。
- 模板装载完成后必须关闭、重开，并通过一次完整有序 Iterator 验证：记录数恰好 10,000,000、Key 依次等于编号 0..9,999,999、Value 逐字节等于固定 Value。
- 只有验证通过的关闭状态目录才能原子发布为模板；不完整临时模板不得被正式运行使用。
- 每次运行创建唯一空目标目录；在数据库关闭时调用 `/bin/cp -cR` 从对应模板恢复。命令失败、目标预先存在或复制后布局不完整时立即失败；clonefile 不受文件系统支持时允许由 macOS `cp` 明确回退到普通复制。
- 模板恢复后对运行目录的任何写入不得改变模板。模板必须保持关闭且只读使用，不能作为被测数据库直接打开。
- 点查/Range：恢复对应模板，打开后在计时前执行一次从最小 Key 开始的完整有序 Iterator Scan，访问每条 Key/Value并验证总数；该预热不计入正式指标。
- 单条/批量插入：每次创建全新空数据库，不从满模板恢复，不执行变更型预热。
- 单条/批量删除：恢复对应满模板，不执行变更型预热。
- 计时后先停止全部工作线程，再关闭、重开数据库并验证终态。插入必须得到完整 0..9,999,999 及正确 Value；删除必须没有用户记录；读取类再次验证数据未改变。
- 全量验证通过 B2 的 Iterator Scan “计时外全量验证模式”完成，必须逐条验证编号和完整 Value 字节；不得仅检查条数或 Value 长度。
- 模板建立、复制、Open、Close、预热、重开和全量验证耗时均不得进入工作负载墙钟时间或请求延迟。

【历史禁止事项】以下条款只约束保留的 B5 模板能力；B6/B7 适用更严格的“完全禁止模板正式路径”规则。

- 禁止把同一个可写数据库目录连续用于不同重复、并发度或 Backend。
- 禁止用硬链接制作可写副本，禁止跟随模板中的符号链接。
- 禁止在数据库打开时复制模板或运行目录。
- 禁止跳过关闭重开、只抽样验证、只验证记录数或只验证 Value 长度。
- 禁止给插入/删除执行预写、预删或其他变更型预热。
- 禁止把失败/中断的模板或运行目录标记为可复用。
- 禁止清理未登记路径、模板路径、仓库根目录或用户已有文件。

【历史测试要求】这些测试继续用于防止已保留代码退化，但不能证明现行 Load → Run 路径正确。

- 用小配置分别为真实 RustKV/LevelDB 创建模板，关闭重开后逐 Key/Value 全量验证。
- 对同一模板创建两个独立副本，修改/删除其中一个后，另一个和模板必须保持原值；验证目标预存在时拒绝覆盖。
- 构造缺文件、额外符号链接、截断文件、打开中复制和未完成发布等场景，必须安全失败且不把目录标成有效模板。
- 读取预热测试验证调用一次完整 Iterator、访问所有 Key/Value，且 Runner 的墙钟/延迟计数在预热前后仍为零。
- 插入准备必须为空；删除准备必须包含全集；两者均无变更型预热。
- 对双 Backend 的小规模插入、删除、点查、Range 运行执行“关闭—重开—全量验证”，注入缺 Key、额外 Key、错误 Key、错误 Value 和残留删除记录均必须判失败。
- 运行目录清理测试只能删除登记的测试目录，模板和同级哨兵文件必须保留。

【历史验收】

```bash
cd /Users/Admin/work/kv/rustkv/benchmarks
cargo fmt --check
cargo build --locked
cargo test --locked --test fs --test template --test validation
cargo test --locked
cargo build --release --locked

cd /Users/Admin/work/kv/rustkv
cargo build --locked
cargo test --locked
git diff -- benchmarks docs
```

该历史验收已经完成并提交。B6 必须另行测试逐 RunUnit Load → Run，禁止以这些模板测试替代。

【历史输出】

1. 改动文件清单。
2. 模板发布、`cp -cR` 恢复、安全清理和计时边界说明。
3. 双 Backend 关闭重开全量验证结果。
4. 模板/副本隔离及错误注入测试结果。
5. 明确写出“等待用户 Review，尚未提交”。

# RustKV Benchmark 阶段 B6：逐 RunUnit Load → Run、矩阵编排与报告工具

## 全局约束

1. 唯一性能执行规范是 `/Users/Admin/work/kv/benchmark_plan.md`，SHA-256 必须与 [`04_Benchmark分阶段实现方案.md`](./04_Benchmark分阶段实现方案.md) 记录值一致。
2. 本阶段实现可审计的逐 RunUnit Load → Run、命令行、240 次矩阵、原始 CSV 和报告工具；只运行小规模 smoke，不产生正式性能报告。
3. 正式配置和矩阵不得通过 CLI 缩小；smoke 必须使用不同命令/模式并在输出中明确标记。
4. 每个 RunUnit 必须使用新建且独占的数据库目录。禁止调用 B5 模板生成/恢复、密封模板、APFS COW 克隆或物理目录复制能力，也不得以节省 Load 时间为由重新引入。
5. 失败运行必须保留原始记录但不得进入有效汇总，禁止静默补值或删除失败行。
6. 只修改【实现文件】；测试结束后等待用户 Review，不得自行提交。

---

【任务】实现逐 RunUnit 的直接 Load → Run、单单元执行、全矩阵编排、可恢复 CSV 写入、五次中位数汇总、六张 SVG 图和 Markdown 报告生成，并以双 Backend 小规模 smoke 贯通全链路。

【读取章节】只读以下章节，其余章节本次不读：

- `benchmark_plan.md` 第3节“固定配置”
- `benchmark_plan.md` 第4节“固定测试矩阵”
- `benchmark_plan.md` 第5节“逐 RunUnit 初始状态与 Load → Run”
- `benchmark_plan.md` 第6节“并发与计时”
- `benchmark_plan.md` 第7节“正确性要求”
- `benchmark_plan.md` 第8节“执行环境与结果”

【实现文件】

- `benchmark_plan.md`
- `docs/04_Benchmark分阶段实现方案.md`
- `docs/10_Benchmark阶段B5_模板恢复与正确性验证.md`
- `docs/11_Benchmark阶段B6_矩阵编排与报告工具.md`
- `docs/12_Benchmark阶段B7_Mac正式跑测与报告.md`
- `docs/中途修改说明.md`
- `benchmarks/src/lib.rs`
- `benchmarks/src/main.rs`
- `benchmarks/src/cli.rs`
- `benchmarks/src/matrix.rs`
- `benchmarks/src/run_unit.rs`
- `benchmarks/src/csv.rs`
- `benchmarks/src/report.rs`
- `benchmarks/src/validation.rs`
- `benchmarks/src/fs.rs`（只允许移除尚未验收的密封模板扩展或提供独立 RunUnit 目录能力）
- `benchmarks/src/template.rs`（只允许移除尚未验收的密封模板扩展；B5 已提交历史能力保留）
- `benchmarks/scripts/run_smoke.sh`
- `benchmarks/tests/cli.rs`
- `benchmarks/tests/matrix.rs`
- `benchmarks/tests/run_unit.rs`
- `benchmarks/tests/csv.rs`
- `benchmarks/tests/report.rs`
- `benchmarks/tests/validation.rs`
- `benchmarks/tests/fs.rs`（仅对应上述 `fs.rs` 范围）
- `benchmarks/tests/template.rs`（只移除尚未验收的密封模板测试）
- `benchmarks/tests/build_smoke.rs`
- `benchmarks/tests/fixtures/report_input.csv`
- `benchmarks/tests/fixtures/report_expected.md`

此前 B6 尚未验收的 `prepare`、跨进程密封模板和模板复用实现全部废弃。B5 已提交的模板实现及其历史测试继续保留，但 `run-one`、`matrix`、`smoke` 不得调用任何模板构建、复制、恢复、发布、密封或重开登记入口。

【接口契约】

- CLI 只提供 `run-one`、`matrix`、`report`、`smoke` 四个子命令；删除 `prepare` 命令及帮助文本。所有路径、Backend、负载、线程数和重复编号必须解析为强类型并验证。
- `run-one` 只执行一个完整的逐 RunUnit Load → Run 单元；`matrix` 编排全部正式单元；`report` 只读取 CSV 生成汇总；`smoke` 使用内置小配置贯通两 Backend。
- 每个 RunUnit 严格执行：生成固定 Trace→创建独立目录→按负载直接 Load→关闭→重开并全量验证初始状态→关闭→独立打开正式 Run→读取负载完整顺序预热→Barrier 后计时执行 Trace→结束计时→关闭重开→全量验证终态→原子写一行 CSV→清理本 RunUnit 目录。
- 点查、Range、单条删除和批量删除的 Load 必须在本 RunUnit 目录按 Key 递增写入全部记录，每个原子 Batch 恰好 1000 条、`sync=false`；插入负载只建立空数据库。正式配置为 10,000,000 条，smoke 使用编译期固定小配置。
- Load 完成后必须关闭第一次数据库实例；第二次以既有库模式打开并完整验证满库或空库初始状态，再次关闭；正式 Run 使用第三次独立打开。任何一步失败都不得进入计时。
- 点查和 Range 在第三次打开后、工作线程创建前执行一次完整顺序 Iterator 预热并逐 Key/Value 验证；插入和删除不执行额外 Put/Delete/Get/Iterator 预热。删除允许继承 Load 和初始状态验证产生的缓存状态，不主动清缓存。
- 只有 Runner 内 Barrier 释放后的 Run 请求进入 `wall_seconds` 和请求延迟；Trace、目录创建、Load、Open/Close、初始验证、读取预热、终态验证、CSV 和清理一律计时外。
- 每个 RunUnit 的数据库目录身份必须唯一且由当前 Benchmark workspace 登记；不同 Backend、负载、线程数或重复编号不得复用同一可写目录。
- `matrix --dry-run` 不打开数据库，确定性输出恰好 240 个唯一 Run ID：6 负载 × 4 并发 × 2 Backend × 5 重复。
- 正式 Run ID 使用新的 `rustkv-leveldb-v2` 配置版本（与废弃模板路径的 v1 隔离），并完整包含 Backend、负载、线程数和重复编号；旧 v1 CSV 不得被 resume 或报告接受。
- 每个负载/线程/重复组合的两个 Backend 顺序按重复编号与组合序号奇偶交替；双方仍顺序运行，禁止同时竞争机器资源。
- 矩阵使用 `BenchConfig::formal()`，CLI 不提供 record count、Value 大小、Range 长度、Batch 大小、工作量、种子或重复次数覆盖选项。
- smoke 使用编译进二进制的固定小配置，输出 `mode=smoke` 且写入独立目录；smoke CSV 不能被 `report` 当作正式输入。
- 原始 CSV 每个完成或失败的 Run ID 恰好一行，至少包含：模式、配置版本、Backend、负载、线程数、重复编号、完成 op、完成 records、墙钟秒、ops/s、records/s、平均/P50/P95/P99（us/请求）、错误数、验证状态、错误文本、RustKV commit、LevelDB commit和环境 ID。
- CSV 使用稳定列顺序、RFC 4180 转义和充分数值精度；每行先写临时文件/检查点并同步后再发布，进程中断不能产生被误认成有效的半行。
- `matrix --resume` 只跳过 CSV 中 Run ID 唯一、字段完整、错误数 0、验证成功且配置/commit/环境完全匹配的行；恢复后未完成的每个 RunUnit 仍从新的独立目录重新 Load，不得复用中断遗留数据库。
- 汇总前严格验证 240 行、每单元 5 次有效运行、无重复 Run ID、无错误、全部验证成功和固定字段一致。
- 每个数据库/负载/线程的 ops/s 为五次运行中位数；P50/P95/P99 分别取五个对应单次运行分位数的中位数；RustKV/LevelDB 比值用双方 ops/s 中位数计算，不平均百分位数、不删除离群值。
- Range/Batch 输出辅助 records/s 中位数；其他负载该字段保持空值或明确不适用，不能伪装成主指标。
- 报告生成六张独立 SVG：横轴 `[1,10,100,1000]`，纵轴 `ops/s`，RustKV/LevelDB 各一条线；SVG 坐标和刻度由本项目直接生成，不引入绘图库。
- Markdown 报告包含固定配置、逐 RunUnit Load → Run 初始状态、Mac 环境占位引用、六张图、六张结果表、辅助 records/s、正确性结论和原始 CSV 相对链接；不要求披露 RustKV 直调与 LevelDB FFI 差异。

【禁止事项】

- 禁止 B6/B7 正式或 smoke 路径调用 `build_template`、`restore`、`prepare_both_templates`、密封模板加载或任何目录 clone/copy；静态测试必须锁定该依赖边界。
- 禁止跳过逐 RunUnit Load，禁止在不同 RunUnit 间保留并复用已装载数据库，禁止把失败或中断遗留目录收养为后续初始状态。
- 禁止将 Load、初始验证、读取预热、终态验证或目录清理耗时加入正式墙钟或请求延迟。
- 禁止为删除负载增加额外操作预热，禁止主动清除其中一个 Backend 的系统缓存。
- 禁止减少或增加正式 240 次运行，禁止动态早停或根据前一结果调整工作量。
- 禁止让两个 Backend 同时运行，禁止改变交替顺序来挑选更好结果。
- 禁止覆盖已有正式行、删除失败行、自动重跑直到得到更好数值或只保留最好结果。
- 禁止把五次吞吐量取平均、把线程百分位数平均或把 Range/Batch records/s 当主吞吐量。
- 禁止使用 Python/R/Excel/gnuplot、Criterion 或外部在线服务生成正式图表和报告。
- 禁止在 smoke 输出中写 `mode=formal`，或用 smoke 通过替代正式 B7。

【测试要求】

- CLI 单元测试覆盖四个子命令、必填参数、未知/已删除的 `prepare`、非法 Backend/负载/线程/重复编号、路径冲突及退出码。
- 逐 RunUnit 状态机测试必须对真实 RustKV 和 LevelDB 的六类负载证明调用顺序：独立目录→Load 或空库→第一次关闭→第二次打开初始验证→第二次关闭→第三次打开→规定预热→Run→关闭重开终态验证→CSV→清理。
- 测试必须证明满库 Load 使用递增 Key、固定 Value、1000 条 Batch（测试尾批允许由小配置不足 1000 条形成），插入初始状态为空；Load/初始验证失败不得启动工作线程或产生有效指标。
- 测试必须证明读取仅执行一次完整顺序预热，插入/删除无额外操作预热；初始验证和预热均不改变 Runner 的 `wall_seconds` 或请求延迟样本。
- 静态或行为测试必须证明 B6 的 CLI、matrix、run_unit 和 smoke 不引用 B5 模板构建/恢复/密封接口，不调用 `cp -cR`、clonefile、硬链接或物理复制。
- 每个单元必须使用不同登记目录；成功行原子落盘后清理该目录；失败行保留且不得被 resume 当作完成；中断恢复必须为剩余单元创建新目录并重新 Load。
- `matrix --dry-run` golden 测试断言 240 个唯一 Run ID、每组合 5 次、线程集合精确、双方顺序交替且多次运行输出一致。
- CSV 测试覆盖 Unicode/逗号/换行错误文本转义、浮点精度、原子追加、中断半行、重复 Run ID、配置/commit/环境不匹配和 resume 判定。
- 汇总 golden 使用手写五次非排序数据，严格断言 ops/s 和各延迟列中位数、双方比值、Range/Batch records/s。
- 报告 golden 断言六个负载章节、六张 SVG、四个并发点、两条曲线、单位 `ops/s` 与 `us/请求`、逐 RunUnit Load → Run 说明、原始 CSV 链接和正确性结论。
- 缺行、多行、失败行、验证失败、NaN/Infinity、错误单位或混入 smoke 时报告生成必须失败。
- `run_smoke.sh` 在当前 Mac 对真实 RustKV/LevelDB 执行六负载 × 至少 1/10 线程，完整走逐 RunUnit Load、关闭重开初始验证、读取预热、计时、关闭重开终态验证、CSV、清理和报告；只检查正确性，不设性能阈值。

【验收】

```bash
cd /Users/Admin/work/kv/rustkv/benchmarks
cargo fmt --check
cargo build --locked
cargo test --locked --test cli --test matrix --test run_unit --test csv --test report --test fs --test template --test validation --test build_smoke
cargo test --locked
cargo build --release --locked
cargo run --release --locked -- matrix --dry-run
./scripts/run_smoke.sh

cd /Users/Admin/work/kv/rustkv
cargo build --locked
cargo test --locked
git diff -- benchmarks docs
```

全部通过；dry-run 恰好 240 项；smoke 全部正确；正式路径不存在模板调用；差异只包含本阶段文件。等待用户 Review 后，才允许提交 `benchmark stage B6: 逐RunUnit Load-Run矩阵与报告工具`。

【输出】

1. 改动文件清单。
2. 逐 RunUnit Load → Run 状态机、计时边界、CLI、Run ID、矩阵顺序、resume/CSV 原子性和汇总算法说明。
3. 240 项 dry-run 统计与所有单元/golden 测试结果。
4. 双 Backend smoke 的正确性结果及输出目录。
5. 明确写出“等待用户 Review，尚未提交”。

## 验收后附加说明

B6 已于提交 `5d0c3958408bbb99384e6e8ac8ace3da5afd790a` 验收。此后用户要求增加单项自定义规模测试：`custom-run` 和 `scripts/run_custom.sh` 一次只执行用户指定的一个 Backend、负载、线程数和条目数。该入口复用本阶段逐 RunUnit 生命周期，但使用独立 `mode=custom` 结果格式，属于非正式辅助能力；本阶段冻结的正式 `run-one/matrix/report` 语义和 B7 正式矩阵不变，自定义结果不得进入正式报告。

# RustKV Benchmark 阶段 B7：当前 Mac 正式跑测与报告

## 验收与执行移交

Benchmark 实现、执行入口以及本阶段规范已经由用户验收，当前 Mac 的实际性能跑测、结果汇总和报告生成移交用户自行完成。此验收不表示仓库已经产生 240 行正式结果、六张图或最终性能报告；下文仍是用户生成这些正式产物时必须遵守的执行与验收规范。`custom-run`、`run_custom.sh` 及其自定义结果只能用于用户自主试跑，不能冒充下文定义的正式矩阵和正式报告。

## 全局约束

1. 唯一性能执行规范是 `/Users/Admin/work/kv/benchmark_plan.md`，SHA-256 必须与 [`04_Benchmark分阶段实现方案.md`](./04_Benchmark分阶段实现方案.md) 记录值一致；本阶段不得修改固定配置、Trace、Backend、计时或统计语义。
2. B0～B6 必须全部验收后才能开始。本阶段原则上只增加运行脚本和结果，不修改 Benchmark/RustKV 实现代码。
3. 正式结果在当前 Apple Silicon Mac 上产生，结论只代表本次记录的 Mac 环境，不外推为 Linux 或其他硬件结果。
4. 任何构建、环境、资源、运行或验证错误都必须停止并报告；禁止降低并发、工作量、重复次数或正确性标准。
5. 首次完整结果生成后必须等待用户 Review，不得自行提交。

---

【任务】冻结当前 Mac 环境，按逐 RunUnit Load → Run 模型执行 240 次正式运行，验证原始 CSV，生成六张图、汇总 CSV 和最终 Markdown 性能对比报告。

【读取章节】只读以下章节，其余章节本次不读：

- `benchmark_plan.md` 全文
- `04_Benchmark分阶段实现方案.md` 第4～7节
- B0～B6 各阶段【验收】和【输出】部分

【实现文件】

- `docs/04_Benchmark分阶段实现方案.md`（仅更新 B7 状态和验收提交）
- `benchmarks/scripts/capture_environment.sh`
- `benchmarks/scripts/run_formal_mac.sh`
- `benchmarks/results/mac/environment.txt`
- `benchmarks/results/mac/raw.csv`
- `benchmarks/results/mac/report/random_get.svg`
- `benchmarks/results/mac/report/range_scan.svg`
- `benchmarks/results/mac/report/single_put.svg`
- `benchmarks/results/mac/report/batch_put.svg`
- `benchmarks/results/mac/report/single_delete.svg`
- `benchmarks/results/mac/report/batch_delete.svg`
- `benchmarks/results/mac/report/report.md`

运行中断检查点只允许位于被 `.gitignore` 排除的 `benchmarks/.runs/`，不得作为最终报告输入直接提交。

【接口契约】

- `capture_environment.sh` 在跑测前一次性记录：时间、时区、Mac 型号、CPU 架构/型号与核心数、内存、SSD、文件系统与可用空间、macOS 版本/build/kernel、Rust/C/C++/CMake 版本、RustKV commit、Benchmark commit/dirty 状态、LevelDB 版本/commit及电源状态。
- 正式开始时 RustKV 仓库必须处于已知状态；如有未提交改动，环境文件必须逐项记录且用户明确批准，否则停止。
- LevelDB 必须为 1.23/`99b3c03` Release；`kv_bench` 和 RustKV 必须使用 `cargo build --release --locked` 产物；不得在 240 次之间重新编译或更换二进制。
- 跑测前检查本地 SSD 可用空间、逐 RunUnit Load 所需峰值空间、结果路径、文件描述符/线程资源和 1000 OS 线程创建能力；不足时停止，不得缩小测试。
- 正式运行期间使用同一电源模式，禁止系统睡眠；两个 Backend 顺序运行，不并行运行其他 Benchmark。无法控制的环境变化必须记录在环境文件和报告限制中。
- `run_formal_mac.sh` 只调用已验收 `kv_bench matrix`；正式矩阵固定 240 个 Run ID，按照 B6 交替 Backend 顺序执行。
- 每个正式 RunUnit 必须创建新的独立数据库目录，并严格执行 Trace 生成→直接 Load/空库建立→关闭→重开全量验证初始状态→关闭→独立打开 Run→规定预热→Barrier 后计时→关闭重开终态验证→CSV→清理；不得先准备或恢复模板。
- 点查、Range 和删除负载必须在各自 RunUnit 内重新 Load 10,000,000 条；插入负载建立空库。Load 失败、初始验证失败或关闭重开失败不得进入正式 Run 计时。
- Load、初始验证产生的缓存状态属于统一流程；不得主动清缓存。读取负载执行规定的完整顺序预热，删除负载不执行额外删除或读取预热。
- 每个单元完成后立即持久化原始行和 resume 检查点。中断恢复只允许跳过同环境、同 commit、同配置且已成功验证的 Run ID；不得重复挑选较优结果。
- 每次正式运行必须满足预期 op/records、错误数 0、关闭重开全量验证成功；失败行保留，但本轮矩阵不算完成且不得进入性能汇总。
- 240 行完成后执行严格校验：48 个测试单元、每单元 5 次、Run ID 唯一、线程数 `[1,10,100,1000]`、全部 formal、固定工作量/种子一致、错误数 0、验证全通过。
- 最终生成六张 SVG 和 `report/report.md`；中位数汇总表直接写入 Markdown。报告写明结果机器为当前 Mac、固定配置、五次中位数、延迟单位 `us/请求`、主吞吐量 `ops/s`、Range/Batch 辅助 `records/s`、RustKV/LevelDB 比值和正确性结论。
- 报告不得宣称结果代表 Linux、所有 Apple Silicon 或其他硬件；不得从数据无法支持的现象推断内部原因。

【禁止事项】

- 禁止执行 `prepare`，禁止调用 B5 模板生成/恢复、密封模板、APFS COW 克隆或物理目录复制；禁止为了缩短 240 次逐单元 Load 而重新引入模板。
- 禁止把 `custom-run`、`run_custom.sh`、开发 smoke、Debug 构建或历史结果混入正式 CSV。
- 禁止在跑测中修改代码、Cargo.lock、LevelDB、配置、Trace 或机器；需要修复代码时立即终止 B7，回到对应阶段补测试并重新验收。
- 禁止删除失败结果、只保留最好五次、额外运行后挑选五次或手工修改 CSV 数值。
- 禁止因 1000 线程失败而改用线程池、协程或较低并发。
- 禁止用缓存未预热/额外预热其中一个 Backend，禁止为删除负载增加额外预热，或让双方同时运行。
- 禁止覆盖既有 `benchmarks/results/mac`；目录已存在时停止，由用户决定归档方式。
- 禁止未经用户 Review 自动提交正式结果。

【测试要求】

- 正式跑测前重跑 B6 全量测试和双 Backend smoke，确认同一 Release 二进制可执行。
- 环境采集测试逐字段检查非空、命令退出码、commit/dirty 状态和 LevelDB 版本；环境 ID 必须稳定写入每一 CSV 行。
- 预检真实创建并 Join 1000 个最小 OS 线程，不执行数据库请求；失败即阻塞。
- 逐 RunUnit 测试证据必须证明每个满库单元都在自身目录完成 10,000,000 条 Load、关闭重开初始验证和计时后的终态验证；插入单元初始空库验证必须通过。
- 静态检查和执行日志必须证明正式脚本未调用 `prepare` 或模板/clone/copy 路径，resume 后剩余单元仍创建新目录并重新 Load。
- 原始 CSV 验证恰好 240 行、48 单元各 5 行、无重复、无 smoke、错误数全 0、验证全 true、工作量和 records 计数与负载匹配。
- 汇总结果由独立再解析检查中位数、比值和 Range/Batch `records/s = ops/s × 100`；禁止只相信报告生成器成功退出。
- 六张 SVG 和 Markdown 中每个负载、四个并发点、两 Backend、单位和链接必须齐全；所有相对链接在仓库内可解析。
- 完成后再执行 Benchmark 全量测试和 RustKV 全量回归，证明跑测脚本/结果未改代码语义。

【验收】

```bash
cd /Users/Admin/work/kv/rustkv/benchmarks
cargo fmt --check
cargo build --locked
cargo test --locked
cargo build --release --locked
./scripts/capture_environment.sh
./scripts/run_smoke.sh
./scripts/run_formal_mac.sh
cargo run --release --locked -- report \
  --csv /Users/Admin/work/kv/rustkv/benchmarks/results/mac/raw.csv \
  --output-dir /Users/Admin/work/kv/rustkv/benchmarks/results/mac/report

cd /Users/Admin/work/kv/rustkv
cargo build --locked
cargo test --locked
git diff -- benchmarks docs
```

验收必须同时满足：240 次有效运行、错误数 0、全量验证通过、六张图和报告生成、全部链接有效、差异只含本阶段允许文件。等待用户 Review 后，才允许提交 `benchmark stage B7: 当前Mac正式跑测与报告`。

【输出】

1. 改动/结果文件清单。
2. 环境摘要、commit、LevelDB 版本和预检结果。
3. 240 次运行完成数、失败数、resume 情况及总耗时。
4. CSV 严格验证、汇总复算、六张图和报告路径。
5. Benchmark/RustKV 全量测试结果。
6. 明确写出“等待用户 Review，尚未提交”；任何失败须如实报告，不得生成完成结论。
