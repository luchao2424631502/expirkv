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
