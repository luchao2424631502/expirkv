# RustKV Benchmark 分阶段实现方案

## 1. 文档定位

本文档是 RustKV/LevelDB 性能对比工作的实施总纲。具体实现必须逐阶段使用以下执行文档：

1. [`05_Benchmark阶段B0_工程骨架与构建基线.md`](./05_Benchmark阶段B0_工程骨架与构建基线.md)
2. [`06_Benchmark阶段B1_固定数据与Trace.md`](./06_Benchmark阶段B1_固定数据与Trace.md)
3. [`07_Benchmark阶段B2_双后端适配.md`](./07_Benchmark阶段B2_双后端适配.md)
4. [`08_Benchmark阶段B3_并发执行与统计.md`](./08_Benchmark阶段B3_并发执行与统计.md)
5. [`09_Benchmark阶段B4_六类负载.md`](./09_Benchmark阶段B4_六类负载.md)
6. [`10_Benchmark阶段B5_模板恢复与正确性验证.md`](./10_Benchmark阶段B5_模板恢复与正确性验证.md)
7. [`11_Benchmark阶段B6_矩阵编排与报告工具.md`](./11_Benchmark阶段B6_矩阵编排与报告工具.md)
8. [`12_Benchmark阶段B7_Mac正式跑测与报告.md`](./12_Benchmark阶段B7_Mac正式跑测与报告.md)

每次只能执行一个阶段。测试完成后必须先交由用户 Review；无论测试成功或失败，未经用户明确确认均不得提交。

## 2. 规范、目录与版本

- 唯一性能执行规范：`/Users/Admin/work/kv/benchmark_plan.md`
- Benchmark 方案 SHA-256：`2e4d954cec7489c44e9ca25dec814e6e132143518cdf69f84c3ae074106262d2`
- RustKV 语义规范：`/Users/Admin/work/kv/系统设计文档_v2.md`
- 系统设计 SHA-256：`29c3f572ed051f09665fe178f4d0ab180417069f8e7968ebf829fd43dc56b3fd`
- 需求背景：`/Users/Admin/work/kv/需求分析文档_v2.md`
- Benchmark 工程：`/Users/Admin/work/kv/rustkv/benchmarks`
- RustKV 工程：`/Users/Admin/work/kv/rustkv`
- 阶段文档：`/Users/Admin/work/kv/rustkv/docs`
- LevelDB：官方 `1.23`，release commit `99b3c03`

`benchmark_plan.md` 决定性能负载、固定参数、Key 分布、计量单位和报告内容；系统设计文档只决定 RustKV 公共 API 语义、安全边界及 L5 Benchmark 总体边界。发现无法由这两条规则消解的冲突时必须停止并报告，不得自行选择。

## 3. 阶段顺序

```text
B0 工程骨架、仓库内独立子crate、LevelDB 1.23构建/链接基线
 ↓
B1 固定配置、Key/Value、确定性随机数与全局Trace
 ↓
B2 BenchBackend、RustKV直调、LevelDB C API与两个C聚合函数
 ↓
B3 OS线程、Barrier、固定工作量计时、ops/s和us/请求统计
 ↓
B4 六类负载及双后端小规模端到端测试
 ↓
B5 模板生成/恢复、读取预热、计时外全量正确性验证
 ↓
B6 CLI、240次矩阵编排、CSV、汇总和Markdown/SVG报告工具
 ↓
B7 当前Mac正式跑测、240次有效结果和最终性能报告
```

B0～B7均在当前 Mac 上执行。B7生成的结论只代表文档记录的当前 Mac 环境，不外推到 Linux 或其他硬件。

## 4. 全局执行约束

1. 每次只执行一个阶段，前一阶段未验收不得开始后一阶段。
2. 只读取阶段【读取章节】列出的规范章节，只修改【实现文件】列出的文件。
3. Benchmark 必须是 RustKV Git 仓库内 `benchmarks/` 子目录中的独立 Rust crate，不修改 RustKV 根 crate 的 `src/`、`tests/`、`Cargo.toml` 或 `Cargo.lock`。
4. RustKV Backend 只能调用现有公共 API；不得增加 Benchmark 专用 RustKV 公共 API。
5. LevelDB 不使用第三方高层 Rust crate；Rust 直接声明官方 C API。
6. LevelDB 只允许增加 `bench_leveldb_write_batch()` 和 `bench_leveldb_iterator_scan()` 两个 C 聚合函数。
7. C 聚合函数只能编排官方 LevelDB C API，不得缓存、重试、改写输入或增加并发。
8. 禁止 YCSB、`db_bench`、Google Benchmark、Criterion 或其他通用 Benchmark 框架进入正式驱动路径。
9. 正式配置只能来自 `BenchConfig::formal()`；测试专用小配置不得进入正式输出。
10. 禁止 `todo!()`、`unimplemented!()`、忽略错误、占位成功、削弱断言或添加 `#[ignore]` 取得绿色结果。
11. 任一 API 错误、命中数错误、Range 条数/顺序错误或计时后验证失败都必须使该次运行无效。
12. 每阶段必须先执行定向测试，再执行 Benchmark 全量构建测试和 RustKV 全量回归。
13. 任何测试结果出来后先停止并等待用户 Review；不得自动提交。

## 5. 工程与Git边界

Benchmark 工程位于现有 RustKV Git 仓库的 `benchmarks/` 子目录，不建立嵌套或独立 Git 仓库。所有 Benchmark 源码、测试、脚本和正式结果都由 `/Users/Admin/work/kv/rustkv` 仓库统一管理。

每个已验收阶段对应 RustKV 仓库中的一个独立提交：

```text
benchmark stage B<N>: <阶段名称>
```

提交前必须确认：

```bash
cd /Users/Admin/work/kv/rustkv
git status --short
git diff -- benchmarks docs

cd /Users/Admin/work/kv/rustkv/benchmarks
cargo fmt --check
cargo build --locked
cargo test --locked
cargo build --release --locked

cd /Users/Admin/work/kv/rustkv
cargo build --locked
cargo test --locked
```

只有用户 Review 后明确批准，才允许创建该阶段提交。不得把两个阶段合并提交，也不得夹带用户已有改动。

## 6. 阶段状态

| 阶段 | 名称 | 状态 | 验收提交 |
|---|---|---|---|
| B0 | 工程骨架与构建基线 | 已验收 | `48ab1640f51c40b1f5b1fd2be7596c0e5547b1a6` |
| B1 | 固定数据与Trace | 已验收 | `16c58a9c3f32ee400464a5cade0ef7471c2e3c7c` |
| B2 | 双后端适配 | 已验收 | `7843a317dc13ba223ab151973072403ca01785cb` |
| B3 | 并发执行与统计 | 已验收 | `89f503d446a81954c145d1604260c33905ef0e5d` |
| B4 | 六类负载 | 已实现未验收 | — |
| B5 | 模板恢复与正确性验证 | 未开始 | — |
| B6 | 矩阵编排与报告工具 | 未开始 | — |
| B7 | Mac正式跑测与报告 | 未开始 | — |

B4 独立 Review 后经用户批准，将 `benchmarks/src/trace.rs` 纳入 B4 文件范围，用于保存并校验 Trace 的完整生成配置来源。

状态只能是：`未开始`、`执行中`、`已实现未验收`、`已验收`、`阻塞`。实现 AI 不得自行把阶段改为 `已验收`。

## 7. 统一阶段输出

每阶段必须输出：

1. 改动文件清单；
2. 关键实现及对应规范章节；
3. 定向测试命令、退出码和通过/失败数量；
4. Benchmark `cargo fmt/build/test/build --release` 结果；
5. RustKV `cargo build/test` 全量回归结果；
6. 错误、未完成项或阻塞项；
7. 明确说明“等待用户 Review，尚未提交”；
8. 用户批准提交后再补充提交 hash 和是否允许进入下一阶段。

不得只报告“实现完成”或“测试通过”。
