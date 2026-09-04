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
