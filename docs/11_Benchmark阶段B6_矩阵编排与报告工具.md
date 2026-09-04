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
