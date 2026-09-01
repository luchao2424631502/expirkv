# RustKV Benchmark 阶段 B6：矩阵编排与报告工具

## 全局约束

1. 唯一性能执行规范是 `/Users/Admin/work/kv/benchmark_plan.md`，SHA-256 必须与 [`04_Benchmark分阶段实现方案.md`](./04_Benchmark分阶段实现方案.md) 记录值一致。
2. 本阶段实现可审计的命令行、240 次矩阵、原始 CSV 和报告工具；只运行小规模 smoke，不产生正式性能报告。
3. 正式配置和矩阵不得通过 CLI 缩小；smoke 必须使用不同命令/模式并在输出中明确标记。
4. 失败运行必须保留原始记录但不得进入有效汇总，禁止静默补值或删除失败行。
5. 只修改【实现文件】；测试结束后等待用户 Review，不得自行提交。

---

【任务】实现模板准备、单单元执行、全矩阵编排、可恢复 CSV 写入、五次中位数汇总、六张 SVG 图和 Markdown 报告生成，并以双 Backend 小规模 smoke 贯通全链路。

【读取章节】只读以下章节，其余章节本次不读：

- `benchmark_plan.md` 第3节“固定配置”
- `benchmark_plan.md` 第4节“固定测试矩阵”
- `benchmark_plan.md` 第5节“初始状态”
- `benchmark_plan.md` 第6节“并发与计时”
- `benchmark_plan.md` 第7节“正确性要求”
- `benchmark_plan.md` 第8节“执行环境与结果”

【实现文件】

- `docs/04_Benchmark分阶段实现方案.md`（仅更新 B6 状态和验收提交）
- `benchmarks/src/lib.rs`
- `benchmarks/src/main.rs`
- `benchmarks/src/cli.rs`
- `benchmarks/src/matrix.rs`
- `benchmarks/src/csv.rs`
- `benchmarks/src/report.rs`
- `benchmarks/scripts/run_smoke.sh`
- `benchmarks/tests/cli.rs`
- `benchmarks/tests/matrix.rs`
- `benchmarks/tests/csv.rs`
- `benchmarks/tests/report.rs`
- `benchmarks/tests/fixtures/report_input.csv`
- `benchmarks/tests/fixtures/report_expected.md`

【接口契约】

- CLI 至少提供 `prepare`、`run-one`、`matrix`、`report`、`smoke` 五个明确子命令；所有路径、Backend、负载、线程数和重复编号必须解析为强类型并验证。
- `prepare` 建立并验证双方关闭模板；`run-one` 只执行一个完整“恢复/准备—预热—计时—关闭重开验证”单元；`matrix` 编排全部正式单元；`report` 只读取 CSV 生成汇总；`smoke` 使用内置小配置贯通两 Backend。
- `matrix --dry-run` 不打开数据库，确定性输出恰好 240 个唯一 Run ID：6 负载 × 4 并发 × 2 Backend × 5 重复。
- 正式 Run ID 完整包含配置版本、Backend、负载、线程数和重复编号；CSV 中不得出现无法定位配置的行。
- 每个负载/线程/重复组合的两个 Backend 顺序按重复编号与组合序号奇偶交替；双方仍顺序运行，禁止同时竞争机器资源。
- 矩阵使用 `BenchConfig::formal()`，CLI 不提供 record count、Value 大小、Range 长度、Batch 大小、工作量、种子或重复次数覆盖选项。
- smoke 使用编译进二进制的固定小配置，输出 `mode=smoke` 且写入独立目录；smoke CSV 不能被 `report` 当作正式输入。
- 原始 CSV 每个完成或失败的 Run ID 恰好一行，至少包含：模式、配置版本、Backend、负载、线程数、重复编号、完成 op、完成 records、墙钟秒、ops/s、records/s、平均/P50/P95/P99（us/请求）、错误数、验证状态、错误文本、RustKV commit、LevelDB commit和环境 ID。
- CSV 使用稳定列顺序、RFC 4180 转义和充分数值精度；每行先写临时文件/检查点并同步后再发布，进程中断不能产生被误认成有效的半行。
- `matrix --resume` 只跳过 CSV 中 Run ID 唯一、字段完整、错误数 0、验证成功且配置/commit/环境完全匹配的行；重复、冲突、损坏或失败行必须停止并报告，不得猜测。
- 汇总前严格验证 240 行、每单元 5 次有效运行、无重复 Run ID、无错误、全部验证成功和固定字段一致。
- 每个数据库/负载/线程的 ops/s 为五次运行中位数；P50/P95/P99 分别取五个对应单次运行分位数的中位数；RustKV/LevelDB 比值用双方 ops/s 中位数计算，不平均百分位数、不删除离群值。
- Range/Batch 输出辅助 records/s 中位数；其他负载该字段保持空值或明确不适用，不能伪装成主指标。
- 报告生成六张独立 SVG：横轴 `[1,10,100,1000]`，纵轴 `ops/s`，RustKV/LevelDB 各一条线；SVG 坐标和刻度由本项目直接生成，不引入绘图库。
- Markdown 报告包含固定配置、Mac 环境占位引用、六张图、六张结果表、辅助 records/s、正确性结论和原始 CSV 相对链接；不要求披露 RustKV 直调与 LevelDB FFI 差异。

【禁止事项】

- 禁止减少或增加正式 240 次运行，禁止动态早停或根据前一结果调整工作量。
- 禁止让两个 Backend 同时运行，禁止改变交替顺序来挑选更好结果。
- 禁止覆盖已有正式行、删除失败行、自动重跑直到得到更好数值或只保留最好结果。
- 禁止把五次吞吐量取平均、把线程百分位数平均或把 Range/Batch records/s 当主吞吐量。
- 禁止使用 Python/R/Excel/gnuplot、Criterion 或外部在线服务生成正式图表和报告。
- 禁止在 smoke 输出中写 `mode=formal`，或用 smoke 通过替代正式 B7。

【测试要求】

- CLI 单元测试覆盖所有子命令、必填参数、未知参数、非法 Backend/负载/线程/重复编号、路径冲突及退出码。
- `matrix --dry-run` golden 测试断言 240 个唯一 Run ID、每组合 5 次、线程集合精确、双方顺序交替且多次运行输出一致。
- CSV 测试覆盖 Unicode/逗号/换行错误文本转义、浮点精度、原子追加、中断半行、重复 Run ID、配置/commit/环境不匹配和 resume 判定。
- 汇总 golden 使用手写五次非排序数据，严格断言 ops/s 和各延迟列中位数、双方比值、Range/Batch records/s。
- 报告 golden 断言六个负载章节、六张 SVG、四个并发点、两条曲线、单位 `ops/s` 与 `us/请求`、原始 CSV 链接和正确性结论。
- 缺行、多行、失败行、验证失败、NaN/Infinity、错误单位或混入 smoke 时报告生成必须失败。
- `run_smoke.sh` 在当前 Mac 对真实 RustKV/LevelDB 执行六负载 × 至少 1/10 线程，完整走模板、预热、计时、重开验证、CSV 和报告；只检查正确性，不设性能阈值。

【验收】

```bash
cd /Users/Admin/work/kv/rustkv/benchmarks
cargo fmt --check
cargo build --locked
cargo test --locked --test cli --test matrix --test csv --test report
cargo test --locked
cargo build --release --locked
cargo run --release --locked -- matrix --dry-run
./scripts/run_smoke.sh

cd /Users/Admin/work/kv/rustkv
cargo build --locked
cargo test --locked
git diff -- benchmarks docs
```

全部通过；dry-run 恰好 240 项；smoke 全部正确；差异只包含本阶段文件。等待用户 Review 后，才允许提交 `benchmark stage B6: 矩阵编排与报告工具`。

【输出】

1. 改动文件清单。
2. CLI、Run ID、矩阵顺序、resume/CSV 原子性和汇总算法说明。
3. 240 项 dry-run 统计与所有单元/golden 测试结果。
4. 双 Backend smoke 的正确性结果及输出目录。
5. 明确写出“等待用户 Review，尚未提交”。
