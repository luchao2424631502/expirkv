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
