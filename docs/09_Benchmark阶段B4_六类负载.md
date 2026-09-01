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
