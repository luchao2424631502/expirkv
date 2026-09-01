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
