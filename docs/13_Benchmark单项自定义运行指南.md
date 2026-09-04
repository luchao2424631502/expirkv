# KV Benchmark 单项自定义运行指南

## 1. 用途

`benchmarks/scripts/run_custom.sh` 一次只执行一个用户指定的 RunUnit。脚本不会自动展开负载、线程或 Backend 矩阵。

自定义条目数的结果用于快速观察和调试性能，不是 B7 正式结果，不能合并到 `benchmarks/results/mac/raw.csv`，也不能交给正式 `report` 命令生成最终报告。

## 2. 脚本参数

```text
run_custom.sh \
  --backend rustkv|leveldb \
  --workload NAME \
  --threads 1|10|100|1000 \
  --records N \
  --output-dir ABS_PATH
```

| 参数 | 含义 |
|---|---|
| `--backend` | 本次只运行 `rustkv` 或 `leveldb` 中的一个 |
| `--workload` | 本次只运行一个负载，名称见下表 |
| `--threads` | 本次创建的 OS 工作线程数，只允许 `1`、`10`、`100`、`1000` |
| `--records` | 数据库条目数，必须至少为100且能被100整除 |
| `--output-dir` | 本次结果目录，必须是尚不存在的绝对路径，并且父目录必须已存在 |

可选负载：

| 名称 | 初始状态 | 计时内请求数 |
|---|---|---:|
| `random_get` | Load `N` 条记录 | `N` 次 Get |
| `range_scan` | Load `N` 条记录 | `N / 100` 次100条 Iterator Scan |
| `single_put` | 空库 | `N` 次 Put |
| `batch_put` | 空库 | `N / 100` 个100条原子 WriteBatch |
| `single_delete` | Load `N` 条记录 | `N` 次 Delete |
| `batch_delete` | Load `N` 条记录 | `N / 100` 个100条原子 WriteBatch |

其他配置继续固定：16 Bytes Key、1 KiB Value、Range=100、Batch=100、`sync=false`、关闭压缩、固定种子 `20260720`，以及双方相同的缓存和文件参数。

本次负载的请求数不能少于线程数，确保每个指定线程至少获得一个真实请求。因此Range和Batch在10线程时至少需要 `1000` 条记录，在100线程时至少需要 `10000` 条，在1000线程时至少需要 `100000` 条；单条操作和点查只需满足 `N >= threads`。

## 3. 使用示例

在 `rustkv/benchmarks` 目录执行：

```bash
./scripts/run_custom.sh \
  --backend rustkv \
  --workload random_get \
  --threads 10 \
  --records 100000 \
  --output-dir /private/tmp/rustkv-get-t10-100k
```

使用相同参数测试 LevelDB，只改变 Backend 和结果目录：

```bash
./scripts/run_custom.sh \
  --backend leveldb \
  --workload random_get \
  --threads 10 \
  --records 100000 \
  --output-dir /private/tmp/leveldb-get-t10-100k
```

脚本会自动执行 `cargo build --release --locked --bin kv_bench`，然后调用同一个 `kv_bench custom-run` 实现。双方比较时必须使用相同的 `--workload`、`--threads` 和 `--records`。

## 4. 执行流程和计时

自定义单项仍复用已经验收的逐 RunUnit 流程：

```text
Trace → 新目录 → Load或空库 → 关闭 → 重开验证初态 → 关闭
→ 重开Run → 读取负载预热 → Barrier后计时 → 关闭重开验证终态
→ 写结果 → 清理数据库目录
```

只有 Barrier 释放后的数据库请求进入 `wall_seconds` 和请求延迟。Trace、Load、关闭重开、初态验证、读取预热、终态验证和清理均不计时。

`random_get` 和 `range_scan` 执行完整顺序预热；插入和删除不执行额外操作预热。正式执行路径不使用模板、COW或目录复制。

## 5. 结果文件

每次成功运行在 `--output-dir` 下产生：

- `result.csv`：一条原始结果；
- `result.md`：便于直接查看的结果表；
- `parameters.txt`：完整参数、RustKV commit、工作树状态和LevelDB commit。

`result.csv` 的主吞吐量是 `ops_per_second`，延迟单位是 `us/请求`。Range和Batch另有 `records_per_second`；由于每个请求处理100条记录，其值为 `ops/s × 100`。

程序只有在关闭重开并完成终态验证后才把结果标记为成功。运行中会输出开始和完成进度，避免长时间无提示。中断时脚本会清理本次 `workspace`；结果目录会保留，重新执行时应指定新的、尚不存在的目录。

## 6. `kv_bench` 命令边界

| 命令 | 用途 |
|---|---|
| `run-one` | 单个正式配置RunUnit，参数冻结，不能修改条目数 |
| `custom-run` | 供 `run_custom.sh` 调用的单项自定义规模运行，结果明确标记为非正式 |
| `matrix` | B7固定240次正式矩阵 |
| `report` | 严格读取正式矩阵CSV并生成最终报告 |
| `smoke` | 固定小规模功能验证，不用于性能结论 |

## 7. 自定义规模批量脚本

已经单独完成单线程 `single_put` 和 `batch_put` 后，可用下面的脚本先补齐双方 Backend 的其余单线程负载，再执行10、100、1000线程下的六种完整负载矩阵：

```bash
cd /Users/Admin/work/kv/rustkv/benchmarks
caffeinate -i ./scripts/run_remaining_t1.sh \
  --output-root "$HOME/work/result"
```

脚本依次执行：

- 单线程部分：`2 Backend × 4 workload × 4 数据量 = 32` 个 RunUnit，不重复已经完成的单线程 `single_put` 和 `batch_put`；
- 并发部分：双方 Backend、六种 workload、1万/10万/100万/1000万数据量，以及10/100/1000线程；
- 1万数据量、1000线程下的 `range_scan`、`batch_put`、`batch_delete` 只有100个请求，无法让1000个线程实际参与，因此双方共6项明确跳过；
- 最终执行170个有效 RunUnit，另输出6条明确的跳过记录。

输出目录示例：

```text
leveldb_random_get_1w_t1
rustkv_range_scan_10w_t1
leveldb_single_delete_100w_t1
rustkv_batch_delete_1000w_t1
leveldb_single_put_10w_t100
rustkv_random_get_1000w_t1000
```

运行前可只查看完整清单，不创建目录、不执行 Benchmark：

```bash
./scripts/run_remaining_t1.sh \
  --output-root "$HOME/work/result" \
  --dry-run
```

目录统一使用 `<backend>_<workload>_<数据量标签>_t<线程数>`。脚本遇到已经完整成功的同名结果时会跳过；遇到不完整或参数不匹配的同名目录时会保留目录并停止，不会覆盖或删除已有结果。任一 RunUnit 失败时整套执行立即停止。

不要同时运行其他 Benchmark；并发运行会争抢 CPU、内存和磁盘，使结果失真。
