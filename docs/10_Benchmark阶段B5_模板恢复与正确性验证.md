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
