# IndexOnlyDelete 优化阶段执行文档

## 1. 文档定位与规范优先级

本文档是 `delete-optimize` 分支的固定任务模板，只定义 OPT-1～OPT-5 的实施顺序、文件边界、测试与验收门禁。它不得替代或修改设计。

唯一实现规范，按同级并列待满足：

1. `/Users/Admin/work/kv/rustkv/docs/系统设计文档_v3.md`；
2. `/Users/Admin/work/kv/rustkv/docs/RustKV-复合提交与崩溃恢复协议_v3.md`。

文档定稿时 SHA-256：

| 规范 | SHA-256 |
|---|---|
| `系统设计文档_v3.md` | `7f577f2945702cfcb461e179a1b6db79870c5f314118130afd19818580ec7bd9` |
| `RustKV-复合提交与崩溃恢复协议_v3.md` | `c69972c00e2340479bce1f04a5d5c0504b3ad4254a80acf6f8a20cb7d9b0f089` |

实现每个阶段前必须验证两份文档存在且 hash 与上表一致。两份 v3 规范无优先级覆盖关系；如果无法同时满足，立即停止并报告用户。

[`05_IndexOnlyDelete优化方案与v2-v3修改清单.md`](./05_IndexOnlyDelete优化方案与v2-v3修改清单.md) 是说明与差异索引，本文档是执行约束；二者都不是设计规范。根目录 v2 文档和现有 `00/01/02/03` 阶段文档只作历史记录，不用于 `delete-optimize` 代码阶段的设计判断。

## 2. 全局执行约束

1. 所有工作只在 `/Users/Admin/work/kv` 范围内进行，项目目录为 `/Users/Admin/work/kv/rustkv`。
2. 一次只执行一个 OPT 阶段，禁止提前实现后续阶段或把两个阶段混入同一 diff。
3. 必须阅读本阶段【读取章节】列出的章节，并允许跟随两份 v3 规范内部交叉引用；禁止用其他文档补齐、覆盖或改写 v3。
4. 只修改本阶段【实现文件】列出的文件。发现必须扩大范围时立即停止，说明原因、拟增文件和影响，等待用户明确批准。
5. 公共 API 已经确定，不得重新设计或“冻结”；不定义 v3 规范之外的公共类型，不把 Fjall 类型暴露到公共 API。
6. 禁止自行发挥、简化、改变字节布局、调整持久化顺序、放宽 fail-closed 边界、引入未要求依赖或做旧格式兼容。
7. 不得删除、忽略、吞错、降低、改弱或用更宽断言替换现有测试。修改旧 Delete 测试只能把 v2 物理协议断言更新为 v3 确切协议，公共语义断言不变。
8. 每个代码阶段必须先运行定向测试，再运行 `cargo build --locked`，最后运行全量 `cargo test --locked`。任一失败时报告失败原因，在本阶段范围内修复后重跑，直到全部通过；无法在规范内修复时停止。
9. “命令已执行”必须以实际退出码0和输出中0 failed为证据；禁止伪造、概括或隐藏测试结果。
10. 测试全部正确后也不提交。先输出 diff 和测试证据给用户 review；只在用户明确说可以提交后才执行 Git commit。
11. 每个已验收代码阶段对应一个独立提交。提交前重新确认 `git diff` 仅包含本阶段文件、`cargo build --locked` 通过、全量 `cargo test --locked` 通过，且不夹带用户改动。
12. 现有重型10GiB和 Benchmark 测试不在普通全量 `cargo test --locked` 运行时宣称已执行；只有实际单独运行且成功才能报告。

## 3. 统一执行前缀

每次交给实现 AI 的提示必须由以下前缀和本文档对应阶段的完整原文组成：

```text
你现在只执行《06_IndexOnlyDelete优化阶段执行文档.md》中的“OPT-N：<阶段名称>”。

唯一实现规范：
/Users/Admin/work/kv/rustkv/docs/系统设计文档_v3.md
/Users/Admin/work/kv/rustkv/docs/RustKV-复合提交与崩溃恢复协议_v3.md

必须遵守：
1. 校验两份规范的 SHA-256。
2. 必须阅读本阶段【读取章节】列出的章节，并允许跟随两份 v3 规范内部交叉引用；禁止用其他文档覆盖 v3。
3. 只修改本阶段【实现文件】列出的文件。
4. 严格落实【接口契约】，不得重新设计、简化、优化或兼容旧布局。
5. 不定义规范之外的公共类型，不暴露 Fjall 类型，不新增未授权依赖。
6. 不得删除、忽略、降低或改弱测试。
7. 不得开始下一阶段。
8. 出现规范冲突、范围外修改需求或无法实现时立即停止并报告。
9. 验证全部通过后不提交，等待用户 review 和明确提交授权。

执行顺序：
1. 执行 `cd /Users/Admin/work/kv/rustkv`，后续 Git/Cargo 命令均以该目录为 cwd。
2. 确认前置阶段已验收。
3. 检查 git status/diff，保护用户已有改动。
4. 实现并运行本阶段定向测试。
5. 运行 cargo build --locked。
6. 运行全量 cargo test --locked。
7. 按本阶段【输出】格式报告，不执行 commit。

下面是本次唯一任务，禁止改写：
<粘贴对应 OPT 阶段完整原文>
```

## 4. 阶段状态

| 阶段 | 名称 | 前置 | 初始状态 | 验收提交 |
|---|---|---|---|---|
| OPT-1 | v3 元数据格式与内部前沿模型 | 文档提交已验收 | 未开始 | — |
| OPT-2 | Recovery 识别与恢复 IndexOnlyDelete | OPT-1 | 未开始 | — |
| OPT-3 | 生产 Delete 无 VLog 提交与屏障 | OPT-2 | 未开始 | — |
| OPT-4 | 崩溃、重开与并发矩阵 | OPT-3 | 未开始 | — |
| OPT-5 | 现有 workload 手工性能验收 | OPT-4 | 等待用户执行 | 不创建空提交 |

OPT-1～OPT-4 只能在用户完成 review、明确说阶段可验收并授权提交后提交代码。验收提交 hash 无法写入产生它的同一提交；本状态表默认由用户维护，只有用户另行明确授权时，AI 才能以独立文档记录提交更新状态/hash，不得夹带进代码阶段提交。OPT-5 是无代码改动的用户手工检查点，不伪造空提交。

## 5. OPT-1：v3 元数据格式与内部前沿模型

【任务】`metadata-v3-and-frontier-model`

在不开启生产 Delete 优化的前提下，一次性实现 v3 TxMeta、DurableFrontier、RecoveryState 编码与内部 head/durable VLog 前沿模型。本阶段生产 Put、Delete 和所有非空 WriteBatch 暂时仍统一产生 `VLogEnvelope`；该限制用于把格式/状态风险与行为切换风险隔离。

【读取章节】必须阅读以下章节，并允许跟随两份 v3 规范内部交叉引用；禁止用其他文档覆盖 v3：

- 《系统设计文档_v3》：第3.2.1、3.5.1节，第4.5.3节，第5.2.2、5.3～5.6节，第7.1～7.2节，第10.2节；
- 《RustKV-复合提交与崩溃恢复协议_v3》：第2.1、2.4、3.1，第7.2，第8.3节。

【实现文件】

生产代码：

- `src/commit/descriptor.rs`
- `src/commit/mod.rs`
- `src/commit/protocol.rs`
- `src/commit/coordinator.rs`
- `src/index/mod.rs`
- `src/recovery/mod.rs`
- `src/recovery/undo.rs`
- `src/db.rs`

测试文件（只允许更新新字段构造、精确编码和不变量断言）：

- `tests/metadata_format_golden.rs`
- `tests/commit_planning.rs`
- `tests/compound_commit.rs`
- `tests/concurrency_failures.rs`
- `tests/descriptor_cleanup.rs`
- `tests/durability_barrier.rs`
- `tests/get_path.rs`
- `tests/index_backend_contract.rs`
- `tests/index_backend_fjall.rs`
- `tests/open_initialization.rs`
- `tests/public_api.rs`
- `tests/read_corruption.rs`
- `tests/recovery_analysis.rs`
- `tests/recovery_execution.rs`
- `tests/recovery_recrash.rs`
- `tests/snapshot_write_stopped.rs`

【接口契约】

- 新增仅 crate 内可见的 `TransactionKind::{VLogEnvelope=0, IndexOnlyDelete=1}`，TxMeta 必须携带该字段；
- TxMeta v0 精确87字节，字段 offset、小端序、kind 验证和 Descriptor CRC transcript 与第5.4节逐字节一致；
- DurableFrontier v0 精确39字节，增加 `durable_vlog_seq`，完整验证 seq/end 关系；
- RecoveryState v0 精确57字节，增加 `target_vlog_seq`，完整验证 phase/seq/end 关系；
- 86/31/49字节旧值必须稳定拒绝，不增加兼容分支；
- 内存状态增加 `head_vlog_seq`并与 `head_vlog_end`、Frontier 对称；状态只在 Fjall Batch 成功后更新；
- 本阶段所有生产非空事务必须仍为 VLogEnvelope，因而成功后 `head_vlog_seq=head_seq`，屏障后 `durable_vlog_seq=durable_seq`；
- 公共 `DbStats`、公共 API 和 Fjall 隔离边界不变。

【禁止事项】

- 禁止在本阶段让生产 Delete 跳过 VLog；
- 禁止接受旧长度、猜测 kind、从零 VLog 字段推断旧事务或进行数据迁移；
- 禁止更改 VLog 五种 Record、ValuePointer、TxMutation、公共 Stats 或公共错误的字节/接口布局；
- 禁止因大量构造器编译失败而删测试或用 `Default` 隐藏必填新字段。

【测试要求】

- 逐字节断言 TxMeta=87、DurableFrontier=39、RecoveryState=57，验证全部 offset、CRC 覆盖和 encode/decode 对称；
- 对每个字段、kind、tag、seq/end 关系做损坏和非法值测试，旧长度确定拒绝；
- 测试 IndexOnlyDelete TxMeta 的合法全零 VLog 字段、任一非零字段拒绝、未知 kind 拒绝、Descriptor CRC 覆盖 kind；
- 现有 Put/Delete/Batch/屏障/恢复测试在生产行为仍全 VLog 的条件下通过，证明本阶段未提前切换行为。

定向命令：

```bash
cd /Users/Admin/work/kv/rustkv
cargo test --locked --test metadata_format_golden
cargo test --locked --test commit_planning
cargo test --locked --test compound_commit
cargo test --locked --test durability_barrier
cargo test --locked --test recovery_analysis
cargo test --locked --test recovery_execution
cargo test --locked --test recovery_recrash
```

【验收】`cargo build --locked` 无 error；定向测试全部0 failed；`cargo test --locked` 全部通过；Git diff 仅包含本阶段文件；尚未 commit，等待用户 review。

【输出】1) 改动文件清单；2) 关键实现说明，包括三种编码实际长度和行为未切换证据；3) 测试命令、退出码与通过/失败数；4) 如失败，报告原因、修复和重跑结果；5) 明确说明未提交。

## 6. OPT-2：Recovery 识别与恢复 IndexOnlyDelete

【任务】`recovery-index-only-delete`

在生产提交仍不生成 IndexOnlyDelete 的前提下，使 Open/Recovery 完整理解人工构造的 v3 IndexOnlyDelete Descriptor、逻辑/VLog 双前沿、禁止跨洞规则和 IndexOnly Undo。

【读取章节】必须阅读以下章节，并允许跟随两份 v3 规范内部交叉引用；禁止用其他文档覆盖 v3：

- 《系统设计文档_v3》：第5.4～5.6节，第6.1.6节，第8.1～8.9节，第9.2节，第10.4.1节；
- 《RustKV-复合提交与崩溃恢复协议_v3》：第2.1、2.4节，第3.1节，第6.1～6.6节，第8.3节。

【实现文件】

生产代码：

- `src/recovery/mod.rs`
- `src/recovery/undo.rs`
- `src/recovery/topology.rs`
- `src/commit/descriptor.rs`
- `src/db.rs`

测试代码：

- `tests/index_only_delete_recovery.rs`（新增）
- `tests/recovery_analysis.rs`
- `tests/recovery_execution.rs`
- `tests/recovery_recrash.rs`
- `tests/recovery_topology.rs`

【接口契约】

- stable 边界验证使用 `{D,F,E}`；`F=0,E=Empty,D>0` 合法，`F>0` 时 Footer.commit_seq 必须等于 F；
- 先独立完整验证 `D+1..H` 的全部 Descriptor，再以 `C=D,accepted_vlog_seq=F,accepted_end=E` 顺序接受；
- 合法 IndexOnlyDelete 只推进 C，不读 VLog，不推进 accepted VLog seq/end；合法 VLogEnvelope 同时推进三者；
- 第一个无效 VLogEnvelope 终止连续接受，后续 IndexOnlyDelete 不得跨过该缺口；
- RecoveryPlan/RecoveryState 携带 `target_vlog_seq`，已有 State 固定 target，不重算 C；
- RecoveryState 必须结合当前 `{D,F,E}` 验证 `D<=target_seq<=next_undo_seq<=original_head`、`F<=target_vlog_seq<=target_seq`、seq/end 对应和 Undo phase 的 `head_seq==next_undo_seq`；
- `C+1..H` 从后向前整事务 Undo；IndexOnlyDelete Undo 仅处理 Fjall 索引、Descriptor、HeadSeq 和 RecoveryState，无 VLog I/O；
- 若需创建 RecoveryState 且 accepted VLog 边界超过 F，必须先将已接受前缀持久化到 CE，再以 Fjall SyncAll 创建固定 `{C,CF,CE}` 的 State；同步失败不得 Undo/Trim；
- 不主动同步严格位于 CE 之后的文件；边界文件整文件 sync 顺带落盘的 rejected 尾仍是 orphan，目标 Frontier `{C,CF,CE}` 持久化后才允许 Trim；
- 恢复后内存 `head_seq/head_vlog_seq/head_vlog_end` 从完整 Frontier 初始化。

【禁止事项】

- 禁止在本阶段修改生产 Delete 提交路径；
- 禁止为 IndexOnlyDelete 合成伪 Envelope、伪 Position 或伪 Footer；
- 禁止以后续合法 IndexOnlyDelete 跨过前面无效 VLogEnvelope；
- 禁止先 Trim、主动同步严格位于 CE 后的文件、降低 D 或在 RecoveryState 未清除时开放 Db；不得将边界文件整文件 sync 顺带落盘的 rejected 尾误判为 accepted；
- 禁止用仅内存 Mock 替代真 Fjall/VLog/Open 恢复证据。

【测试要求】

- 人工在真 Fjall 安装 IndexOnlyDelete Descriptor，覆盖空 VLog 的 `D>0,F=0,E=Empty`、VLog 后尾随 IndexOnly、Put/IndexOnly 交替；
- 验证 IndexOnly 接受不调用 VLog Reader/sync，C 前进而 CF/CE 不变；
- 构造残缺 VLogEnvelope 后带完整 IndexOnly Descriptor，断言不跨洞、后缀逆序 Undo、最终 User Index 与目标前缀一致；
- 覆盖 target end 为 Empty 和 Position 的 RecoveryState/Trim，以及 Undo、Frontier、Trim、Finalize 中间状态重开续跑；
- 验证 accepted VLog sync 必须先于 RecoveryState SyncAll；sync 失败时 State 不存在且 User Index/HeadSeq/VLog 物理尾未被 Undo/Trim；State 已存在时目标 VLog 前缀已持久；
- 显式验证 `target_seq<D`、`target_vlog_seq<F`、target seq/end 不匹配、Undo 的 `head_seq!=next_undo_seq`、坏 kind、非零 IndexOnly VLog 字段/CRC、Footer.seq≠F 全部 fail closed。

定向命令：

```bash
cd /Users/Admin/work/kv/rustkv
cargo test --locked --test index_only_delete_recovery
cargo test --locked --test recovery_analysis
cargo test --locked --test recovery_execution
cargo test --locked --test recovery_recrash
cargo test --locked --test recovery_topology
```

【验收】`cargo build --locked` 无 error；定向测试全部0 failed；`cargo test --locked` 全部通过；生产 Delete 仍走 VLogEnvelope；Git diff 仅包含本阶段文件；尚未 commit，等待用户 review。

【输出】1) 改动文件清单；2) 恢复接受、跨洞禁止、Undo、Frontier 和条件 VLog sync 说明；3) 每个真实组件测试的状态构造与断言；4) 测试命令、退出码与通过/失败数；5) 失败、修复与重跑记录；6) 明确说明未提交。

## 7. OPT-3：生产 Delete 无 VLog 提交与屏障

【任务】`production-index-only-delete-commit`

在 OPT-1 格式/状态与 OPT-2 恢复能力已验收的基础上，开启生产单 Delete/全 Delete WriteBatch 的 IndexOnlyDelete 路径，实现无 VLog I/O 和完整 `sync=true` 前缀屏障。

【读取章节】必须阅读以下章节，并允许跟随两份 v3 规范内部交叉引用；禁止用其他文档覆盖 v3：

- 《系统设计文档_v3》：第4.3、4.5.3节，第5.4.4、5.5.4～5.5.5节，第6.1、6.5节，第7.1～7.6节，第10.2、10.4节；
- 《RustKV-复合提交与崩溃恢复协议_v3》：第1.2～1.3节，第2.1～2.4节，第3章，第4～5章，第7.1～7.2、8.2节。

【实现文件】

生产代码：

- `src/commit/protocol.rs`
- `src/commit/coordinator.rs`
- `src/commit/durability.rs`
- `src/commit/mod.rs`
- `src/db.rs`
- `src/fault_injection.rs`
- `src/stats.rs`
- `src/vlog/format.rs`
- `src/vlog/reader.rs`
- `src/vlog/writer.rs`

测试代码（只允许更新 Delete 的 v2 物理协议断言并增强 v3 断言）：

- `tests/commit_planning.rs`
- `tests/commit_preflight.rs`
- `tests/compound_commit.rs`
- `tests/durability_barrier.rs`
- `tests/core_api.rs`
- `tests/core_restart.rs`
- `tests/descriptor_cleanup.rs`
- `tests/get_path.rs`
- `tests/index_only_delete_no_vlog.rs`（新增）
- `tests/public_api.rs`

【接口契约】

- 分类只取决于原始非空操作列表：全 Delete 为 IndexOnlyDelete；任意 Put 为 VLogEnvelope，包括 Put→Delete、Delete→Put和空 Value Put；
- IndexOnlyDelete 不调用 VLog 布局规划、Writer append、文件创建/滚动、dirty 登记、sync 或 Reader；
- 增加仅测试编译可见的 VLog 调用计数或 fail-on-call 探针，分别覆盖布局规划、append/创建与滚动、Reader 解引用、文件 sync 和目录 sync；探针不得暴露公共 API，非测试构建不得增加分支、锁或运行时开销；
- IndexOnlyDelete 仍生成完整 TxMeta、按首次出现顺序去重的 TxMutation、最终 User Index delete 和 HeadSeq，并在同一 Fjall Batch 原子提交；
- `sync=false` 成功只推进 head_seq，head VLog seq/end 不变；
- 非空 `sync=true` 在当前事务准备后计算目标 VLog seq/end：有脏 VLog 先 sync-through，无脏 VLog 零 VLog I/O；随后以一个 SyncAll Batch 提交当前事务与完整 Frontier；
- 空 `sync=true` 捕获 `(S,SF,SE)`，只有 `(SF,SE)` 超过旧 `(F,E)` 才同步 VLog，但 `S>D` 时仍用 SyncAll 推进逻辑 Frontier；
- 公共 Delete/WriteBatch 返回、线性化点、WriteOutcome/错误状态和公共 Stats 字段不变。

【禁止事项】

- 禁止把“最终状态都是 Delete”的含 Put Batch 误归类为 IndexOnlyDelete；
- 禁止为纯 Delete 写 `DELETE_RECORD`、空 Envelope、占位 PAGE_END 或惰性创建 VLog 文件；
- 禁止把重复 Delete 按每次操作持久化到 VLog，也不得丢失原始 `logical_op_count`；
- 禁止为减少 I/O 放弃 `sync=true` 前缀屏障、跳过前方脏 Put VLog 同步或把 Frontier 更新拆分；
- 禁止更改公共 Stats 结构或添加 Delete 专用公共 API。

【测试要求】

- T1：全新库的单 Delete、删除不存在 Key、重复 Key 全 Delete Batch，覆盖 sync false/true；断言没有 VLog 文件和字节，Descriptor/HeadSeq/Frontier 准确；
- T2：在已有 Put VLog 上做删除存在/不存在 Key、覆盖删除和全 Delete Batch，断言 VLog 文件清单、每个文件长度、append cursor、VLog Stats 逐字节不变；
- T3：含 Put 的混合 Batch 覆盖 Put/Delete、Put→Delete、Delete→Put、重复 Key、空 Value；断言仍有完整 Envelope 和原顺序 Record，终态全对；
- T4：`3笔 sync=false Put → sync=true Delete`，断言先同步脏 Put VLog，再提交 Delete，最终 `H=D`且 F/end 停在最后 Put；
- T5：只有 IndexOnlyDelete 的 `sync=false → 空 sync=true`和直接非空 `sync=true` Delete，断言完全没有 VLog I/O，但逻辑 H/D 和 Descriptor cleanup 正确；
- T6：启用 test-only 调用计数/fail-on-call 探针重跑 T1、T2 的纯 Delete 和 T5 的两类屏障。测量窗口精确从单次 Delete/WriteBatch/屏障调用前开始，到该调用返回后结束；在之后的终态 `ValueLogReader` 校验前必须先快照并关闭/重置探针，不得把验收读本身计入提交路径。每个测量窗口逐类断言 VLog 布局规划、Writer append/create/roll、Reader、文件 sync、目录 sync 调用数均为0；再用含 Put 的独立正向对照证明探针确实能记录/拦截相应调用，禁止只用文件长度、Stats 或最终状态代替零调用证明；
- 所有数据终态用真 Fjall User Index 和 `FileSet + ValueLogReader` 读取 Put Value，不手工切片 VLog；Delete Key 断言索引无 Key。

定向命令：

```bash
cd /Users/Admin/work/kv/rustkv
cargo test --locked --test commit_planning
cargo test --locked --test compound_commit
cargo test --locked --test durability_barrier
cargo test --locked --test core_api
cargo test --locked --test core_restart
cargo test --locked --test descriptor_cleanup
cargo test --locked --test index_only_delete_no_vlog
cargo test --locked --test public_api
```

【验收】`cargo build --locked` 无 error；定向测试全部0 failed；`cargo test --locked` 全部通过；旧 Delete 测试已更新为 v3 且未弱化公共语义；Git diff 仅包含本阶段文件；尚未 commit，等待用户 review。

【输出】1) 改动文件清单；2) 分类、提交、屏障和运行时前沿说明；3) 旧 Delete 断言变更对照；4) T1～T6 的真实组件、VLog 零增量及调用计数/fail-on-call 零调用证据；5) 测试命令、退出码与通过/失败数；6) 失败、修复与重跑记录；7) 明确说明未提交。

## 8. OPT-4：崩溃、重开与并发矩阵

【任务】`index-only-delete-crash-and-concurrency`

为已开启的 IndexOnlyDelete 生产路径补齐确定性故障注入、真实重开、子进程 SIGKILL、Recovery 再次崩溃和并发历史证据。

【读取章节】必须阅读以下章节，并允许跟随两份 v3 规范内部交叉引用；禁止用其他文档覆盖 v3：

- 《系统设计文档_v3》：第3.2节，第4.6节，第5.4～5.6节，第7.1～7.6节，第8章，第9.2节，第10.1、10.4、10.4.1、10.5、10.6、10.7、10.9节；
- 《RustKV-复合提交与崩溃恢复协议_v3》：第1.3～1.4节，第3章，第4～7章，第8.2～8.3节。

【实现文件】

故障注入所需的生产文件（只允许增加私有 test-only hook 及为其做最小接线）：

- `src/fault_injection.rs`
- `src/commit/descriptor.rs`
- `src/commit/protocol.rs`
- `src/commit/coordinator.rs`
- `src/commit/durability.rs`
- `src/recovery/mod.rs`
- `src/recovery/undo.rs`
- `src/recovery/topology.rs`
- `src/db.rs`
- `src/index/fjall.rs`
- `src/runtime/state.rs`
- `src/runtime/write_gate.rs`
- `src/vlog/writer.rs`

测试代码：

- `tests/index_only_delete_crash.rs`（新增）
- `tests/core_crash.rs`
- `tests/core_restart.rs`
- `tests/recovery_analysis.rs`
- `tests/recovery_execution.rs`
- `tests/recovery_recrash.rs`
- `tests/concurrency_writes.rs`
- `tests/concurrency_history.rs`
- `tests/concurrency_reads.rs`
- `tests/concurrency_snapshots.rs`
- `tests/concurrency_failures.rs`
- `tests/durability_barrier.rs`
- `tests/compound_commit.rs`

【接口契约】

- 故障注入只是私有测试能力，不增加公共 API，不改变无注入时的调用次数、顺序或持久化语义；
- IndexOnlyDelete Fjall Batch 在崩溃后只能整体存在或不存在，不得出现 User Index/Descriptor/HeadSeq 交叉状态；
- `sync=true` 严格保留前缀屏障：有脏 Put 先 VLog sync，无脏 VLog 不产生 VLog I/O，Frontier 三元组原子；
- Recovery 只接受连续逻辑前缀，IndexOnlyDelete 不跨过无效 VLogEnvelope，Undo/Frontier/Trim/Finalize 在任一再次崩溃点后幂等收敛；
- 并发 Put、Delete、WriteBatch、空/非空屏障仍形成一个满足线程内顺序和实时顺序的提交全序，无死锁或部分 Batch；
- SIGKILL 测试只报告进程崩溃证据，不标记为 OS 真实掉电/L4 验收。
- 当前阶段的崩溃环境限定为 L3 子进程 SIGKILL；对“accepted VLog 先同步、后持久化 RecoveryState”使用 L1 精确调用顺序/failpoint 证据加 L3 同步前后 SIGKILL 证据。丢失 OS 页缓存的 L4 用例暂缓，不属于 OPT-4 当前验收结论。

【禁止事项】

- 禁止通过正常 shutdown、Drop 或事后补 sync 代替强制终止；
- 禁止只测 Mock、只测编码或只测无故障成功路径却声称完成崩溃恢复；
- 禁止把预期 `CommitUnknown` 当成可自动重放的 `NotCommitted`；
- 禁止为了让重开成功而降低 DurableFrontier、忽略 Descriptor 损坏、重建数据库或接受非连续前缀；
- 若故障测试发现必须改变生产协议行为，必须停止、报告缺陷和拟改文件，等待用户扩大范围；不得以“测试接线”名义顺带修改生产语义；
- 禁止实际运行 OPT-5 重型 Benchmark 脚本。

【测试要求】

每个适用场景均断言公共终态、User Index、所有 Put Pointer 经 `FileSet + ValueLogReader` 读回、Descriptor 数量/内容、H/D/F/end、脏账、文件清单/长度和重复 Open 收敛。必须覆盖：

1. 全新库单 Delete、删不存在 Key、重复 Key 全 Delete Batch；屏障后 `H=D>0,F=0,E=Empty`且没有 VLog 文件。
2. 稳定 Put 后尾随多个 IndexOnlyDelete；D 前进而 F/end 留在最后 Put。
3. Put 与 IndexOnlyDelete 交替；空屏障和非空 `sync=true` Delete 保留前缀语义。
4. `sync=false` IndexOnlyDelete 的 Fjall Buffer 调用前、明确未应用、可能应用和成功后 SIGKILL；重开只得到整事务旧或新状态。
5. 无脏 VLog 的 `sync=true` IndexOnlyDelete 在 Fjall SyncAll 前、可能应用和成功后崩溃；不得有 VLog I/O。
6. 前方有脏 Put 的 `sync=true` IndexOnlyDelete：VLog 同步前、同步后但 Delete Fjall commit 前、SyncAll 未应用/未知/已应用。
7. 已接受 IndexOnlyDelete 后存在残缺 VLog 物理尾；分别覆盖 target end 为 Empty 和 Position 的 Trim。
8. 无效 VLogEnvelope 之后存在完整 IndexOnlyDelete Descriptor；禁止跨洞接受，后缀按 `H..C+1` 逆序 Undo。
9. accepted VLog 前置同步失败/成功、同步成功后但 RecoveryState SyncAll 前 SIGKILL、RecoveryState SyncAll 未应用/可能应用/成功后，以及每个 IndexOnlyDelete Undo Batch、目标 Frontier、Trim、Finalize 的提交前/可能应用/成功后再次崩溃；反复 Open 幂等收敛，并证明 State 不会在其引用的新增 accepted VLog 持久化前提交。
10. 非法 kind、IndexOnlyDelete 非零 VLog 字段/CRC、非法 Frontier/RecoveryState seq/end、Footer.commit_seq≠durable_vlog_seq；全部 fail closed。
11. L1 确定性故障注入、L2 真 Fjall+真 VLog+真重开、L3 子进程 SIGKILL+父进程反复 Open 三层证据均必须存在。L4 OS 崩溃/掉电和页缓存丢失测试明确标记“暂缓”，不得伪装为 SIGKILL 已覆盖。
12. 并发 VLogEnvelope/IndexOnlyDelete 交替，以及 Delete 与空/非空 sync 屏障交错；commit_seq 连续、前沿和 VLog 零增量正确、历史可线性化。

定向命令：

```bash
cd /Users/Admin/work/kv/rustkv
cargo test --locked --test index_only_delete_crash
cargo test --locked --test core_crash
cargo test --locked --test core_restart
cargo test --locked --test recovery_recrash
cargo test --locked --test concurrency_writes
cargo test --locked --test concurrency_history
cargo test --locked --test concurrency_failures
cargo test --locked --test durability_barrier
```

【验收】`cargo build --locked` 无 error；定向测试全部0 failed；`cargo test --locked` 全部通过；L1/L2/L3、本文列出的 SIGKILL 强制崩溃矩阵和并发矩阵都有可审查的实际证据；L4 明确暂缓且未宣称通过；Git diff 仅包含本阶段文件；尚未 commit，等待用户 review。

【输出】1) 改动文件清单；2) 故障点→预期 WriteOutcome/重开结果映射；3) 12类强制场景与 L1/L2/L3 证据对照；4) 并发历史和终态断言；5) 测试命令、退出码与通过/失败数；6) 失败、修复与重跑记录；7) 明确说明未提交；8) 明确说明 OPT-5 脚本未运行。

## 9. OPT-5：现有 workload 手工性能验收

【任务】`manual-performance-acceptance`

这是 OPT-1～OPT-4 全部已 review、已验收且已独立提交后的用户手工检查点，不是 AI 代码任务。

【读取章节】用户执行时参考《系统设计文档_v3》第10.8～10.9节和现有 `/Users/Admin/work/kv/benchmark_plan.md`；AI 不在本阶段自行读取并执行。

【实现文件】无。不修改 RustKV、Benchmark、脚本、配置、原结果数据或文档；不创建空 Git commit。

【接口契约】由用户自行确认位于 `delete-optimize` 分支，调用现有 `/Users/Admin/work/kv/rustkv/benchmarks/scripts/run_all_workload.sh`，并与 main 分支已有结果数据比较。保留精确 commit、命令、机器/系统环境、运行日志、原始结果和对比摘要。

【禁止事项】AI 禁止自行切换分支、启动脚本、编造性能结果、改变 workload/参数/统计口径、覆盖 main 原数据，或声称未实际执行的组合已通过。

【测试要求】脚本自带的全 workload 组合按现有口径运行；对比 main 已保存结果。不设固定提升百分比。此前 OPT-3/OPT-4 已通过的纯 Delete VLog 零增量、数据正确性、前缀屏障和崩溃恢复是不可被性能数字取代的硬门禁。

【验收】由用户确认脚本实际完成、无未处理错误、原始结果完整，并完成与 main 结果的对比。不要求也不创建空 Git commit。

【输出】用户自行保留：1) `delete-optimize` 精确 commit；2) 实际命令与退出码；3) 完整结果目录；4) 与 main 的同口径对比摘要；5) 异常、失败或无法比较的组合及原因。

## 10. Git 提交门禁

用户完成阶段 review 并明确授权提交后，才执行：

```bash
cd /Users/Admin/work/kv/rustkv
git status --short
git diff -- <本阶段全部允许文件>
cargo build --locked
cargo test --locked
```

提交必须满足：

- diff 仅有本阶段列出的文件，不夹带用户改动；
- 构建退出码0，全量测试0 failed；
- 本阶段新增测试、旧回归和两个 Fjall spike 都在全量测试中通过；
- 每个阶段一个提交，统一信息：`opt stage <N>: <阶段名称>`；
- 提交后报告 commit hash；状态表默认由用户维护。只有用户另行明确授权时，AI 才能在后续独立文档记录提交中更新状态/hash，不得将不可预知的当前 commit hash 夹带进产生它的同一提交。
