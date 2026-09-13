# IndexOnlyDelete 优化方案与 v2→v3 修改清单

## 1. 文档定位

本文档记录 `delete-optimize` 分支的优化目标、已确认决策、v2→v3 差异和实施边界，便于审查与 Git 跟踪。它不是第三份设计规范，也不得改写两份 v3 规范的含义。

`delete-optimize` 分支的唯一实现规范是：

1. [`系统设计文档_v3.md`](./系统设计文档_v3.md)；
2. [`RustKV-复合提交与崩溃恢复协议_v3.md`](./RustKV-复合提交与崩溃恢复协议_v3.md)。

两份 v3 文档为同级规范，应当同时满足。若实现者发现两者相互矛盾、字段无法同时满足或必须超出阶段文件范围，必须停止并报告，不得自行选择、简化或“优化”规范。

根目录下的《系统设计文档_v2.md》和《RustKV-复合提交与崩溃恢复协议.md》是 main 分支的历史基线，保持不变；优化分支不用它们指导代码实现。

## 2. 目标和非目标

### 2.1 目标

- 单条 Delete 和全 Delete 的非空 WriteBatch 不再产生 VLog 字节、文件创建、文件滚动或 VLog 同步负担；
- Delete 仍是完整逻辑事务，保留提交全序、WriteBatch 原子性、`sync` 持久化语义、Descriptor 证据和崩溃恢复能力；
- 使逻辑持久前沿与最后一个 VLog 事务的物理前沿显式分离，消除“事务 `D` 必有 VLog Footer”的隐含前提；
- 在开启生产优化前先完成新格式、新运行时状态和恢复读路径，保证阶段可回归。

### 2.2 非目标

- 不兼容、迁移或自动识别旧 v0 内部布局；
- 不改变公共 Rust API，不新增公共 Stats 字段；
- 不改变任何含 Put 的 WriteBatch 的完整 Envelope 编码；
- 不实现 VLog GC、历史 Value 回收、通用事务或新的 Benchmark 框架；
- 不以固定性能提升百分比作为协议正确性门禁；
- SIGKILL 证据不宣称等同真实断电或 L4 掉电证明。

## 3. 已确认的事务分类

| 用户请求 | `transaction_kind` | VLog | Descriptor | Fjall 发布 |
|---|---|---|---|---|
| Put，包括空 Value | `VLogEnvelope` | 完整 Envelope | 完整 | User Index + Descriptor + HeadSeq |
| 含至少一个 Put 的 WriteBatch | `VLogEnvelope` | 按原操作顺序写全部 `KV_RECORD/DELETE_RECORD` | 按不同 Key 合并 | User Index + Descriptor + HeadSeq |
| 单条 Delete | `IndexOnlyDelete` | 无 | 完整，包含 `Absent→Absent` | User Index delete + Descriptor + HeadSeq |
| 非空全 Delete WriteBatch | `IndexOnlyDelete` | 无；重复 Delete 不按操作写物理记录 | 首次出现顺序去重 | 最终 User Index deletes + Descriptor + HeadSeq |
| 空 `sync=false` WriteBatch | 无事务 | 无 | 无 | 无 |
| 空 `sync=true` WriteBatch | 无事务 | 只有已覆盖前缀中存在脏 VLog 时同步 | 无新 Descriptor | 必要时推进完整 DurableFrontier |

非空 `IndexOnlyDelete` 仍必须分配连续 `commit_seq`和非零、不复用 `tx_uuid`。`logical_op_count` 是原始 Delete 操作数，`distinct_key_count` 是按 User Key 字节内容去重后的数量，TxMutation ordinal 按 Key 首次出现顺序分配，且每个 `after_state` 都是 `Absent`。

## 4. 内部格式与状态模型变更

### 4.1 TxMeta v0

TxMeta v0 直接从86字节重写为87字节，在 offset 6 新增私有 `transaction_kind:u8`，其后字段整体后移1字节。

- `0=VLogEnvelope`；
- `1=IndexOnlyDelete`；
- 其他值非法；
- `IndexOnlyDelete` 的 begin/end 四个数值字段和 `envelope_crc32c` 全为0；
- `descriptor_crc32c` 位于 offset 83，计算时覆盖 TxMeta `[0..83]`，因而包含 kind；
- 不接受86字节旧布局。

### 4.2 DurableFrontier v0

DurableFrontier v0 从31字节重写为39字节：

```text
{ durable_seq, durable_vlog_seq, durable_vlog_end }
```

`durable_seq=D` 是已持久的逻辑事务前缀；`durable_vlog_seq=F` 是其中最后一个 `VLogEnvelope` 的序号；`durable_vlog_end=E` 是该事务 Footer 后的物理终点。必须始终满足：

```text
0 <= F <= D <= H
F == 0  <=> E == Empty
F > 0   <=> E == Position(...)
```

因此 `D>0,F=0,E=Empty` 是合法状态，表示已持久前缀全部为 IndexOnlyDelete。不接受31字节旧布局。

### 4.3 RecoveryState v0

RecoveryState v0 从49字节重写为57字节，新增 `target_vlog_seq`：

```text
{ phase, original_head, target_seq, target_vlog_seq,
  target_vlog_end, next_undo_seq, trim_required }
```

必须结合当前 DurableFrontier `{D,F,E}` 满足 `D <= target_seq <= next_undo_seq <= original_head`、`F <= target_vlog_seq <= target_seq` 以及 `target_vlog_seq=0 <=> target_vlog_end=Empty`。`target_vlog_seq==F` 时 target end 必须等于 E；大于 F 时 target end 必须严格前进、指向合法 Footer 后边界，并在 RecoveryState 创建前已持久化。Undo phase 还必须有 `head_seq==next_undo_seq`。已存在 RecoveryState 时只能继续固定 target，不得重算或猜测新目标。不接受49字节旧布局。

### 4.4 运行时状态

内存提交状态对称地跟踪：

```text
head_seq
head_vlog_seq
head_vlog_end
durable_seq
durable_vlog_seq
durable_vlog_end
```

`VLogEnvelope` 成功后同时推进 head 的逻辑序号和 VLog 序号/位置；`IndexOnlyDelete` 只推进 `head_seq`。公共 `DbStats` 不增加 `head_vlog_seq/durable_vlog_seq`，但允许 `durable_seq>0` 时公共 `durable_vlog_end=None`。纯 Delete 不得改变 VLog 文件数、逻辑字节数或活动文件。

## 5. 提交、屏障与 VLog I/O

### 5.1 `sync=false`

- `VLogEnvelope`：保持现有完整 Envelope 追加，再以 Fjall Buffer Batch 原子发布；
- `IndexOnlyDelete`：不调用 VLog 布局规划器、Writer 或同步器，直接以 Fjall Buffer Batch 原子发布 User Index delete、完整 Descriptor 和 `head_seq`；
- 两者都不修改 DurableFrontier。

### 5.2 非空 `sync=true`

`sync=true` IndexOnlyDelete 仍是完整的前缀屏障：

1. 在 `frontier_mutex -> commit coordinator` 固定顺序下准备当前事务；
2. 目标 VLog 边界必须是当前事务准备后前缀中最后 VLogEnvelope 的 seq/end；
3. 前方存在脏 Put VLog 时，必须先同步它；当前是 VLogEnvelope 时目标包含它的新末端；
4. 前缀没有新 VLog 时，不得创建、打开、写入或同步 VLog；
5. 用一个 Fjall SyncAll Batch 同时发布当前事务和完整 `{T,last_vlog_seq(T),last_vlog_end(T)}`。

如果前方脏 VLog 同步失败，当前 Delete 尚未进入 Fjall 提交，返回 `NotCommitted`；一旦 Fjall SyncAll 进入可能生效阶段，按《系统设计文档_v3》第4.6节返回 `CommitUnknown`。

### 5.3 空 `sync=true`

空屏障捕获 `(S,SF,SE)=(head_seq,head_vlog_seq,head_vlog_end)`。`S==D` 时零 I/O 成功；`S>D` 时，只有 `(SF,SE)` 超过 `(F,E)` 才同步 VLog，随后用一个 SyncAll Batch 推进完整 `{S,SF,SE}`。不允许把 Fjall 屏障和 Frontier 更新拆成两个 Batch。

## 6. 崩溃恢复规则

### 6.1 stable prefix

读取 `{D,F,E}` 和 `H`，验证 `0<=F<=D<=H`。`F=0` 时要求 `E=Empty`，不要求 `D=0`；`F>0` 时从 `E` 反向定位并验证事务 `F` 的 Footer 和完整 Envelope。stable 边界不依赖可能已 cleanup 的 Descriptor。

### 6.2 unstable 连续接受前缀

1. 先完整验证 `D+1..H` 的全部 Descriptor，包括 kind、CRC、序号、计数、ordinal、Before/AfterState 和 Pointer 结构；
2. 以 `C=D, CF=F, CE=E` 开始顺序处理；
3. `IndexOnlyDelete` 在 Descriptor 合法且前面无缺口时，依据 Fjall 复合 Batch 原子性接受：只令 `C=i`，`CF/CE` 不变；
4. `VLogEnvelope` 必须从当前 `CE` 开始并通过完整 Envelope/Pointer 验证：令 `C=i,CF=i,CE=end(i)`；
5. 第一个无效或不完整 VLogEnvelope 使连续接受停止，其后的 IndexOnlyDelete 不得跨过逻辑序号缺口。

### 6.3 Undo、Frontier 与 Trim

- `C+1..H` 必须按 `H..C+1` 逐事务逆序 Undo；IndexOnlyDelete 的 Undo 只恢复 BeforeState、删除 Descriptor 并回退 HeadSeq/RecoveryState，不读写、同步或 Trim VLog；
- 当需要 RecoveryState 且 `CF>F` 时，必须先将已接受 VLog 到 `CE` 持久化，再使用 Fjall SyncAll 创建引用 `{C,CF,CE}` 的 RecoveryState；任一前置同步失败时不执行 Undo/Trim；
- 恢复不主动同步严格位于 `CE` 之后的文件。由于 sync 是整文件操作，边界文件中 CE 后的 rejected 尾可能顺带落盘，但仍是不得复活且必须 Trim 的 orphan；
- 目标 Frontier 始终为 `{C,CF,CE}`，RecoveryState 中对称保存 `target_vlog_seq=CF`；存在 State 的路径不依赖 Undo 后才执行的延后 VLog sync；
- 先完成索引 Undo 和目标 Frontier SyncAll，再 Trim `CE` 之后的已证明未提交物理后缀；
- 已有 RecoveryState 时按其 phase 幂等续跑，不重新选择 C；RecoveryState 清除前不开放公共 API。

## 7. 必须的正确性证据

### 7.1 格式与单元层

- 87/39/57字节逐字节 Golden Tests，CRC 覆盖区间精确；
- 86/31/49字节旧布局一律拒绝；
- 未知 kind、IndexOnlyDelete 非零 VLog 字段/CRC、Frontier/RecoveryState 序号与 end 非法组合全部 fail closed；
- 重复 Delete 的原始计数、去重计数、ordinal、BeforeState 和 `Absent→Absent` 准确。

### 7.2 真实组件端到端

- 全新库单 Delete、删除不存在 Key、重复 Key 全 Delete Batch，提交与屏障后均不创建/增长 VLog；
- 已有 VLog 上覆盖删除、删后再写、含 Put 混合 Batch，最终索引和 `ValueLogReader` 读回完全正确；
- `sync=false` 纯 Delete、无脏 VLog 的 `sync=true` Delete、前方有脏 Put 的 `sync=true` Delete、空 `sync=true` 屏障，每步 H/D/F/end 和脏账均正确；
- 实际 Fjall 复合提交、实际 VLog 文件、`FileSet + ValueLogReader` 和真实重开路径联合验证。

### 7.3 崩溃恢复边界

必须同时提供：

- L1：确定性故障注入；
- L2：真 Fjall + 真 VLog + 真重开；
- L3：子进程 SIGKILL + 父进程反复 Open 验证收敛。

强制场景是：全 IndexOnly 空 VLog 前缀、Put 后尾随 IndexOnly、Put/Delete 交替、Buffer 提交前/未应用/可能应用/成功后崩溃、无脏与有脏 VLog 的 SyncAll、已接受 IndexOnly 后残缺物理尾、无效 Envelope 后的 IndexOnly 跨洞禁止、IndexOnly Undo、accepted VLog 前置同步与 RecoveryState 创建的顺序故障点、RecoveryState/Frontier/Trim/Finalize 期间再次崩溃、非法格式 fail closed。当前 OPT-4 以 L1 确定性顺序证据、L2 真重开和 L3 子进程 SIGKILL 为验收边界；SIGKILL 不能模拟丢失 OS 页缓存，L4 OS 崩溃/掉电验证暂缓，不得宣称已通过。精确矩阵以《系统设计文档_v3》第10.4.1节为准。

## 8. v2→v3 修改清单

### 8.1 《系统设计文档_v3》

| 章节 | 修改内容 |
|---|---|
| 第1～3章 | 改为按 kind 可选 VLog Prepare；明确优化分支唯一规范和不兼容旧库 |
| 第4.5.3节 | 公共 Stats 不增字段，但允许逻辑前沿非零而 VLog end 为 None |
| 第5.4节 | TxMeta 新增私有 kind，重写87字节布局和 Descriptor CRC transcript；规定 IndexOnly mutation |
| 第5.5节 | DurableFrontier 增加 `durable_vlog_seq`，重写39字节布局、不变量和运行时更新规则 |
| 第5.6节 | RecoveryState 增加 `target_vlog_seq`，重写57字节布局和 phase 不变量；固定新增 VLog 目标前必须先持久化 accepted 前缀 |
| 第6章 | Envelope 仅适用 Put/含Put Batch；IndexOnlyDelete 不产生物理记录或消耗 cursor |
| 第7章 | 新增事务分类、IndexOnly 提交、head VLog 状态和有条件 VLog 屏障；保留纯 Delete 前缀屏障 |
| 第8～9章 | 恢复转为 `D/F/E/H/C/CF/CE`；引入按 kind 接受、禁止跨洞、IndexOnly Undo、RecoveryState 创建前 accepted VLog sync 和边界文件 rejected 尾 Trim 规则 |
| 第10章 | 更新 Golden/API/故障/并发矩阵，新增 IndexOnlyDelete 强制崩溃恢复矩阵与 OPT-5 手工性能对比边界 |

### 8.2 《RustKV-复合提交与崩溃恢复协议_v3》

| 章节 | 修改内容 |
|---|---|
| 第1章 | 总体方案改为可选 Envelope，DurableFrontier 为逻辑/VLog 三元组 |
| 第2章 | 新增 F、head/durable VLog 含义、transaction_kind、IndexOnly Descriptor 和全零 VLog 字段约束 |
| 第3章 | 核心不变量和线性化证明改为按 kind 分支 |
| 第4～5章 | 正常提交与空屏障转为必要时 VLog sync，明确 `sync=true` 纯 Delete 语义 |
| 第6章 | stable 边界验证改为 Footer.seq=F，接受前缀和 RecoveryState/Undo/Trim 增加 VLog seq 维度；新增 accepted VLog 先持久化、RecoveryState 后固定目标的崩溃安全顺序 |
| 第7～8章 | 错误、Stats、故障点和共同断言增加 IndexOnlyDelete 边界；明确所有参数已由系统设计 v3 确定 |

## 9. 实施、审查与 Git 顺序

1. 先在 `delete-optimize` 分支上完成并 review 本次四份文档；本次文档独立提交，不包含生产代码；
2. 按 [`06_IndexOnlyDelete优化阶段执行文档.md`](./06_IndexOnlyDelete优化阶段执行文档.md) 严格一次只执行 OPT-1、OPT-2、OPT-3、OPT-4 中的一个阶段；
3. 每个代码阶段完成定向测试、`cargo build --locked`和全量 `cargo test --locked` 后仍不提交，先等待用户 review；
4. 用户明确验收后，再确认 diff 仅包含本阶段文件并创建唯一独立提交；
5. OPT-1 先改格式和内部状态，但生产提交暂时统一生成 VLogEnvelope；OPT-2 先让 Recovery 理解人工构造的 IndexOnlyDelete；OPT-3 才开启生产 Delete 无 VLog；OPT-4 补齐崩溃、重开与并发证据；
6. OPT-5 是用户执行的性能验收检查点，不是 AI 代码修改阶段，不创建空 Git 提交。

## 10. OPT-5 性能验收边界

OPT-1～OPT-4 全部验收后，由用户自行切换/确认位于 `delete-optimize` 分支，调用现有：

```bash
cd /Users/Admin/work/kv/rustkv
/Users/Admin/work/kv/rustkv/benchmarks/scripts/run_all_workload.sh
```

与 main 分支已保存的原结果数据比较。AI 不自行切换分支、不自行启动该重型脚本、不改写 Benchmark 口径。结果报告应保留精确 commit、命令、机器/系统环境、原始数据与 main 对照。

性能不设固定提升百分比。硬性协议验收是纯 Delete 的 VLog 文件/字节增量为0，同时公共行为、最终数据、前缀屏障和崩溃恢复全部正确。
