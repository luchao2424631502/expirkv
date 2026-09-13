# RustKV：VLog + Fjall 复合提交与崩溃恢复协议 v3

> 文档版本：v3
>
> 文档状态：`delete-optimize` 分支协议基线
>
> 同级唯一规范：[`系统设计文档_v3.md`](./系统设计文档_v3.md)
>
> 历史背景：[`../../需求分析文档_v2.md`](../../需求分析文档_v2.md)
>
> 历史错误状态背景（非规范）：[`../../RustKV-存储错误状态机与重试协议.md`](../../RustKV-存储错误状态机与重试协议.md)

本文档是根目录 v2 协议的不兼容后继版。根目录原文保持不变；`delete-optimize` 分支的实现只以本文档和《系统设计文档_v3》为规范。本版直接重写现有 v0 内部编码，不识别、迁移或兼容任何旧布局数据库。

## 1. 协议概述

### 1.1 目标与范围

RustKV 将 Put 的完整 User Key 和 User Value 写入 Append-Only Value Log（VLog），Fjall User Index 只保存 `UserKey -> ValuePointer`。单条 Delete 和全 Delete 的非空 WriteBatch 不写 VLog，但仍通过 Fjall User Index、完整 descriptor 和 `head_seq` 的复合原子 Batch 形成 `IndexOnlyDelete` 逻辑事务。只要 WriteBatch 含有任意 Put，整个 Batch 仍写现有完整 Prepare Envelope，其中 Delete 仍编码为 `DELETE_RECORD`。

本协议规定：

- Put、IndexOnlyDelete 和含 Put WriteBatch 的复合提交；
- `sync=false` 与 `sync=true` 的提交和持久化语义；
- 进程崩溃、OS 崩溃和掉电后的连续前缀恢复；
- VLog 孤儿、残缺尾部、悬空 Pointer 和未稳定索引后缀的处理；
- `Ok`、`NotCommitted`、`CommitUnknown` 的判定；
- 恢复过程再次崩溃时的幂等续跑。

错误后的实例状态、读能力、重试规则和 `REC-TOPO-*` 拓扑异常由同级规范《系统设计文档_v3》第4.6、8～10章确定。历史错误状态文档只用于背景追溯，不得补充、覆盖或改写两份 v3 规范。所有拓扑处理都必须遵守以下边界：只自动处理可证明未提交的连续物理后缀，其余情况 fail closed。

本协议不为 WriteBatch 增加用户可配置的最大操作数、最大总字节数或提交超时。编码可表示性、整数溢出、fallible allocation 和资源耗尽仍必须安全处理，且不得静默截断 Batch。

VLog GC、Pointer 重写、文件号回收、Repair、备份恢复和公共全量索引重建不属于本协议。

### 1.2 总体方案

协议采用：

> **可选 VLog Prepare Envelope + Fjall 原子事务 descriptor + DurableFrontier + before-image 后缀逆序回滚**

```text
分配 commit_seq 和 tx_uuid，确定 transaction_kind
        |
        +-- VLogEnvelope: 向 VLog 追加完整 Prepare Envelope
        |
        +-- IndexOnlyDelete: 不访问 VLog
        |
        v
Fjall 原子发布 User Index + 完整 descriptor + head_seq
        |
        +-- sync=false: Buffer 成功后返回 Ok
        |
        +-- sync=true : 必要时先同步已覆盖的 VLog，再以 SyncAll 同批推进 DurableFrontier，
                        成功后返回 Ok
```

DurableFrontier 固定为：

```text
DurableFrontier = {
  durable_seq,
  durable_vlog_seq,
  durable_vlog_end
}
```

`durable_seq` 表示已持久化的逻辑事务前缀；`durable_vlog_seq/durable_vlog_end` 表示该前缀中最后一个 `VLogEnvelope` 事务及其物理终点。尾随 `IndexOnlyDelete` 只推进 `durable_seq`，不推进 VLog 字段。

本协议不采用跨事务 `durable_chain_digest`。每条物理记录的 CRC 和每个 VLogEnvelope 自己的 `envelope_crc32c` 必须保留；IndexOnlyDelete 的 `envelope_crc32c` 固定为0。

### 1.3 外部一致性承诺

- 每个 Put、Delete 和 WriteBatch 都是一个逻辑写事务；
- 读取者只能观察事务提交前或完整提交后的状态，WriteBatch 不得部分可见；
- 同一 WriteBatch 多次操作同一 User Key 时，结果等价于按加入顺序执行，最后一次操作决定最终状态；
- 所有并发写形成满足线程内程序顺序和不重叠调用实时顺序的单一提交全序；
- Put、Delete 和 WriteBatch 使用相同的 `WriteOptions` 持久化语义；
- 任一可见 ValuePointer 必须指向存在、完整、校验通过且 User Key 匹配的 VLog 记录；
- 恢复结果必须是提交全序的连续前缀，不得出现部分 Batch、悬空 Pointer 或孤儿复活。

协议级写入结果只有：

```text
Ok | NotCommitted | CommitUnknown
```

- `Ok`：能够证明事务已经完整提交；
- `NotCommitted`：能够证明请求现在及恢复后都不会产生用户可见效果；
- `CommitUnknown`：Fjall 原子 Batch 已进入可能生效阶段，当前进程不能证明重开后事务整体存在还是整体不存在，但绝不允许部分存在。

结果由失败所处的协议阶段决定，不由 `errno` 名称直接决定。

| 调用结果 | 当前实例 | DB 进程崩溃后重开 | OS 崩溃或掉电后重开 |
|---|---|---|---|
| `Ok, sync=false` | 完整可见 | 必须完整存在 | 可以丢失，但只能整体丢失最近提交后缀 |
| `Ok, sync=true` | 完整可见 | 必须完整存在 | 必须完整存在 |
| `NotCommitted` | 不可见 | 不可见 | 不可见 |
| `CommitUnknown` | 调用方不得假定 | 整体存在或整体不存在 | 整体存在或整体不存在 |

`sync=true` 成功构成提交全序上的持久化屏障，本事务及其之前已经成功提交的 `sync=false` 事务都必须在所有支持的崩溃模型后恢复。写入返回 `Ok` 后，不得再执行能够改变该请求提交或持久化结论的步骤。

### 1.4 依赖契约与故障模型

RustKV 必须针对锁定的 Fjall 版本验证：

1. `OwnedWriteBatch` 可以跨 Keyspace 原子提交；
2. User Index、descriptor 和 `head_seq` 在同一 Batch 中要么全部恢复，要么全部不存在；
3. `PersistMode::Buffer` 成功表示写入已进入 OS Buffer，但不承诺 OS 崩溃或掉电后的持久性；
4. `PersistMode::SyncAll` 成功会稳定当前 Batch，并覆盖此前成功的 Buffer 写入；
5. Fjall 崩溃恢复后的原子 Batch 构成提交顺序的一个前缀；
6. Fjall commit 返回错误时，适配层可以区分“可证明未应用”和“可能已经应用”；无法证明时按 `CommitUnknown` 处理；
7. Fjall 持久化错误后，不能绕过其 fail-stop 保护继续写入。

任一契约不成立时，本协议不能原样实现。

文件系统契约如下：

- `write`/`write_all` 成功不等于掉电持久化；
- 不依赖 VLog 与 Fjall 的后台回写顺序；
- 推进 DurableFrontier 前，仅当目标前缀包含超过旧 VLog 边界的新 VLogEnvelope 时，必须先同步对应范围的 VLog；
- 把新建或滚动的 VLog 文件计入 DurableFrontier 前，必须同步文件和必要的父目录元数据；
- 不假设一次物理写、页写或扇区写天然原子；
- 持久化保证只适用于能够兑现本地文件 `fsync`/`SyncAll` 语义的受支持文件系统。

正常崩溃恢复只处理合法持久化前缀和未提交尾部。稳定前缀文件缺失、记录中部损坏、Pointer 指向错误 User Key 等属于物理损坏，必须返回 `Corruption`，不得降低 `durable_seq`、恢复成 `NotFound` 或猜测性回滚。本协议信任成功的同步屏障，不对恶意存储设备或 Byzantine 文件系统提供证明。

## 2. 协议状态与持久化结构

### 2.1 前沿与位置

| 名称 | 含义 |
|---|---|
| `commit_seq` | 全局连续递增的逻辑提交序号；`0` 是空库哨兵 |
| `tx_uuid` | 不复用的事务标识，防止序号重用时发生 ABA 误认 |
| `head_seq`，记为 `H` | Fjall 已原子发布 User Index、descriptor 和 `head_seq` 的最高序号 |
| `head_vlog_seq` | `seq <= H` 中最后一个已成功发布的 `VLogEnvelope` 事务序号；不存在时为0 |
| `head_vlog_end` | `head_vlog_seq` 对应事务的排他物理终点；`head_vlog_seq=0` 时为 Empty |
| `durable_seq`，记为 `D` | Fjall 及其中所有 VLogEnvelope 所需 VLog 均已通过有序持久化屏障的最高连续序号 |
| `durable_vlog_seq`，记为 `F` | `seq <= D` 中最后一个 `VLogEnvelope` 事务序号；不存在时为0 |
| `durable_vlog_end`，记为 `E` | 事务 `F` 的 `TX_PREPARED_END` 之后的排他物理位置；`F=0` 时为 Empty |
| recovery frontier，记为 `C` | Open 时复合验证得到的最长可保留连续事务前沿 |
| stable prefix | `seq <= D` 的已承诺持久前缀 |
| unstable suffix | `D < seq <= H` 的已发布、但未被 DurableFrontier 覆盖的后缀 |
| `physical_tail` | 最后一个受管 VLog 文件的逻辑物理尾 |
| `published_end` | Fjall 已发布前缀中最后一个 VLogEnvelope 的物理末端 |
| `accepted_end` | 恢复接受至 `C` 后，其中最后一个 VLogEnvelope 的排他物理终点 |
| orphan | VLog 中存在，但没有已提交 Fjall 事务状态引用的完整或部分记录 |

正常运行必须满足：

```text
0 <= durable_vlog_seq <= durable_seq <= head_seq
durable_vlog_seq == 0 <=> durable_vlog_end == EMPTY
durable_vlog_seq > 0  <=> durable_vlog_end == Position(...)
```

`head_seq` 只表示 Fjall 已发布状态，不表示 VLog 已经掉电安全。`durable_seq>0` 可以与 `durable_vlog_seq=0,E=EMPTY` 同时成立，表示已持久前缀全部是 IndexOnlyDelete。`durable_vlog_end` 是物理边界，不是内容校验和。

`head_vlog_seq/head_vlog_end` 是 crate 内运行时状态对，新库初始为 `0/Empty`，Open 完成后从 `durable_vlog_seq/durable_vlog_end` 初始化。只有 VLogEnvelope 的 Fjall Batch 成功后才推进它；IndexOnlyDelete 成功后保持不变。`last_vlog_seq(T)/last_vlog_end(T)` 表示事务 T 完成 kind 规划与适用的 VLog 追加后，前缀 `seq <= T` 中最后一个 VLogEnvelope 的该状态对：T 为 VLogEnvelope 时取 `T/vlog_end(T)`，T 为 IndexOnlyDelete 时沿用原 head VLog 状态对。

VLog 位置表示为：

```text
VLogPos { file_id, offset }
```

按 `(file_id, offset)` 比较。`offset` 必须能表达 `2^32` 这个排他文件尾位置。

- `sync through P`：同步从旧 DurableFrontier 后到 `P` 涉及的文件数据、文件头、封存元数据和尚未持久化的文件创建目录项；
- `trim to P`：将 `P` 所在文件截断到指定 offset，并删除所有更高编号的纯后缀文件。

### 2.2 VLog 与 Prepare Envelope

- 文件按 `D000000.data`、`D000001.data` 递增命名；首个 `VLogEnvelope` 事务发生时才惰性创建首个文件；纯 IndexOnlyDelete 逻辑前缀不创建 VLog；
- 单文件逻辑长度上限为 `2^32` bytes；下一条完整物理记录放不下时先滚动文件；
- 页面固定为 64KiB，任一独立物理记录不得跨页；页尾空间不足时写入可校验的 `PAGE_END`，其内部 Padding 固定为全零字节，不存在独立 PADDING 记录；
- 一个事务可以跨多个页面和 VLog 文件；
- 每个 `VLogEnvelope` 事务独占一个连续的逻辑追加区间 `[vlog_begin, vlog_end)`，不同 VLog 事务的物理记录不得交错；中间的 IndexOnlyDelete 不消耗物理区间；
- `vlog_begin`、`vlog_end` 与受管 VLog 文件拓扑必须能够唯一确定事务涉及的全部物理范围，不另外保存物理区段列表；
- 文件号单调递增且不复用；仍能被 User Index、Snapshot 或 descriptor 引用的位置不得复用；
- 已经完成索引 undo 并按持久化恢复计划安全裁掉的未提交尾部不再可达，可以从 `accepted_end` 重新使用 offset；
- 不使用会提前扩展 `st_size` 的文件预分配；未来如果引入预分配，必须另设可靠的 logical append cursor。

每个 Put 和每个含至少一个 Put 的非空 WriteBatch 写入一个 Prepare Envelope：

```text
TX_BEGIN(commit_seq, tx_uuid, logical_op_count, ...)
KV_RECORD(commit_seq, tx_uuid, op_index, key, value, ...)
DELETE_RECORD(commit_seq, tx_uuid, op_index, key, ...)
...
TX_PREPARED_END(commit_seq, tx_uuid, actual_counts, envelope_crc32c, ...)
```

要求：

- BEGIN、每个操作记录和 END 都是独立物理记录；控制字段位于 header/control payload，不嵌入 User Value；
- ValuePointer 只指向 `KV_RECORD`；`DELETE_RECORD` 不保存 User Value；只有含 Put 的 Batch 中的 Delete 写 `DELETE_RECORD`；
- 单条 Delete 和全 Delete 非空 WriteBatch 是 `IndexOnlyDelete`，不创建 VLog 文件、不写 Envelope、不改变 append cursor；
- footer 汇总事务范围、实际记录数和按顺序计算的事务 digest；
- `TX_PREPARED_END` 必须能从 `durable_vlog_end` 反向定位；
- BEGIN、全部操作记录和 END 均存在且校验通过时，事务才是 prepared；
- `TX_PREPARED_END` 只证明 VLog 数据备妥，不是 commit marker。

最终编码至少包含 magic、格式版本、记录类型、完整编码长度、`commit_seq`、`tx_uuid`、操作记录的 `op_index`、Key/Value 长度、header 校验、整记录 CRC 和自描述尾部信息。必须证明最大合法 `KV_RECORD` 能完整放入一个 64KiB 页面。

### 2.3 ValuePointer

ValuePointer 至少包含：

```text
ValuePointer {
  format_version,
  file_id,
  record_offset,
  record_len,
  value_offset_or_len
}
```

读取时必须验证文件存在、Pointer 未越界且符合页面边界、目标为完整 `KV_RECORD`、CRC 正确、记录 User Key 与请求 Key 相同、Value 边界合法。失败返回 `Corruption` 或实际 I/O 错误，不得返回 `NotFound` 或错误 Value。

### 2.4 Fjall Keyspace 与 descriptor

```text
User Index Keyspace
  UserKey -> ValuePointer
  删除状态为 Absent

Transaction Metadata Keyspace
  tx/<seq>/meta   -> TxMeta
  tx/<seq>/op/<n> -> TxMutation

System Metadata Keyspace
  head_seq
  durable_frontier -> { durable_seq, durable_vlog_seq, durable_vlog_end }
  recovery_state   -> { phase, original_head, target_seq,
                        target_vlog_seq, target_vlog_end,
                        next_undo_seq, trim_required }
```

Transaction Metadata 和 System Metadata 必须与 User Keyspace 物理隔离，且不得通过用户 API 暴露。DurableFrontier 应编码为一个逻辑值；即使拆成多个 Key，也必须在同一个原子 SyncAll Batch 中共同更新。

空库状态为：

```text
head_seq = 0
durable_seq = 0
durable_vlog_seq = 0
durable_vlog_end = EMPTY
```

`EMPTY` 位于任何合法 VLog 记录之前。在 `F=0,E=EMPTY` 时，失败创建留下的首个 VLog 文件属于待 Trim 的物理尾部；此时 `D` 可以因已持久的 IndexOnlyDelete 而大于0。

descriptor 的逻辑结构为：

```text
TxMeta {
  format_version,
  transaction_kind: VLogEnvelope | IndexOnlyDelete,
  commit_seq,
  tx_uuid,
  prev_seq,
  vlog_begin,
  vlog_end,
  logical_op_count,
  distinct_key_count,
  envelope_crc32c,
  descriptor_checksum
}

TxMutation {
  user_key,
  before_state: Absent | ValuePointer,
  after_state:  Absent | ValuePointer
}
```

TxMeta v0 在 v3 中直接重写，不提供旧 v0 解码器。`transaction_kind=0` 表示 `VLogEnvelope`，`transaction_kind=1` 表示 `IndexOnlyDelete`，其他值非法。`IndexOnlyDelete` 的 `vlog_begin`、`vlog_end` 的 file_id/offset 必须全部为0，`envelope_crc32c` 必须为0；`VLogEnvelope` 仍要求 `vlog_begin < vlog_end` 并校验完整 Envelope。descriptor checksum 必须覆盖 `transaction_kind`。

descriptor 可以为恢复重复保存 User Key 和新旧 Pointer，但不得保存 User Value、Value 分片或完整 Key+Value payload。同一 Batch 多次修改同一 User Key 时，descriptor 只保存一个“Batch 前状态 -> Batch 最终状态”的 mutation。VLogEnvelope 按用户加入顺序保存全部逻辑操作；IndexOnlyDelete 不按操作数写 VLog，但 `logical_op_count` 仍保存原始 Delete 数，`distinct_key_count` 与 TxMutation 保存首次出现顺序去重后的 Key 数。每个 IndexOnlyDelete TxMutation 的 `after_state` 必须为 Absent，包括 `Absent -> Absent`。

descriptor 可以拆成多个 Fjall KV。mutation 必须按稳定 ordinal 或等价规范顺序编码，checksum 覆盖条目数、ordinal、User Key、BeforeState 和 AfterState；缺失、重复或多余条目都使 descriptor 无效。条目数和编码长度必须检查整数溢出及可表示性，不得静默截断。

事务 `T` 的 Fjall 原子 Batch 为：

```text
user_index[k1]       = P_new_1
remove user_index[k2]
tx/T/meta            = { kind, T, uuid, prev=T-1, vlog fields, digests, ... }
tx/T/op/0            = { k1, before=P_old_1, after=P_new_1 }
tx/T/op/1            = { k2, before=P_old_2, after=Absent }
system/head_seq      = T
[sync=true 时还包含 durable_frontier = {T, last_vlog_seq(T), last_vlog_end(T)}]
commit(Buffer | SyncAll)
```

User Index mutation、完整 descriptor 和 `head_seq` 必须同批原子提交；descriptor 不属于 ValuePointer，也不得在 User Index 提交后补写。普通读取只通过 User Index 和 VLog 完成，不得依赖允许清理的 descriptor。

## 3. 核心不变量与并发顺序

### 3.1 核心不变量

实现必须始终满足：

1. User Index Value 只保存 ValuePointer，不保存 User Value 或 `commit_seq`；
2. descriptor 可以重复保存 User Key 和 Pointer，但不保存 User Value；
3. User Index mutation、完整 descriptor 和 `head_seq` 同批原子提交；
4. `durable_seq <= head_seq`，正常运行中只前进不后退；恢复 undo 除外，且必须由 RecoveryState 管理；
5. `(durable_seq, durable_vlog_seq, durable_vlog_end)` 是不可拆分的 DurableFrontier，必须同批 SyncAll；
6. 仅在屏障覆盖新 VLogEnvelope 时，推进 DurableFrontier 前先同步 VLog 至对应 `durable_vlog_end`；只覆盖 IndexOnlyDelete 时不执行 VLog I/O；
7. `seq > durable_seq` 的 descriptor 和 before-image 不得清理；
8. `TX_PREPARED_END` 单独存在不表示事务提交；
9. 没有已提交 Fjall 状态引用的完整 Prepare Envelope 是孤儿，不得复活；
10. 可见 Pointer 必须指向存在、完整、校验通过且 User Key 匹配的记录；
11. WriteBatch 只能整体可见或整体不可见；
12. undo 必须按 `H -> C+1` 逆序执行，恢复 BeforeState 前验证当前状态等于 AfterState；
13. 文件号不得复用，仍可达的 Pointer/位置不得复用；仅安全裁掉的未提交尾部 offset 可以重新用于追加；
14. stable prefix 损坏只能返回 `Corruption`，不得降低 `durable_seq`；
15. 先持久化索引 undo 和新 DurableFrontier，再 Trim VLog；
16. RecoveryState 支持恢复期间再次崩溃后的幂等续跑；
17. `sync=true` 成功是本次及此前成功提交的持久化屏障；
18. Fjall commit 成功后，不再执行影响该请求结论的可失败持久化步骤；
19. 已留下物理副作用、可能破坏活动尾部或提交结果不确定的错误发生后，停止后续提交直至重新 Open；
20. descriptor 断档、undo 数据缺失或状态不匹配时不得猜测性回滚；
21. 开放写入前，每个被接受的 `commit_seq` 恰好对应一个非零 `tx_uuid` 和一个完整 descriptor；只有 `VLogEnvelope` 必须恰好对应一个 Prepare Envelope，stable prefix 不得存在逻辑序号缺口、重复序号或夹在 VLog 前缀中的孤儿；
22. 开放 Writer 前，`accepted_end` 后不得保留可被当作有效追加前缀的完整孤儿、残缺记录或新文件，append cursor 必须等于 `accepted_end`。

### 3.2 写入协调与线性化点

单一写入协调器建立全局提交顺序。具体可以使用 writer queue、leader/follower group 或提交互斥区，但必须保证：

1. descriptor 的 BeforeState 在提交顺序保护下读取；
2. 新事务分配 `T = head_seq + 1`；
3. `T+1` 不得先于 `T` 发布到 Fjall；
4. 从分配序号到 Fjall commit 结果确定前，不得产生无法解释的序号洞；
5. 已产生物理副作用或 Fjall 提交不确定时立即停止后续写；
6. DurableFrontier 推进和前台 `sync=true` 共用 `frontier_mutex`。推进者从选择候选 `S`、执行适用时的 VLog 同步到 Fjall SyncAll 结果确定期间持有该锁；
7. 提交 frontier 前重新读取当前值；当前 `durable_seq >= S` 时不得写回较小值；cleanup 只依据实际成功持久化的 frontier。
8. 并发调用线程通过有序写入队列进入提交路径；任意时刻只有协调器选定的活跃提交者可以规划页面/文件滚动、修改 append cursor 并追加 Prepare Envelope。用户读取不进入该队列。

同时取得两个协调对象时，锁顺序固定为：

```text
frontier_mutex -> commit_coordinator
```

任何路径不得反向持锁。空 `sync=true` WriteBatch 按上述锁序捕获持久化目标 `(S,last_vlog_seq(S),last_vlog_end(S))`。读取不持有全局写锁，Get、Snapshot、Iterator 和 Range 基于已发布的 Fjall 视图并发执行。

线性化点为：

- `sync=false`：包含 User Index、descriptor 和 `head_seq` 的 Fjall Buffer Batch 成功；
- `sync=true`：包含上述内容和新 DurableFrontier 的 Fjall SyncAll Batch 成功；
- Fjall commit 成功前，VLogEnvelope 的 Prepare Envelope 只是未提交数据；IndexOnlyDelete 没有 VLog 物理证明，其提交性由 Fjall 复合 Batch 原子性证明。

## 4. 正常提交

### 4.1 空 WriteBatch

空 WriteBatch 不分配 `commit_seq`，也不写 Prepare Envelope：

- `sync=false` 不执行 I/O，直接返回 `Ok`；
- `sync=true` 作为当前 `head_seq` 的持久化屏障，执行第 5 章的显式前沿推进流程；如果 `head_seq == durable_seq`，屏障已经满足，可以直接返回 `Ok`。

### 4.2 `sync=false`

假设提交前 `head_seq = H`，非空事务按以下步骤执行：

1. 在 I/O 前整体校验输入、Batch 语义、编码长度和计数溢出；技术不可表示或资源失败必须安全失败，不得截断 Batch；
2. 进入写入协调区，读取每个不同 User Key 的 BeforeState，并计算最终 AfterState；提交顺序保护保持到步骤 6 的 Fjall commit 结果确定；
3. 分配 `commit_seq = T = H + 1` 和不复用的 `tx_uuid`；
4. 确定事务种类：单 Delete 或全 Delete 非空 Batch 为 IndexOnlyDelete；其余为 VLogEnvelope。VLogEnvelope 按用户操作顺序追加完整 Prepare Envelope；IndexOnlyDelete 完全不调用 VLog writer；
5. 构造 Fjall 原子 Batch，写入全部最终 User Index mutation、完整 descriptor 和 `head_seq=T`；
6. 以 Buffer 语义提交 Fjall Batch；
7. Batch 成功即发布事务、更新内存 `head_seq`；只有 VLogEnvelope 同时更新 `head_vlog_seq/head_vlog_end`，然后返回 `Ok`。

故障结果：

- 步骤 1 失败：`NotCommitted`，没有物理副作用；
- VLogEnvelope 在步骤 4 首字节前失败且可证明没有物理副作用：`NotCommitted`，是否有界重试由《系统设计文档_v3》第4.6节决定；
- 步骤 4 已创建/滚动文件、写入文件头或部分事务记录后失败：`NotCommitted`，停止后续写并重新 Open；
- 步骤 6 可证明在 Batch 未应用前失败：`NotCommitted`；
- 已进入 Fjall commit 且不能证明 Batch 未应用：`CommitUnknown`；
- Fjall Batch 成功后结果为 `Ok`，任何后续失败不得追溯修改结果。

### 4.3 `sync=true`

非空事务按以下步骤执行：

1. 先完成与 `sync=false` 相同的 I/O 前预检，再按 `frontier_mutex -> commit_coordinator` 的顺序取得协调对象，完成 BeforeState 读取、序号分配和 transaction_kind 决定；只有 VLogEnvelope 执行 VLog Prepare；
2. 如果从旧 `durable_vlog_end` 到捕获的 `head_vlog_end` 存在尚未持久化的 VLog 前缀，同步该范围涉及的数据、文件头、封存元数据、文件创建和父目录项；没有脏 VLog 时不调用 VLog sync。当前事务是 VLogEnvelope 时，目标包含它自身的新末端；
3. 构造 Fjall 原子 Batch，同时包含当前事务的全部 User Index mutation、完整 descriptor、`head_seq=T` 和 `durable_frontier={T,last_vlog_seq(T),last_vlog_end(T)}`；
4. 以 SyncAll 提交；该屏障必须同时稳定此前成功的 Buffer Batch；
5. SyncAll 成功后返回 `Ok`；`seq <= T` 的 descriptor 此时才具备清理资格。

步骤 2 失败且 Fjall 尚未提交时，当前事务结果为 `NotCommitted`。因此 `sync=true` IndexOnlyDelete 在前方存在脏 Put VLog 时仍可以返回 VLogSync 失败；该 Delete 自身始终没有 VLog 字节。步骤 4 可证明在 Batch 未应用前失败时为 `NotCommitted`；已进入可能生效阶段后返回错误时为 `CommitUnknown`。

## 5. 空 `sync=true` 的持久化屏障

本流程仅由空 `sync=true` WriteBatch 显式触发，将该请求在线性化顺序上此前已成功返回的异步事务前缀批量变为掉电持久事务。非空 `sync=true` 在第 4.3 节的正常提交路径中直接推进 DurableFrontier。本协议不使用独立后台线程、固定周期、资源阈值、Fjall MemTable Flush 或 VLog 文件滚动自动推进 `durable_seq`；如果后续没有 `sync=true` 请求，`head_seq` 与 `durable_seq` 的差值可以持续增长。

空 WriteBatch 到达提交顺序位置后，按下述步骤捕获 `S = head_seq`。`S == durable_seq` 时持久化屏障已经满足，可以直接返回 `Ok`。

Writer 必须跟踪尚未被 DurableFrontier 覆盖的新文件/目录元数据集合，或使用等价 generation；Open 可以根据旧 `durable_vlog_end` 和受管文件清单重建。frontier 成功推进后，只能清除创建位置或代际不晚于 `S` 的条目，不能误清理由并发事务 `S+1..H` 新增的元数据。

1. 按 `frontier_mutex -> commit_coordinator` 的顺序取得协调对象，捕获 `S = head_seq`、`SF = head_vlog_seq`和 `SE = head_vlog_end`；当前 `durable_seq == S` 时直接返回 `Ok`，否则确认 `durable_seq < S`；
2. 仅当 `(SF,SE)` 高于当前 durable VLog 边界时，同步对应 VLog 数据、文件头、封存元数据、文件创建和父目录项；只有 IndexOnlyDelete 待持久时跳过所有 VLog I/O；
3. 以一个 Fjall 原子元数据 Batch 写入：

   ```text
   durable_frontier = {
     durable_seq: S,
     durable_vlog_seq: SF,
     durable_vlog_end: SE
   }
   ```

   提交前重新读取 frontier；当前 `durable_seq >= S` 时放弃更新，否则以 SyncAll 提交。该 SyncAll 必须同时稳定此前的 User Index、descriptor 和 `head_seq` Buffer Batch；
4. 只有步骤 3 成功后，才允许异步、分批清理 `seq <= S` 的 descriptor/undo 元数据；
5. cleanup 失败只增加元数据空间，不改变事务结果。

Fjall 屏障和 frontier 推进必须由一个携带完整 DurableFrontier 的 SyncAll Batch 完成，不采用“先独立 SyncAll、再用第二个 SyncAll Batch 推进 frontier”的变体。frontier 持久化成功前绝不允许 cleanup。

RustKV 只承诺 DurableFrontier 记录的连续逻辑前缀；空 `sync=true` 请求之后提交的事务不属于本次屏障覆盖范围。Open 根据 transaction_kind、descriptor 和适用时的 Prepare Envelope 判断更晚事务。空 `sync=true` 持久化屏障失败不得修改此前请求已经返回的 `Ok`，实例必须停止新写并重新 Open。

descriptor 生命周期为：

| 序号范围 | 要求 |
|---|---|
| `seq > durable_seq` | 必须完整保留，以支持恢复验证和 undo |
| `seq <= durable_seq` | 可以异步、分批删除 |
| 恢复正在处理的事务 | 由 RecoveryState 和单事务 undo Batch 管理 |

cleanup 必须读取已持久化的 DurableFrontier，不能只依赖内存值。部分 cleanup 成功、失败或崩溃只影响空间。如果未来支持从 VLog 全量重建 User Index，必须按事务顺序重放 `KV_RECORD` 和 `DELETE_RECORD`。

## 6. 崩溃恢复

### 6.1 前提与恢复流程

恢复开始前必须取得数据库目录独占锁，Fjall 已完成自身 journal 恢复，用户读写、descriptor cleanup 和 compaction 尚未启动。恢复期 I/O 失败作为 Open/Recovery 错误返回，不解释为某次用户请求的 `NotCommitted` 或 `CommitUnknown`。

```text
读取并校验 D、F、E、H 和 VLog 拓扑
        |
        v
验证 stable prefix 边界
        |
        v
验证 D+1..H 的 descriptor，按 kind 验证 Envelope，计算 C/CF/CE
        |
        v
若需创建 RecoveryState 且 CF>F，先同步已接受 VLog 至 CE
        |
        v
必要时持久化 RecoveryState
        |
        v
从 H 到 C+1 逆序 undo
        |
        v
无 RecoveryState 的快速推进路径在 CF>F 时同步 VLog 至 CE，持久化新 DurableFrontier
        |
        v
Trim 未提交物理后缀，清除 RecoveryState
        |
        v
确认 append cursor == accepted_end，开放数据库
```

### 6.2 验证 stable prefix

读取：

```text
D = durable_seq
F = durable_vlog_seq
E = durable_vlog_end
H = head_seq
```

验证 `0 <= F <= D <= H`、`F=0 <=> E=EMPTY` 和 DurableFrontier 编码完整。枚举全部受管 VLog，以最后一个文件的 `st_size` 得到 `physical_tail`。编号断档、未知受管对象和多活动候选按 `REC-TOPO-*` fail closed；不得把 `physical_tail` 直接当作提交前沿。

最低校验为：

1. `F=0` 时 `E=EMPTY`，此时即使 `D>0` 也可合法；
2. `F>0` 时 `E` 所需的全部 VLog 文件存在且逻辑长度足够；
3. `E` 位于完整 `TX_PREPARED_END(F)` 后，不得落在记录、PAGE_END 或空洞中部；
4. `F>0` 时，普通 Open 验证边界 footer 的编码、`commit_seq==F`、`tx_uuid`、长度和 CRC，并从 footer 记录的事务起点扫描事务 `F` 的完整 envelope，重算事务 digest；
5. 普通 Open 完成受管文件拓扑和引用边界验证；是否扫描整个 stable prefix 可以由系统设计确定，但不能取消读取时的 Pointer、边界、CRC 和 User Key 校验。

stable prefix 的持久性来自“必要时先同步 VLog，再以 Fjall SyncAll 原子写入 `(D,F,E)`”的顺序；IndexOnlyDelete 的物理证明是 Fjall 复合 Batch 原子性。`E` 只证明最后 VLogEnvelope 边界。VLog 短于 `E`、所需文件缺失或 `E` 不是事务 `F` 的合法 footer 边界时返回 `Corruption`，不得降低 `D`。

### 6.3 验证 unstable suffix 并计算 `C`

1. 初始化 `C=D`、`CF=F`、`CE=E`，并以 `CE` 作为下一个 VLogEnvelope 必须开始的位置；
2. 首先读取并完整验证 `D+1..H` 的全部 descriptor：序号、`prev_seq`、transaction_kind、非零 `tx_uuid`、计数、TxMutation、Pointer 和 descriptor checksum 均必须合法；
3. VLogEnvelope 必须具有合法非空位置和 `envelope_crc32c`；IndexOnlyDelete 必须具有全零 VLog 字段、全部 `AfterState=Absent`，并保留 `Absent->Absent`；
4. 任一 descriptor 缺失、损坏、断档或类型关系非法时，局部 undo 不安全，直接返回 `Corruption`；
5. 然后从 `D+1` 开始按序计算接受前缀：
   - IndexOnlyDelete 不读 VLog，依据 Fjall 已恢复的复合 Batch 原子性接受它，仅令 `C=i`，`CF/CE` 不变；
   - VLogEnvelope 的 `vlog_begin` 必须恰好等于 `CE`，再验证 BEGIN、全部操作记录、END、记录 CRC、User Key、计数、`envelope_crc32c` 和 AfterState Pointer；成功后令 `C=i,CF=i,CE=end(i)`；
6. 遇到第一个不完整或无效 VLogEnvelope 后停止接受；后续任何 VLogEnvelope 或 IndexOnlyDelete 都不得跨过该逻辑序号缺口，但其 descriptor 仍必须在步骤2已经完整验证，以供逆序 undo；
7. `published_end` 是 `D+1..H` 中最后一个 VLogEnvelope 声明的 `vlog_end`；该范围没有 VLogEnvelope 时取 `E`。`accepted_end=CE`，`accepted_vlog_seq=CF`；`physical_tail > CE` 表示必须 Trim。

例如：

```text
D=100, F=99, H=104
tx101 IndexOnlyDelete：descriptor 有效，接受后 C=101, CF=99
tx102 VLogEnvelope：descriptor 和 envelope 有效，接受后 C=102, CF=102
tx103 VLogEnvelope：descriptor 完整，但 envelope 不完整
tx104 IndexOnlyDelete：descriptor 有效

C=102, CF=102, CE=end(102)
```

事务 104 即使不需要 VLog 也不能越过事务 103 被保留；103、104 都必须 undo。

### 6.4 RecoveryState 与逆序 undo

计算：

```text
needs_undo     = C < H
needs_promote  = C > D
needs_trim     = physical_tail > accepted_end
```

存在 `needs_undo` 或 `needs_trim` 时，先执行 RecoveryState 的物理前置门禁：

1. 若 `CF>F`，先同步 `E..CE` 涉及的已接受 VLog 数据、文件头、封存元数据、文件长度、新文件目录项和父目录；成功前不得创建 RecoveryState。
2. 若 `CF==F`，必须有 `CE==E`，不调用 VLog sync。
3. 然后才以 Fjall SyncAll 持久化：

```text
RecoveryState {
  phase: Undo,
  original_head: H,
  target_seq: C,
  target_vlog_seq: CF,
  target_vlog_end: CE,
  next_undo_seq: H,
  trim_required: needs_trim
}
```

RecoveryState 必须结合当前 `HeadSeq` 和 DurableFrontier `{D,F,E}` 验证：`D <= target_seq <= next_undo_seq <= original_head`、`F <= target_vlog_seq <= target_seq`、`target_vlog_seq=0 <=> target_vlog_end=EMPTY`；Undo phase 还必须有 `head_seq==next_undo_seq`。`target_vlog_seq==F` 时必须有 `target_vlog_end==E`；`target_vlog_seq>F` 时 target end 必须严格超过 E、是该序号的合法 Footer 后边界，且已在 RecoveryState 创建前持久化。`target_seq` 可以大于 `target_vlog_seq`；此时尾随 IndexOnlyDelete 属于恢复保留的逻辑前缀。Open 发现已有 RecoveryState 时，必须按其中的 phase 和进度续跑，不得重新选择恢复目标。

前置 VLog 同步或 RecoveryState SyncAll 失败/结果不确定时，Open 失败且不执行 Undo/Trim。下次 Open 若不存在 State 则重算 C/CF/CE；若 State 存在，其引用的新增已接受 VLog 必须已由上述顺序持久化，只按固定 target 续跑。

VLog sync 是整文件操作。边界文件中 CE 之后的 rejected 尾字节可能被顺带持久化，但仍是必须在目标 Frontier 成功后 Trim 的 orphan。恢复不得主动同步严格位于 CE 之后的文件，也不得因边界文件已落盘而接受或复活 rejected 字节。

只有 `needs_promote=true` 且不需要 undo、没有多余物理尾部时，可以不创建 RecoveryState，直接幂等推进 frontier：SyncAll 前崩溃会重做，成功后崩溃则新 frontier 已完整存在。三者均为 false 时不修改状态。

对 `i=H, H-1, ..., C+1` 逆序执行：

1. 读取事务 `i` 的完整 descriptor；
2. 验证每个不同 User Key 的当前 User Index 原始状态严格等于 AfterState；只比较 Pointer 字节，不要求解引用已损坏的 Value；
3. 状态不匹配时返回 `Corruption`，不得强行覆盖；
4. 用一个 Fjall 原子 SyncAll Batch 恢复全部 BeforeState、删除或标记事务 `i` 的 descriptor、写入 `head_seq=i-1` 和 `recovery_state.next_undo_seq=i-1`；
5. Batch 整体生效或整体不生效，恢复再次崩溃后可以重做当前 `i`。

IndexOnlyDelete 与 VLogEnvelope 使用同一逆序 undo 原子 Batch。IndexOnlyDelete 只恢复 BeforeState、删除 descriptor 并回退 HeadSeq/RecoveryState，不执行 VLog 读、写、同步或 Trim。若一个更早 VLogEnvelope 已成为首个无效事务，它之后的 IndexOnlyDelete 仍须先逆序 undo。

必须逆序 undo：同一 Key 若经历 `P1 -> P2 -> P3`，先 undo 较早事务会破坏较晚事务 AfterState 的验证条件。

descriptor 断档时不能直接 undo。例如事务 102 descriptor 缺失，而事务 103 BeforeState 指向事务 102 的 Pointer，undo 103 会重新引入无法验证的事务 102 状态。因此，VLog envelope 无效但 descriptor 完整时可以逆序 undo；descriptor 或 undo 数据缺失时只能返回 `Corruption`，或执行另行设计和验证的全量重建。

### 6.5 稳定、Trim 与开放数据库

完成 undo 后：

1. RecoveryState 存在时，其新增 accepted VLog 已在 State 创建前同步，此处不依赖延后 sync 来使固定 target 变得持久。无 RecoveryState 的快速推进路径仅在 `CF>F` 时先同步 `E..CE` 的已接受 VLog；只接受 IndexOnlyDelete 时不执行 VLog sync。不得主动同步严格位于 CE 之后的文件；边界文件中顺带落盘的 rejected 尾仍必须 Trim；
2. `CF>0` 时重新验证 `CE` 是事务 `CF` 的合法终点；然后用一个 Fjall 原子 SyncAll Batch 写入 `head_seq=C` 和 `durable_frontier={C,CF,CE}`；如果 RecoveryState 存在，按 `trim_required` 将 phase 置为 Trim 或 Finalize；
3. 如果仅快速推进 frontier 且没有 RecoveryState，步骤 2 成功后直接进入步骤 5，不写入孤立 phase；
4. 只有步骤 2 成功且 `trim_required=true` 时，才幂等执行：
   - 将边界文件截断到 `CE`；
   - 删除全部更高编号的纯后缀文件；
   - `accepted_end=EMPTY` 时删除全部未提交 VLog，恢复为无 VLog 文件的 `AppendState::Empty` 惰性创建状态；逻辑前缀可以保留 IndexOnlyDelete；
   - 同步被截断文件和父目录；
   - 将 append cursor 重置为 `accepted_end`；
5. RecoveryState 存在时，以 Fjall SyncAll 清除；
6. 再次确认实际追加尾等于 `accepted_end`，然后启动 compaction、descriptor cleanup 等允许的后台维护任务并开放用户 API；这些任务不得自动推进 DurableFrontier；
7. `seq <= C` 的残留 descriptor 交由正常 cleanup。

如果 frontier 更新后、Trim 前再次崩溃，下次 Open 根据 RecoveryState 继续 Trim。边界后的完整 envelope 仍是孤儿，不得复活；Writer 不得从崩溃前未裁剪的物理 EOF 继续追加。

RecoveryState、undo 或 frontier SyncAll 失败时，绝不能先截断或删除 VLog；数据库保持未开放并返回 Open/Recovery I/O 错误。已完成的单事务 undo 由原子进度记录，释放空间后重新 Open 可继续。除非未来另有经过证明的模式，不得在部分恢复状态下开放只读或读写实例。

### 6.6 提交真相

`C` 只存在于 Open 恢复期间：

```text
RuntimeCommitted(T)
= D < T <= H
  AND Fjall 原子 Batch 已成功发布 User Index + descriptor + head_seq

RecoveredPendingCommitted(T)
= D < T <= C
  AND descriptor 存在且完整有效
  AND (kind=IndexOnlyDelete
       OR kind=VLogEnvelope 且 descriptor 引用的 Prepare Envelope 完整有效)

StableCommitted(T)
= T <= D
  AND DurableFrontier(D,F,E) 已按“必要时先 VLog sync、后 Fjall SyncAll”持久化
```

对于 stable prefix，`E` 是边界而不是内容证明，任何可见 Pointer 仍需读取时校验。完整 `KV_RECORD`、完整 Prepare Envelope、`TX_PREPARED_END`、VLog 中连续事务序号或孤立 Fjall 元数据，都不能单独证明事务提交。

## 7. 故障处理与可观测性

### 7.1 故障结果

| 故障点 | 请求结果 | 最低处理与恢复行为 |
|---|---|---|
| 参数、大小或资源预检失败，尚未 I/O | `NotCommitted` | 无恢复动作 |
| VLog open/create 在首字节前失败，且可证明没有物理修改 | `NotCommitted` | 按《系统设计文档_v3》第4.6节决定是否有界重试 |
| 已创建/滚动文件、写入文件头或部分事务记录，尚未 Fjall commit | `NotCommitted` | 停止新写；Open 证明为未提交后缀后 Trim |
| `sync=true` 的 VLog/目录同步失败，尚未 Fjall commit | `NotCommitted` | VLog 字节为孤儿；停止新写并重新 Open |
| Fjall 可证明在原子 commit 前拒绝 | `NotCommitted` | Open Trim 对应 VLog 孤儿后缀 |
| 已进入 Fjall commit，无法证明 Batch 未应用 | `CommitUnknown` | 停止新写；Open 后事务整体保留或整体回滚 |
| `sync=false` Buffer Batch 成功 | `Ok` | 进程崩溃后保留；OS 崩溃后按连续前缀恢复 |
| `sync=true` SyncAll Batch 成功 | `Ok` | 本次及此前成功提交必须保留 |
| Fjall 成功后、调用方收到结果前进程崩溃 | 调用方视角未知 | Open 后事务整体存在或整体不存在 |
| 空 `sync=true` 持久化屏障失败 | 不改变此前请求的既有 `Ok` | 本请求按失败阶段返回；停止新写并由 Open 处理未稳定后缀 |
| descriptor cleanup 失败 | 不改变既有 `Ok` | 保留冗余 descriptor，稍后重试 |

IndexOnlyDelete 本身不会触发 VLog append 失败。`sync=true` IndexOnlyDelete 仅在它前方存在尚未持久化的 Put VLog 前缀时才可以在 Fjall commit 前返回 VLogSync 错误；没有脏 VLog 时不得产生 VLog I/O 或相关错误。

已留下物理副作用、可能破坏活动尾部或提交结果不确定时，必须停止新写并重新 Open。只有 `NotCommitted + Healthy` 才可能允许调用方在同一实例重试；`CommitUnknown` 不得盲目重放。具体实例状态、读能力和重试方式以《系统设计文档_v3》第4.6节为准。

### 7.2 当前状态查询

`stats()` 在全部活实例状态下可调用，不得触发磁盘 I/O、恢复、同步、Flush、Compaction 或状态迁移。公共 Stats 不新增 `durable_vlog_seq`字段。`durable_seq>0` 时 `durable_vlog_end` 允许为 `None`，表示目前已持久前缀全部是 IndexOnlyDelete。`durability_lag` 仍严格等于 `head_seq-durable_seq`。纯 Delete 不得增加 VLog 文件数、逻辑字节数或改变活动文件。

RustKV 不提供公共结构化事件监听器、事件回调或可查询事件历史。状态迁移、`CommitUnknown`、持久化屏障、恢复、文件滚动和容量错误的公共观察边界由操作返回值、实例状态和 `stats()` 共同构成。实现可以保留不对外承诺格式、顺序、完整性或持久化的内部诊断；该诊断不得包含原始 User Key/Value，也不得改变事务结果、实例状态或首个锁存错误。

## 8. 实现边界与验证

### 8.1 模块责任

| 模块 | 责任 |
|---|---|
| `commit_coordinator` | 提交全序、`commit_seq`/`tx_uuid`、BeforeState 和 frontier 协调 |
| `vlog::format` | 记录编码、版本、CRC 和 page/file 边界 |
| `vlog::writer` | append、roll、用户态 flush 和 sync-through |
| `vlog::reader` | Pointer 解引用、User Key/CRC/边界校验和 envelope 扫描 |
| `index` | Fjall User Index 适配，禁止 User Value 落入 LSM |
| `txn_descriptor` | descriptor 编解码、校验和 cleanup |
| `durability` | 非空与空 `sync=true` 的持久化屏障 |
| `recovery` | `D/F/E/H/C/CF/CE`、RecoveryState、逆序 undo 和 Trim |
| `fault_injection` | 确定性故障点和恢复断言 |

### 8.2 必须覆盖的故障点

- `TX_BEGIN`、首个/中间/末个操作记录、`TX_PREPARED_END` 的写前、写中和写后；
- 空 WriteBatch；64KiB 页面边界；4GiB 文件滚动；
- 新文件创建、文件同步和目录同步；
- 较早 `sync=false` 创建文件、随后 `sync=true` 未滚动，但仍需同步旧 frontier 以来的目录项；
- Fjall 原子 Batch 调用前、可能生效阶段和返回前后；
- IndexOnlyDelete 的 Fjall Buffer/SyncAll 调用前、明确未应用、可能应用和成功后强制终止；
- `sync=true` IndexOnlyDelete 在“前方脏 Put VLog 同步前/后、Delete SyncAll 前/可能应用/后”的全部边界；
- DurableFrontier 更新，以及并发非空/空 `sync=true` 请求的顺序与屏障覆盖范围；
- descriptor cleanup 的每个分批；
- RecoveryState 创建、每次 undo、frontier 推进、VLog 截断和 RecoveryState 清除；
- 已接受 VLog 前缀同步成功/失败、同步后但 RecoveryState SyncAll 前强制终止，以及 RecoveryState SyncAll 结果不确定；必须证明 State 不可能持久化地引用尚未持久化的新增 accepted VLog；
- 恢复过程中再次发生进程崩溃或 OS 崩溃；
- `H=D` 且 Fjall commit 前留下完整 envelope、残缺记录、残缺 PAGE_END 或新文件头；
- ENOSPC、EDQUOT、inode 耗尽、短写、EIO 和同步失败。

Fjall 适配层还必须专项验证跨三个 Keyspace 的 Batch 原子恢复、Buffer 后的进程崩溃恢复、SyncAll 覆盖此前 Buffer Batch、commit 错误的生效判定、journal persist 失败后的 fail-stop 行为；Fjall 锁定版本升级后重跑全部协议测试。

### 8.3 共同断言

每个适用故障点都必须验证：

1. WriteBatch 全有或全无；
2. 恢复结果为提交全序的连续前缀；
3. 不存在用户可见的悬空 Pointer，Pointer 通过边界、CRC 和 User Key 校验；
4. 完整孤儿不会复活；
5. `sync=true` 已成功事务不丢失，`sync=false` 只允许丢失完整提交后缀；
6. `(durable_seq,durable_vlog_seq,durable_vlog_end)` 只能恢复为旧三元组或新三元组，不得交叉组合；
7. `durable_vlog_seq>0` 时 `durable_vlog_end` 位于该序号完整 envelope 后，VLog 短于该边界或边界落在记录中部时返回 `Corruption`；
8. cleanup 失败不改变事务结果；
9. 重复 Open 最终收敛到同一状态；
10. stable prefix 损坏不得伪装成 `NotFound` 或自动回退；
11. 开放 Writer 前 append cursor 等于 `accepted_end`，不存在重复 `commit_seq` 或夹在前缀中部的孤儿；
12. RecoveryState/undo 因 ENOSPC 失败时，VLog 尚未被截断且数据库保持未开放。
13. 全新数据库上任意数量的纯 Delete 可以使 `head_seq/durable_seq>0`，但 `durable_vlog_seq=0`、VLog end 为 Empty 且不存在 VLog 文件。
14. 第一个无效 VLogEnvelope 后的 IndexOnlyDelete 不得跨过序号缺口被接受，必须与其他后缀事务一起逆序 undo。
15. RecoveryState 存在且 `target_vlog_seq` 超过它创建时的 F 时，目标 VLog 前缀必须已在 State SyncAll 之前完成持久化；边界文件中顺带落盘的 rejected 尾仍不得复活。

### 8.4 已由系统设计 v3 确定的参数

以下参数均已由《系统设计文档_v3》确定；实现阶段只能按对应章节落实，不得再设计、选型或改变：

1. VLog 记录二进制布局、小端序、CRC32C/digest 字节流和直接拒绝未知版本的规则固定为系统设计第6章；
2. ValuePointer v0 固定16字节，字段与验证固定为系统设计第5.7节；
3. Fjall 固定 `=3.1.8`，三个私有 Keyspace、关闭内置 KV separation 和 Buffer/SyncAll 能力边界固定为第2、5章；
4. WriteBatch/Descriptor 宽度、溢出检查、fallible allocation 和资源失败语义固定为第4.3、4.6、5.4、7.1.2节；
5. 写入固定使用短生命周期 Leader/Follower 有序队列，不实现 group commit，不合并不同公共请求的事务边界；
6. 恢复遇到 ENOSPC 或持久化失败时保持数据库未开放、保留 RecoveryState 和未裁剪 VLog，修复外部环境后重新 Open；
7. Rust 错误类型、状态查询 API、重试预算、REC-TOPO 错误与故障点固定为第4.6、8～10章；
8. 每个非空事务使用操作系统安全随机源生成非零、不复用 UUID v4，并作为 Descriptor 和适用 Envelope 的一部分持久化；
9. 普通 Open 只强校验 stable VLog 边界、全部 unstable Descriptor、按 kind 的连续接受前缀、RecoveryState 和 Trim 边界，不扫描整个 stable prefix 或整个 User Index。

这些参数不得改变本文规定的外部语义、提交顺序、恢复真相源和核心不变量。
