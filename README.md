# RustKV

RustKV is a single-process embedded key-value library implemented in Rust.

The implementation contract is defined by `../系统设计文档_v2.md`.

## 60分单线程核心里程碑

当前实现已经按设计文档组装真实公共 `Db`，覆盖：

- 数据库创建、打开、根目录锁、Identity-first 校验与 Open Recovery；
- `Put`、`Get`、`Delete` 和原子 `WriteBatch`；
- `sync=false`、`sync=true` 和空 `sync=true` 前缀持久化屏障；
- 正常 Drop、进程未执行 Drop 退出后的重开恢复；
- 无 I/O、不可失败的真实内存 `DbStats` 快照。

本里程碑的公共端到端验收范围是单线程。内部保留最终状态机、写闸门、提交协调器、operation guard 和恢复协议，但这不表示并发 P0 已验收，也不表示项目已达到完整 P0 或生产可用状态。

以下公共能力仍按契约返回结构化 `Unsupported`，将在后续阶段实现：

- `Snapshot`；
- `DbIterator`；
- `RangeCursor`；
- `Db::destroy`。

SIGKILL 测试只验证进程终止后的协议结果，不等同于 OS 崩溃、掉电或完整存储栈持久化验收。Drop 不会隐式执行同步屏障，也不会把 `sync=false` 的保证升级为 `sync=true`。
